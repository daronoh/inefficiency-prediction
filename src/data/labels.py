"""Observation-aware targets from standalone monitoring-window simulations."""
from dataclasses import asdict
import json
from pathlib import Path
import numpy as np
import pandas as pd
from src.common import sha256
from src.config import SPEC, spec_hash, require_split_access
from src.paths import EVENT_UNIVERSE, data_relative
from src.data.books import RawGroup
from src.data.io import load_frames, observation_groups
from src.data.coverage import classify_frame, observation_fraction
from src.data.universe import build_plan
from src.data.rules import frozen_fee_evidence
from src.simulator.discovery import token_quotes, split_monitoring_hours, discover_observed_package
from src.simulator.execution import experiment
from src.simulator.scheduler import run_event

def summarize_window(detail, signal, spec):
    if detail.empty:
        return dict(positive=False, future_positive=False, positive_episodes=0,
                    known_pnl_usd=0., attempts=0, unknown_attempts=0, partial=0, cash_usd=0.)
    taken = detail.loc[detail.attempted].copy()
    positive = taken.loc[taken.pnl_usd > spec.assumed_other_cost_usd + 1e-10]
    lookup = signal[['pair_id', 'start_time', 'original_spell_start', 'episode_id', 'start_observed']]
    matched = positive.merge(lookup, on=['pair_id', 'start_time'], validate='one_to_one')
    return dict(positive=bool(len(positive)),
        future_positive=bool(((matched.original_spell_start > matched.decision_time) & matched.start_observed).any()),
        positive_episodes=matched.episode_id.nunique(),
        known_pnl_usd=float(taken.pnl_usd.sum() - len(taken) * spec.assumed_other_cost_usd),
        attempts=len(taken), unknown_attempts=int(taken.pnl_usd.isna().sum()),
        partial=int(taken.status.str.startswith('partial_').sum()),
        cash_usd=float(taken.entry_cost_usd.sum()))


def finalize_onsets(output, spec=SPEC):
    """Exclude left-censored first observations from the new-gap diagnostic."""
    output = Path(output)
    labels = pd.read_parquet(output / 'labels.parquet')
    labels['future_positive'] = False
    for event_dir in (output / 'events').iterdir():
        signals = pd.read_parquet(event_dir / 'signals.parquet')
        if signals.empty:
            continue
        for policy in ['carry_forward']:
            path = event_dir / f'{policy}_attempts.parquet'
            if not path.exists():
                continue
            details = pd.read_parquet(path)
            details = details.merge(signals[['pair_id', 'start_time', 'original_spell_start', 'start_observed']],
                on=['pair_id', 'start_time'], validate='one_to_one')
            positives = details.loc[details.pnl_usd.gt(spec.assumed_other_cost_usd + 1e-10)
                & details.original_spell_start.gt(details.decision_time) & details.start_observed]
            mask = labels.event_slug.eq(event_dir.name) & labels.quote_policy.eq(policy) & labels.time.isin(positives.decision_time)
            labels.loc[mask, 'future_positive'] = True
    labels.to_parquet(output / 'labels.parquet', index=False)
    (output / 'complete.json').write_text(json.dumps(dict(labels_sha256=sha256(output / 'labels.parquet'),
        rows=len(labels), events=labels.event_slug.nunique(), new_gap_requires_observed_onset=True,
        final_lockbox_opened=False), indent=2) + '\n')

def build_labels(output, features_path, spec=SPEC):
    require_split_access('development')
    output, features_path = Path(output), Path(features_path)
    feature_run = json.loads((features_path.parent / 'run.json').read_text())
    if feature_run['spec_hash'] != spec_hash(spec):
        raise ValueError('Feature and monitoring-window contracts differ')
    features = pd.read_parquet(features_path)
    if (features.time < pd.Timestamp(spec.development_start)).any() or (features.time + spec.horizon > pd.Timestamp(spec.lockbox_start)).any():
        raise ValueError('Every label window must stay within development')
    output.mkdir(parents=True, exist_ok=False)
    design = dict(version=2, contract=asdict(spec), spec_hash=spec_hash(spec),
        feature_sha256=sha256(features_path), source_sha256=sha256(__file__),
        execution_book_policy='per_token_recording_extent', quote_policies=['carry_forward'],
        max_quote_age_seconds=None, coverage='Every active package is observed or has an observed entry-blocking leg',
        label='At least one positive simulated attempt in a standalone monitoring window',
        unknown='Both classes require full observation coverage and zero unknown attempted executions',
        fees='Frozen market flags false; historical zero-fee assumption, not point-in-time verification',
        final_lockbox_opened=False)
    (output / 'design.json').write_text(json.dumps(design, indent=2) + '\n')
    plans, excluded = build_plan('all')
    universe = pd.read_parquet(EVENT_UNIVERSE)
    frozen_fee_evidence(universe.loc[universe.event_slug.isin([p['event_slug'] for p in plans]),
                                   'market_id'].astype(str).tolist()).to_csv(output / 'fee_evidence.csv', index=False)
    all_labels, inventories = [], []
    for number, plan in enumerate(plans, 1):
        slug = plan['event_slug']
        windows = pd.DatetimeIndex(features.loc[features.event_slug.eq(slug), 'time'].sort_values())
        if windows.empty:
            continue
        print(f'Observation labels {number}/{len(plans)}: {slug}', flush=True)
        frames = {t: RawGroup([f]).frames[0] for t, f in load_frames(plan).items()}
        states = {t: classify_frame(f, spec) for t, f in frames.items()}
        quotes = {t: token_quotes(f, spec) for t, f in frames.items()}
        coverage = observation_fraction(plan, states, windows, spec)
        spells = []
        for i, (ids, start, end) in enumerate(plan['packages']):
            spells.extend(discover_observed_package(ids, quotes, states, start, end, slug, f'{slug}:{i}', spec))
        signals = pd.DataFrame(split_monitoring_hours(spells, spec))
        event_dir = output / 'events' / slug
        event_dir.mkdir(parents=True)
        signals.to_parquet(event_dir / 'signals.parquet', index=False)
        groups, mapping = observation_groups(plan, frames, set(signals.pair_id)) if len(signals) else ({}, {})
        by_time = dict(tuple(signals.groupby('time'))) if len(signals) else {}
        details = []
        for when, fraction in zip(windows, coverage, strict=True):
            selected = by_time.get(when, pd.DataFrame())
            detail = run_event(selected, groups, mapping, experiment(spec))[0] if len(selected) else pd.DataFrame()
            stats = summarize_window(detail, selected, spec)
            known = fraction >= 1 - 1e-9 and stats['unknown_attempts'] == 0
            all_labels.append(dict(event_slug=slug, time=when, quote_policy='carry_forward',
                coverage_fraction=float(fraction), label_known=known,
                target=float(stats['positive']) if known else np.nan, **stats))
            if len(detail):
                details.append(detail.assign(quote_policy='carry_forward'))
        if details:
            pd.concat(details, ignore_index=True).to_parquet(event_dir / 'carry_forward_attempts.parquet', index=False)
        inventories.extend(dict(event_slug=slug, token_id=t, path=data_relative(plan['paths'][t]),
            bytes=plan['paths'][t].stat().st_size, rows=len(f),
            development_first=str(f.snapshot_time.min()), development_last=str(f.snapshot_time.max())) for t, f in frames.items())
        pd.DataFrame(all_labels).to_parquet(output / 'labels.parquet', index=False)
    pd.DataFrame(inventories).to_csv(output / 'input_inventory.csv', index=False)
    pd.DataFrame(excluded).to_csv(output / 'excluded_events.csv', index=False)
    finalize_onsets(output, spec)
    print(f'Completed {len(all_labels):,} carry-forward labels.', flush=True)
    return pd.read_parquet(output / 'labels.parquet')
