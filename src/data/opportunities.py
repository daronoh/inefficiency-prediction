"""Observation-time opportunity labels, independent of order execution outcomes."""
from dataclasses import asdict, dataclass
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.common import sha256
from src.config import SPEC, ScreeningSpec, require_split_access
from src.data.books import RawGroup
from src.data.coverage import classify_book, observation_fraction
from src.data.io import load_frames
from src.data.universe import build_plan
from src.data.rules import frozen_fee_evidence
from src.paths import EVENT_UNIVERSE, PROJECT_ROOT
from src.reporting.workflow import profitability_dataset
from src.simulator.capacity import discover_depth_signals


@dataclass(frozen=True)
class Qualification:
    """Only observation-time constraints; deliberately no latency or unwind fields."""
    sizing: str = 'adaptive'
    min_entry_gross_usd: float = 0.
    min_limit_margin_usd: float = 0.
    min_quantity: int = 5
    max_quantity: int | None = None
    cash_cap_usd: float | None = None


def label_windows(windows, signals, coverage, spec=SPEC):
    """Symmetric full-window coverage; witnessed positives in partial windows are diagnostics."""
    windows = pd.DatetimeIndex(windows)
    if len(coverage) != len(windows) or windows.has_duplicates:
        raise ValueError('Expected one coverage value per unique scan')
    if not np.isfinite(coverage).all() or np.any((np.asarray(coverage) < 0) | (np.asarray(coverage) > 1)):
        raise ValueError('Invalid coverage fractions')
    if len(signals):
        if (signals.end_time <= signals.start_time).any():
            raise ValueError('An opportunity episode must have positive observed duration')
        grouped = dict(tuple(signals.groupby('time')))
    else:
        grouped = {}
    rows = []
    for when, fraction in zip(windows, coverage, strict=True):
        part = grouped.get(when, pd.DataFrame())
        if len(part):
            if not (part.start_time.ge(when) & part.end_time.le(when+spec.horizon)).all():
                raise ValueError('Signal crosses its declared monitoring window')
            witnessed = True
            visible = bool(part.start_time.eq(when).any())
            new_onset = bool((part.original_spell_start.gt(when) & part.start_observed).any())
            episodes = part.episode_id.nunique()
            first = part.start_time.min()
        else:
            witnessed = visible = new_onset = False
            episodes, first = 0, pd.NaT
        known = fraction >= 1-1e-9
        rows.append(dict(time=when, coverage_fraction=float(fraction), label_known=known,
            target=float(witnessed) if known else np.nan,
            witnessed_opportunity=witnessed, opportunity_at_scan=visible,
            future_positive=new_onset and known, witnessed_new_onset=new_onset,
            qualifying_package_episodes=int(episodes), first_opportunity_time=first))
    return pd.DataFrame(rows)


def event_labels(plan, frames, windows, spec=SPEC):
    """Reconstruct quote qualification and coverage; never invoke a trade scheduler."""
    frames = {token:RawGroup([frame]).frames[0] for token,frame in frames.items()}
    states = {}
    for token, frame in frames.items():
        values = [classify_book(r.asks_json,r.bids_json,r.min_order_size,r.tick_size,quantities=None)
                  for r in frame.itertuples(index=False)]
        states[token] = pd.DataFrame(values,columns=['size_qualified','state','reason']).assign(
            time=frame.snapshot_time.to_numpy())
    coverage = observation_fraction(plan, states, pd.DatetimeIndex(windows), spec)
    # This shared helper consults only Qualification's quote constraints, not fills.
    signals = discover_depth_signals(plan, frames, Qualification(), spec)
    if len(signals):
        signals = signals.loc[signals.time.isin(windows)].copy()
    return label_windows(windows, signals, coverage, spec), signals


def build_dataset(output, parent):
    require_split_access('development')
    output, parent = Path(output), Path(parent)
    parent_complete = json.loads((parent/'complete.json').read_text())
    parent_design = json.loads((parent/'design.json').read_text())
    if parent_complete['examples_sha256'] != sha256(parent/'examples.parquet'):
        raise ValueError('Parent snapshot features changed')
    if parent_complete.get('final_lockbox_opened') or parent_design.get('final_lockbox_opened'):
        raise ValueError('Final test must remain closed')
    inherited = parent_design['contract']
    # Dates/window lengths only. Arrival, unwind, funding and old targets are not read.
    window = {k:inherited[k] for k in ('development_start','lockbox_start','cadence_minutes','horizon_minutes')}
    spec = ScreeningSpec(**window)
    schema = pq.read_schema(parent/'examples.parquet').names
    columns = ['event_slug','stratum','time','eligible_snapshot'] + [c for c in schema if c.startswith('x_')]
    source = pd.read_parquet(parent/'examples.parquet',columns=columns)
    base = source.loc[source.eligible_snapshot, columns].copy()
    if not (base.time.ge(pd.Timestamp(spec.development_start)) &
            (base.time+spec.horizon).le(pd.Timestamp(spec.lockbox_start))).all():
        raise ValueError('Every target window must stay within development')
    output.mkdir(parents=True,exist_ok=False)
    (output/'events').mkdir()
    code = ['src/data/opportunities.py','src/data/coverage.py','src/data/books.py',
            'src/data/io.py','src/data/universe.py','src/data/rules.py',
            'src/simulator/capacity.py','src/simulator/sizing.py','src/simulator/discovery.py']
    design = dict(version='observation_opportunity_v1', window=window,
        qualification=asdict(Qualification()), parent_examples_sha256=sha256(parent/'examples.parquet'),
        parent_completion_sha256=sha256(parent/'complete.json'), feature_columns=[c for c in columns if c.startswith('x_')],
        target='At least one qualifying quoted basket in [scan, scan + horizon), including at the scan',
        secondary='Observed new episode onset strictly after the scan; left-censored first observations excluded',
        coverage='Both classes require fully observed/decidable active packages throughout the window',
        fees='Verified zero exchange fees for this frozen historical cohort; no additional execution cost in qualification',
        execution_outcomes_used=False, final_lockbox_opened=False,
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in code})
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    plans,_ = build_plan('all')
    expected = {(row['event_slug'],str(row['token_id'])):row['input_sha256'] for row in parent_complete['input_inventory']}
    inventory, pieces, hashed = [], [], {}
    universe = pd.read_parquet(EVENT_UNIVERSE)
    slugs = set(base.event_slug)
    fees = frozen_fee_evidence(universe.loc[universe.event_slug.isin(slugs),'market_id'].astype(str).tolist())
    fees.to_csv(output/'fee_evidence.csv',index=False)
    for plan in plans:
        slug = plan['event_slug']
        selected = base.loc[base.event_slug.eq(slug)]
        if selected.empty:
            continue
        print(f'Observation-only labels: {slug}',flush=True)
        for token,path in plan['paths'].items():
            path = Path(path)
            if path not in hashed:
                hashed[path] = sha256(path)
            if hashed[path] != expected.get((slug,token)):
                raise ValueError(f'Raw books differ from the frozen feature inputs: {slug}, {token}')
            inventory.append(dict(event_slug=slug,token_id=token,input_sha256=hashed[path]))
        labels,signals = event_labels(plan,load_frames(plan),pd.DatetimeIndex(selected.time),spec)
        folder = output/'events'/slug
        folder.mkdir()
        signals.to_parquet(folder/'signals.parquet',index=False)
        pieces.append(selected.merge(labels,on='time',validate='one_to_one'))
    result = pd.concat(pieces,ignore_index=True).sort_values(['time','event_slug']).reset_index(drop=True)
    if len(result) != len(base) or result.duplicated(['event_slug','time']).any():
        raise ValueError('Changed eligible snapshot population')
    result.to_parquet(output/'examples.parquet',index=False)
    pd.DataFrame(inventory).to_csv(output/'input_inventory.csv',index=False)
    counts = dict(rows=len(result),events=result.event_slug.nunique(),known=int(result.label_known.sum()),
        positive=int(result.target.eq(1).sum()),negative=int(result.target.eq(0).sum()),
        unknown=int((~result.label_known).sum()),future_positive=int(result.future_positive.sum()),
        witnessed_positive_in_unknown=int((result.witnessed_opportunity & ~result.label_known).sum()))
    (output/'summary.json').write_text(json.dumps(counts,indent=2)+'\n')
    files = {p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(examples_sha256=sha256(output/'examples.parquet'),
        files_sha256=files,execution_outcomes_used=False,final_lockbox_opened=False),indent=2)+'\n')
    return result


def load_dataset(folder):
    folder = Path(folder)
    design = json.loads((folder/'design.json').read_text())
    complete = json.loads((folder/'complete.json').read_text())
    if complete.get('execution_outcomes_used') or complete.get('final_lockbox_opened'):
        raise ValueError('Expected observation-only development labels')
    if sha256(folder/'examples.parquet') != complete['examples_sha256']:
        raise ValueError('Opportunity examples changed')
    frame = pd.read_parquet(folder/'examples.parquet')
    spec = ScreeningSpec(**design['window'])
    if not (frame.time.ge(pd.Timestamp(spec.development_start)) &
            (frame.time+spec.horizon).le(pd.Timestamp(spec.lockbox_start))).all():
        raise ValueError('Final test cannot enter development')
    return frame


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    build_dataset(args.output,profitability_dataset())
