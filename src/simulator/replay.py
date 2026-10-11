"""Replay each selected policy independently with a persistent liquidity ledger."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
from src.common import sha256
from src.config import SPEC
from src.data.io import load_frames, observation_groups
from src.data.universe import build_plan
from src.models.evaluation import ranked
from src.simulator.execution import experiment
from src.simulator.scheduler import run_event

def replay(output, labels_dir, models_dir, spec=SPEC, top_n=5):
    output, labels_dir, models_dir = Path(output), Path(labels_dir), Path(models_dir)
    output.mkdir(parents=True, exist_ok=False)
    label_design = json.loads((labels_dir / 'design.json').read_text())
    model_design = json.loads((models_dir / 'design.json').read_text())
    if model_design['label_sha256'] != sha256(labels_dir / 'labels.parquet'):
        raise ValueError('Predictions and replay must use the same rebuilt labels')
    if label_design['contract']['horizon_minutes'] != spec.horizon_minutes:
        raise ValueError('Replay monitoring window differs from labels')
    policies = ['carry_forward']
    group_builder = observation_groups
    prediction = pd.read_parquet(models_dir / 'predictions.parquet')
    if prediction.time.max() >= pd.Timestamp(SPEC.lockbox_start):
        raise ValueError('Final lockbox must remain closed')
    choices = {}
    for name, frame in prediction.groupby('model'):
        selected = ranked(frame, frame.score, top_n)
        choices[name] = set(zip(selected.event_slug, selected.time))
    pool = prediction.drop_duplicates(['event_slug', 'time'])
    choices['monitor_all'] = set(zip(pool.event_slug, pool.time))
    plans, _ = build_plan('all')
    all_details, summaries = [], []
    for plan in plans:
        slug = plan['event_slug']
        path = labels_dir / 'events' / slug / 'signals.parquet'
        if not path.exists():
            continue
        signals = pd.read_parquet(path)
        if signals.empty:
            continue
        eligible = {time for event, time in choices['monitor_all'] if event == slug}
        signals = signals.loc[signals.time.isin(eligible)]
        if signals.empty:
            continue
        print(f'Shortlist execution: {slug}', flush=True)
        groups, mapping = group_builder(plan, load_frames(plan), set(signals.pair_id))
        for quote_policy in policies:
            cache = {}
            for model, selected in choices.items():
                times = frozenset(time for event, time in selected if event == slug)
                if times not in cache:
                    subset = signals.loc[signals.time.isin(times)]
                    details = run_event(subset, groups, mapping, experiment(spec))[0]
                    if len(details):
                        details = details.merge(subset[['pair_id', 'start_time', 'episode_id', 'original_spell_start', 'start_observed']],
                            on=['pair_id', 'start_time'], how='left', validate='one_to_one')
                    cache[times] = details
                detail = cache[times]
                if len(detail):
                    all_details.append(detail.assign(model=model, quote_policy=quote_policy))
        if all_details:
            pd.concat(all_details, ignore_index=True).to_parquet(output / 'attempts.parquet', index=False)
    details = pd.concat(all_details, ignore_index=True) if all_details else pd.DataFrame()
    for model in choices:
        for policy in policies:
            part = details.loc[details.model.eq(model) & details.quote_policy.eq(policy)] if len(details) else details
            taken = part.loc[part.attempted] if len(part) else part
            known = taken.loc[taken.pnl_usd.notna()] if len(taken) else taken
            positive = known.loc[known.pnl_usd > spec.assumed_other_cost_usd] if len(known) else known
            pnl = float(known.pnl_usd.sum()) - len(taken) * spec.assumed_other_cost_usd if len(known) else 0.
            summaries.append(dict(model=model, quote_policy=policy, top_n=top_n if model != 'monitor_all' else None,
                monitored_event_windows=len(choices[model]), attempts=len(taken),
                unknown_attempts=int(taken.pnl_usd.isna().sum()) if len(taken) else 0,
                partial_attempts=int(taken.status.str.startswith('partial_').sum()) if len(taken) else 0,
                positive_attempts=len(positive),
                positive_episodes=int(positive.episode_id.nunique()) if len(positive) else 0,
                future_positive_episodes=int(positive.loc[(positive.original_spell_start > positive.decision_time) & positive.start_observed, 'episode_id'].nunique()) if len(positive) else 0,
                known_pnl_usd=pnl,
                cumulative_purchase_cash_usd=float(known.entry_cost_usd.sum()) if len(known) else 0.))
    pd.DataFrame(summaries).to_csv(output / 'summary.csv', index=False)
    (output / 'run.json').write_text(json.dumps(dict(top_n=top_n,
        predictions_sha256=sha256(models_dir / 'predictions.parquet'),
        labels_sha256=sha256(labels_dir / 'labels.parquet'), source_sha256=sha256(__file__),
        final_lockbox_opened=False, quote_policies=policies,
        execution_book_policy='per_token_recording_extent',
        common_cash_cap_per_attempt_usd=spec.cash_cap_usd,
        portfolio_capital_model=False,
        interpretation='Conditional displayed-depth replay, not actual profit. Event reservations persist across selected windows and folds. Unknowns excluded from known-P&L subtotals. No measured competition, funding or global capital budget.'), indent=2) + '\n')
    return pd.DataFrame(summaries)
