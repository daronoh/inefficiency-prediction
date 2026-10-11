"""Uncapped displayed-depth replay with fixed out-of-sample event selections."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import numpy as np
import pandas as pd
from src.common import sha256
from src.config import SPEC, spec_hash
from src.data.books import RawGroup, ObservationGroup
from src.data.coverage import parse_side
from src.data.io import load_frames, observation_groups
from src.data.universe import build_plan
from src.models.evaluation import ranked
from src.simulator.discovery import split_monitoring_hours
from src.simulator.execution import experiment
from src.simulator.scheduler import run_event
from src.simulator.sizing import choose_order
from src.simulator.sensitivity import summarize, spec_hash_from_contract


def screen_token(frame):
    """Cheap necessary condition; final qualification always checks full depth."""
    rows = []
    for row in frame.itertuples(index=False):
        try:
            asks = parse_side(row.asks_json, True)
            bids = parse_side(row.bids_json, False)
            observed = bool(np.isfinite([row.min_order_size, row.tick_size]).all()
                and row.min_order_size > 0 and 0 < row.tick_size <= 1
                and not (asks and bids and bids[0][0] > asks[0][0]))
            best = asks[0][0] if observed and asks and bids else np.nan
        except (ValueError, TypeError, KeyError, OverflowError):
            observed, best = False, np.nan
        rows.append((observed, best))
    return pd.DataFrame(rows, columns=['observed', 'best_ask'], index=pd.DatetimeIndex(frame.snapshot_time))


def discover_depth_signals(plan, frames, setup, spec=SPEC):
    """Recheck every package with no fixed quantity grid or artificial caps.

    The sum of best asks is only a necessary prefilter. Every potential state
    is qualified by the same decision-time integer-depth optimizer as execution.
    This discovery is independent of the old capped signals and their labels.
    """
    frames = {token: RawGroup([frame]).frames[0] for token, frame in frames.items()}
    screens = {token: screen_token(frame) for token, frame in frames.items()}
    spells = []
    for number, (ids, start, end) in enumerate(plan['packages']):
        start = max(pd.Timestamp(start), pd.Timestamp(spec.development_start))
        end = min(pd.Timestamp(end), pd.Timestamp(spec.lockbox_start))
        if start >= end or any(screens[token].empty for token in ids):
            continue
        stop = min(end, *(screens[token].index[-1] for token in ids))
        if stop <= start:
            continue
        grid = pd.DatetimeIndex([start, stop])
        for token in ids:
            times = screens[token].index
            grid = grid.union(times[(times > start) & (times < stop)])
        grid = grid.sort_values()
        best = np.zeros(len(grid))
        observed = np.ones(len(grid), dtype=bool)
        for token in ids:
            table = screens[token].reindex(grid, method='pad')
            best += table.best_ask.to_numpy()
            observed &= table.observed.eq(True).to_numpy()
        good = np.zeros(len(grid), dtype=bool)
        possible = np.flatnonzero(best < 1)
        group = ObservationGroup([frames[token] for token in ids]) if len(possible) else None
        for i in possible:
            if i == len(grid) - 1:
                continue
            books = group.at(grid[i])
            good[i] = books is not None and choose_order(books, mode=setup.sizing,
                min_gross=setup.min_entry_gross_usd, min_quantity=setup.min_quantity,
                max_quantity=setup.max_quantity, cash_cap=setup.cash_cap_usd,
                min_limit_margin=setup.min_limit_margin_usd) is not None
        begins = np.flatnonzero(good & ~np.r_[False, good[:-1]])
        ends = np.flatnonzero(~good & np.r_[False, good[:-1]])
        pid = f"{plan['event_slug']}:{number}"
        for i, j in zip(begins, ends, strict=True):
            spells.append(dict(pair_id=pid, event_slug=plan['event_slug'], start_time=grid[i], end_time=grid[j],
                episode_id=f'{pid}:{grid[i].value}', window='development', monitoring_end_time=end,
                start_observed=bool(i > 0 and observed[i-1]), end_observed=bool(grid[j] < stop and observed[j])))
    return pd.DataFrame(split_monitoring_hours(spells, spec))


def capacity_summary(details, model, spec=SPEC):
    row = summarize(details, model, 'uncapped_100ms', .1, spec.unwind_delay_s, spec.assumed_other_cost_usd)
    taken = details.loc[details.attempted] if len(details) else details
    row.update(max_quantity=None, cash_cap_usd=None,
        median_quantity=float(taken.quantity.median()) if len(taken) else 0.,
        largest_quantity=int(taken.quantity.max()) if len(taken) else 0,
        largest_reserved_cash_usd=float(taken.reserved_cash_usd.max()) if len(taken) else 0.,
        cumulative_purchase_cash_usd=float(taken.entry_cost_usd.sum()) if len(taken) else 0.,
        median_completed_profit_usd=float(taken.loc[taken.status.eq('complete'), 'pnl_usd'].median()) if len(taken) else 0.,
        unknown_entry_cash_usd=float(taken.loc[taken.pnl_usd.isna(), 'entry_cost_usd'].sum()) if len(taken) else 0.)
    return row


def replay_capacity(output, labels_dir, models_dir, spec=SPEC, top_n=5):
    """Rebuild uncapped candidates, keeping models and eligible selections fixed."""
    output, labels_dir, models_dir = Path(output), Path(labels_dir), Path(models_dir)
    design = json.loads((models_dir/'design.json').read_text())
    if design['label_sha256'] != sha256(labels_dir/'labels.parquet') or spec_hash(spec) != spec_hash_from_contract(design['contract']):
        raise ValueError('Capacity comparison requires the original prediction/label contract')
    prediction = pd.read_parquet(models_dir/'predictions.parquet')
    if prediction.time.min() < pd.Timestamp(spec.development_start) or prediction.time.max() >= pd.Timestamp(spec.lockbox_start):
        raise ValueError('Capacity replay is development-only')
    choices = {}
    for name, frame in prediction.groupby('model'):
        selected = ranked(frame, frame.score, top_n)
        choices[name] = set(zip(selected.event_slug, selected.time))
    pool = prediction.drop_duplicates(['event_slug', 'time'])
    choices['monitor_all'] = set(zip(pool.event_slug, pool.time))
    setup = replace(experiment(spec), delay_s=.1, max_quantity=None, cash_cap_usd=None, compute_failure_stress=False)
    output.mkdir(parents=True, exist_ok=False)
    plans, _ = build_plan('all')
    details, signal_tables, discovery_audit, inventories = [], [], [], []
    for plan in plans:
        slug = plan['event_slug']
        eligible = {time for event, time in choices['monitor_all'] if event == slug}
        if not eligible:
            continue
        print(f'Uncapped discovery and 100 ms replay: {slug}', flush=True)
        frames = load_frames(plan)
        signals = discover_depth_signals(plan, frames, setup, spec)
        signals = signals.loc[signals.time.isin(eligible)] if len(signals) else signals
        original_path = labels_dir/'events'/slug/'signals.parquet'
        original = pd.read_parquet(original_path) if original_path.exists() else pd.DataFrame()
        original = original.loc[original.time.isin(eligible)] if len(original) else original
        old_keys = set(zip(original.pair_id, original.start_time)) if len(original) else set()
        new_keys = set(zip(signals.pair_id, signals.start_time)) if len(signals) else set()
        discovery_audit.append(dict(event_slug=slug, original_signals=len(original), uncapped_signals=len(signals),
            shared_starts=len(old_keys & new_keys), added_starts=len(new_keys-old_keys), removed_starts=len(old_keys-new_keys)))
        inventories.extend(dict(event_slug=slug, token_id=token, rows=len(frame),
            input_sha256=sha256(plan['paths'][token])) for token, frame in frames.items())
        if signals.empty:
            continue
        signal_tables.append(signals)
        groups, mapping = observation_groups(plan, frames, set(signals.pair_id))
        cache = {}
        for model, selected in choices.items():
            times = frozenset(time for event, time in selected if event == slug)
            if times not in cache:
                cache[times] = run_event(signals.loc[signals.time.isin(times)], groups, mapping, setup)[0]
            if len(cache[times]):
                details.append(cache[times].assign(model=model))
        pd.concat(details, ignore_index=True).to_parquet(output/'attempts.parquet', index=False)
    combined = pd.concat(details, ignore_index=True) if details else pd.DataFrame()
    if not details:
        combined.to_parquet(output/'attempts.parquet', index=False)
    (pd.concat(signal_tables, ignore_index=True) if signal_tables else pd.DataFrame()).to_parquet(output/'signals.parquet', index=False)
    summary = pd.DataFrame([capacity_summary(combined.loc[combined.model.eq(model)] if len(combined) else combined, model, spec)
                            for model in choices])
    summary.to_csv(output/'summary.csv', index=False)
    manifest = dict(execution=asdict(setup), model_contract=asdict(spec), top_n=top_n,
        predictions_sha256=sha256(models_dir/'predictions.parquet'), label_sha256=sha256(labels_dir/'labels.parquet'),
        sources={str(path.name): sha256(path) for path in Path(__file__).parent.glob('*.py')},
        labels_rebuilt=False, models_retrained=False, entry_signals_rebuilt=True,
        eligible_population_changed=False, final_lockbox_opened=False, failure_probability=None,
        discovery_audit=discovery_audit, input_inventory=inventories,
        interpretation='Fixed out-of-sample selections; full integer-depth discovery and sizing without artificial cash/share caps. '
        '100 ms assumed arrival, original unwind/hold and reservations. Actual partial losses retained; no imposed failure probability. '
        'Known P&L excludes unknown outcomes. Unlimited funding assumed; not a portfolio return or market-wide opportunity census.')
    # JSON null represents no freshness cutoff rather than nonstandard Infinity.
    manifest['execution']['freshness_s'] = None
    (output/'run.json').write_text(json.dumps(manifest, indent=2, allow_nan=False)+'\n')
    return summary
