from __future__ import annotations
import numpy as np
import pandas as pd
from FYP.config import SPEC
from FYP.data.books import RawGroup, ObservationGroup
from FYP.data.coverage import parse_side
from FYP.simulator.intervals import split_monitoring_hours
from FYP.simulator.sizing import choose_order
from FYP.simulator.summary import summarize

def screen_token(frame):
    """Cheap necessary condition; final qualification always checks full depth."""
    rows = []
    for row in frame.itertuples(index=False):
        try:
            asks = parse_side(row.asks_json, True)
            bids = parse_side(row.bids_json, False)
            observed = bool(np.isfinite([row.min_order_size, row.tick_size]).all() and row.min_order_size > 0 and (0 < row.tick_size <= 1) and (not (asks and bids and (bids[0][0] > asks[0][0]))))
            best = asks[0][0] if observed and asks and bids else np.nan
        except (ValueError, TypeError, KeyError, OverflowError):
            observed, best = (False, np.nan)
        rows.append((observed, best))
    return pd.DataFrame(rows, columns=['observed', 'best_ask'], index=pd.DatetimeIndex(frame.snapshot_time))

def discover_depth_signals(plan, frames, setup, spec=SPEC):
    """Recheck every package with no fixed quantity grid or artificial caps.

    The sum of best asks is only a necessary prefilter. Every potential state
    is qualified by the same decision-time integer-depth optimizer as execution.
    """
    frames = {token: RawGroup([frame]).frames[0] for token, frame in frames.items()}
    screens = {token: screen_token(frame) for token, frame in frames.items()}
    spells = []
    for number, (ids, start, end) in enumerate(plan['packages']):
        start = max(pd.Timestamp(start), pd.Timestamp(spec.data_start))
        end = min(pd.Timestamp(end), pd.Timestamp(spec.data_end))
        if start >= end or any((screens[token].empty for token in ids)):
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
            good[i] = books is not None and choose_order(books, mode=setup.sizing, min_gross=setup.min_entry_gross_usd, min_quantity=setup.min_quantity, max_quantity=setup.max_quantity, cash_cap=setup.cash_cap_usd, min_limit_margin=setup.min_limit_margin_usd) is not None
        begins = np.flatnonzero(good & ~np.r_[False, good[:-1]])
        ends = np.flatnonzero(~good & np.r_[False, good[:-1]])
        pid = f'{plan['event_slug']}:{number}'
        for i, j in zip(begins, ends, strict=True):
            spells.append(dict(pair_id=pid, event_slug=plan['event_slug'], start_time=grid[i], end_time=grid[j], episode_id=f'{pid}:{grid[i].value}', window='study', monitoring_end_time=end, start_observed=bool(i > 0 and observed[i - 1]), end_observed=bool(grid[j] < stop and observed[j])))
    return pd.DataFrame(split_monitoring_hours(spells, spec))

def capacity_summary(details, model, spec=SPEC):
    row = summarize(details, model, 'uncapped_100ms', 0.1, spec.unwind_delay_s, spec.assumed_other_cost_usd)
    taken = details.loc[details.attempted] if len(details) else details
    row.update(max_quantity=None, cash_cap_usd=None, median_quantity=float(taken.quantity.median()) if len(taken) else 0.0, largest_quantity=int(taken.quantity.max()) if len(taken) else 0, largest_reserved_cash_usd=float(taken.reserved_cash_usd.max()) if len(taken) else 0.0, cumulative_purchase_cash_usd=float(taken.entry_cost_usd.sum()) if len(taken) else 0.0, median_completed_profit_usd=float(taken.loc[taken.status.eq('complete'), 'pnl_usd'].median()) if len(taken) else 0.0, unknown_entry_cash_usd=float(taken.loc[taken.pnl_usd.isna(), 'entry_cost_usd'].sum()) if len(taken) else 0.0)
    return row
