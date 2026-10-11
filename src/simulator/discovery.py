"""Depth-qualified basket signals and monitoring-window boundaries."""
import numpy as np
import pandas as pd
from src.data.books import RawGroup, Ladder
from src.data.coverage import UNKNOWN, BLOCKED
from src.config import SPEC

def token_quotes(frame, spec=SPEC):
    group = RawGroup([frame])
    columns = [f'{side}_{q}' for side in ('cost', 'limit') for q in spec.quantities]
    values = []
    for row in group.frames[0].itertuples(index=False):
        asks, bids = Ladder(RawGroup._levels(row.asks_json), True), Ladder(RawGroup._levels(row.bids_json), False)
        cost, limit = asks._quotes(np.asarray(spec.quantities, dtype=float))
        valid = bool(len(asks) and len(bids) and bids.prices[0] <= asks.prices[0])
        metadata = np.isfinite([row.min_order_size, row.tick_size]).all() and row.min_order_size > 0 and row.tick_size > 0
        allowed = np.zeros(len(spec.quantities), dtype=bool)
        if valid and metadata:
            allowed = np.asarray(spec.quantities) >= row.min_order_size
            allowed &= np.isfinite(cost) & np.isclose(limit / row.tick_size, np.rint(limit / row.tick_size), atol=1e-7, rtol=0)
        values.append(np.r_[np.where(allowed, cost, np.nan), np.where(allowed, limit, np.nan)])
    return pd.DataFrame(values, columns=columns, index=group.times[0])


def split_monitoring_hours(spells, spec=SPEC):
    """An already-open gap is visible at selection; entries respect the window.

    Reservations in the scheduler persist between hours, so splitting is not a
    licence to reuse the same displayed liquidity.
    """
    rows = []
    for spell in spells:
        when = spell['start_time']
        while when < spell['end_time']:
            hour = when.floor(spec.frequency)
            end = min(spell['end_time'], hour + spec.horizon)
            rows.append({**spell, 'original_spell_start': spell['start_time'],
                'start_time': when, 'end_time': end, 'time': hour,
                'monitoring_end_time': min(spell['monitoring_end_time'], hour + spec.horizon),
                'boundary_continuation': when != spell['start_time']})
            when = end
    return rows

def discover_observed_package(ids, quotes, states, start, end, slug, pid, spec=SPEC):
    """Use causal pre-start seeds; never extend a leg beyond its recorded end.

A new-gap onset needs a preceding observed non-opportunity, rather than a
transition from missing data. At activation, an existing gap is left-censored.
"""
    start = max(pd.Timestamp(start), pd.Timestamp(spec.development_start))
    end = min(pd.Timestamp(end), pd.Timestamp(spec.lockbox_start))
    if any(quotes[t].empty for t in ids) or start >= end:
        return []
    stop = min(end, *(quotes[t].index[-1] for t in ids))
    if stop <= start:
        return []
    grid = pd.DatetimeIndex([start, stop])
    for token in ids:
        times = quotes[token].index
        grid = grid.union(times[(times > start) & (times < stop)])
    grid = grid.sort_values()
    cost = np.zeros((len(grid), len(spec.quantities)))
    limits = np.zeros_like(cost)
    all_seen, blocked = np.ones(len(grid), dtype=bool), np.zeros(len(grid), dtype=bool)
    for token in ids:
        table = quotes[token].reindex(grid, method='pad')
        cost += table[[f'cost_{q}' for q in spec.quantities]].to_numpy()
        limits += table[[f'limit_{q}' for q in spec.quantities]].to_numpy()
        state = states[token].set_index('time').state.reindex(grid, method='pad')
        all_seen &= state.notna().to_numpy() & state.ne(UNKNOWN).to_numpy()
        blocked |= state.eq(BLOCKED).to_numpy()
    q = np.asarray(spec.quantities)
    reserve = limits * q
    good = ((q - cost > spec.assumed_other_cost_usd + 1e-10)
            & (q - reserve > spec.assumed_other_cost_usd + 1e-10)
            & (reserve <= spec.cash_cap_usd)).any(axis=1)
    good[-1] = False  # terminal boundary is not an entry signal
    observed = all_seen | blocked
    begins = np.flatnonzero(good & ~np.r_[False, good[:-1]])
    ends = np.flatnonzero(~good & np.r_[False, good[:-1]])
    spells = []
    for i in begins:
        j = ends[ends > i][0]
        spells.append(dict(pair_id=pid, event_slug=slug, start_time=grid[i], end_time=grid[j],
            episode_id=f'{pid}:{grid[i].value}', window='development',
            monitoring_end_time=end, start_observed=bool(i > 0 and observed[i - 1]),
            end_observed=bool(grid[j] < stop and observed[j])))
    return spells
