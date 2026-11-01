from __future__ import annotations
from collections import OrderedDict
import numpy as np
import pandas as pd
from FYP.data.books import RawGroup
from FYP.data.coverage import classify_book, observation_fraction
from FYP.data.labels import label_windows
from FYP.data.quotes import quoted_net
from FYP.simulator.discovery import screen_token
from FYP.simulator.intervals import split_monitoring_hours
from FYP.simulator.sizing import choose_order

class BoundedGroup(RawGroup):
    """Backward reader with explicit context and per-member recording ends.

    Actual endpoints come from the timestamp audit, not the final row in the
    bounded price projection. No future book is read to seed an earlier state.
    """

    def __init__(self, frames, *, context_end):
        if not frames:
            raise ValueError('A group needs members')
        self.context_end = pd.Timestamp(context_end)
        if self.context_end.tzinfo is None:
            raise ValueError('UTC-aware context boundary required')
        self.frames, self.times, self.recording_ends, self.paths = ([], [], [], [])
        self._cache = OrderedDict()
        for original in frames:
            frame = original.loc[:, self.COLUMNS].copy()
            frame['snapshot_time'] = pd.to_datetime(frame.snapshot_time, utc=True)
            if frame.snapshot_time.isna().any() or frame.snapshot_time.gt(self.context_end).any():
                raise ValueError('Invalid timestamp or book beyond declared exit context')
            frame['indexed_at_ms'] = pd.to_numeric(frame.indexed_at_ms, errors='raise')
            frame = frame.sort_values(['snapshot_time', 'indexed_at_ms'], kind='stable', na_position='first').drop_duplicates('snapshot_time', keep='last').reset_index(drop=True)
            self.frames.append(frame)
            self.times.append(pd.DatetimeIndex(frame.snapshot_time))
            self.recording_ends.append(pd.Timestamp(original.attrs.get('recording_end', frame.snapshot_time.max())))
        self.last_time = min(self.context_end, max((t for t in self.recording_ends if pd.notna(t)), default=self.context_end))

    @classmethod
    def from_members(cls, group):
        raise ValueError('Bounded reader requires explicit frames and context')

    def at(self, when):
        when = pd.Timestamp(when)
        if when > self.context_end or any((pd.isna(end) or when > end for end in self.recording_ends)):
            return None
        return super().at(when)

def cohort_plans(protocol=None):
    from FYP.data.pipeline import plans
    return plans()

def sample_features(plan, frames, windows, context_end):
    readers = {t: BoundedGroup([f], context_end=context_end) for t, f in frames.items()}
    rows = []
    for when in windows:
        packages = [ids for ids, a, b in plan['packages'] if a <= when < b]
        if not packages:
            continue
        ids = sorted({t for p in packages for t in p})
        seen, clean, ages, spreads, depth, books = ([], [], [], [], [], {})
        for token in ids:
            reader = readers[token]
            position = reader.times[0].searchsorted(when, side='right') - 1
            if position < 0:
                seen.append(0.0)
                clean.append(np.nan)
                continue
            book = reader._book(0, int(position))
            books[token] = book
            seen.append(1.0)
            clean.append(float(book.clean))
            ages.append((when - book.stamp).total_seconds())
            if book.clean:
                spreads.append(book.asks.prices[0] - book.bids.prices[0])
            depth.append(book.asks.sizes.sum())
        values = np.asarray([quoted_net([books.get(t) for t in p]) for p in packages])
        rows.append(dict(event_slug=plan['event_slug'], stratum=plan['stratum'], time=when, eligible_snapshot=all(seen), x_active_packages=len(packages), x_active_tokens=len(ids), x_seen_fraction=np.mean(seen), x_clean_fraction=float(pd.Series(clean).eq(1).mean()), x_spread_mean=np.mean(spreads) if spreads else np.nan, x_spread_max=max(spreads) if spreads else np.nan, x_ask_depth_sum=float(sum(depth)) if depth else np.nan, x_book_age_max_s=max(ages) if ages else np.nan, x_best_quoted_net_usd=float(np.nanmax(values)) if np.isfinite(values).any() else np.nan, x_size_feasible_package_fraction=float(np.isfinite(values).mean()), x_positive_quoted_net_package_fraction=float((values > 0).mean())))
    return pd.DataFrame(rows)
SIGNAL_COLUMNS = ['pair_id', 'event_slug', 'start_time', 'end_time', 'episode_id', 'window', 'monitoring_end_time', 'start_observed', 'end_observed', 'original_spell_start', 'time', 'boundary_continuation']

def discover_bounded(plan, frames, spec, context_end):
    """The frozen integer-depth discovery rule with explicit final boundaries."""
    screens = {t: screen_token(f) for t, f in frames.items()}
    spells = []
    for number, (ids, a, b) in enumerate(plan['packages']):
        start, end = (max(a, pd.Timestamp(spec.data_start)), min(b, pd.Timestamp(spec.data_end)))
        if start >= end or any((screens[t].empty for t in ids)):
            continue
        stop = min(end, *(frames[t].attrs['recording_end'] for t in ids))
        if stop <= start:
            continue
        grid = pd.DatetimeIndex([start, stop])
        for t in ids:
            times = screens[t].index
            grid = grid.union(times[(times > start) & (times < stop)])
        grid = grid.sort_values()
        best, observed = (np.zeros(len(grid)), np.ones(len(grid), dtype=bool))
        for t in ids:
            table = screens[t].reindex(grid, method='pad')
            best += table.best_ask.to_numpy()
            observed &= table.observed.eq(True).to_numpy()
        good = np.zeros(len(grid), dtype=bool)
        group = BoundedGroup([frames[t] for t in ids], context_end=context_end)
        for i in np.flatnonzero(best < 1):
            if i == len(grid) - 1:
                continue
            books = group.at(grid[i])
            good[i] = books is not None and choose_order(books, min_quantity=5, max_quantity=None, cash_cap=None, min_limit_margin=0.0) is not None
        begins = np.flatnonzero(good & ~np.r_[False, good[:-1]])
        ends = np.flatnonzero(~good & np.r_[False, good[:-1]])
        pid = f'{plan['event_slug']}:{number}'
        for i, j in zip(begins, ends, strict=True):
            spells.append(dict(pair_id=pid, event_slug=plan['event_slug'], start_time=grid[i], end_time=grid[j], episode_id=f'{pid}:{grid[i].value}', window='study', monitoring_end_time=end, start_observed=bool(i > 0 and observed[i - 1]), end_observed=bool(grid[j] < stop and observed[j])))
    return pd.DataFrame(split_monitoring_hours(spells, spec), columns=SIGNAL_COLUMNS)

def quote_labels(plan, frames, windows, spec, context_end):
    states = {}
    for t, frame in frames.items():
        values = [classify_book(r.asks_json, r.bids_json, r.min_order_size, r.tick_size, quantities=None) for r in frame.itertuples(index=False)]
        state = pd.DataFrame(values, columns=['size_qualified', 'state', 'reason']).assign(time=frame.snapshot_time.to_numpy())
        stop = min(frame.attrs['recording_end'], context_end)
        if len(state) and state.time.iloc[-1] < stop:
            state = pd.concat([state, state.iloc[[-1]].assign(time=stop)], ignore_index=True)
        states[t] = state
    coverage = observation_fraction(plan, states, pd.DatetimeIndex(windows), spec)
    signals = discover_bounded(plan, frames, spec, context_end)
    signals = signals.loc[signals.time.isin(windows)].reset_index(drop=True)
    labels = label_windows(windows, signals, coverage, spec)
    peaks = []
    for pair, part in signals.groupby('pair_id'):
        ids = plan['packages'][int(pair.rsplit(':', 1)[1])][0]
        group = BoundedGroup([frames[t] for t in ids], context_end=context_end)
        updates = pd.DatetimeIndex([], tz='UTC')
        for t in ids:
            updates = updates.union(pd.DatetimeIndex(frames[t].snapshot_time))
        for when, spells in part.groupby('time'):
            best, peak, quantity, checked = (0.0, pd.NaT, 0, 0)
            for spell in spells.itertuples():
                grid = pd.DatetimeIndex([spell.start_time]).union(updates[(updates > spell.start_time) & (updates < spell.end_time)])
                for stamp in grid:
                    books = group.at(stamp)
                    order = None if books is None else choose_order(books, min_quantity=5, max_quantity=None, cash_cap=None, min_limit_margin=0.0)
                    if order is None:
                        raise ValueError('Qualified interval contains an unqualified quote')
                    checked += 1
                    if order['quoted_gross_usd'] > best:
                        best, peak, quantity = (order['quoted_gross_usd'], stamp, order['quantity'])
            peaks.append(dict(time=when, pair_id=pair, peak_quoted_usd=best, peak_time=peak, quantity=quantity, states_checked=checked))
    peaks = pd.DataFrame(peaks, columns=['time', 'pair_id', 'peak_quoted_usd', 'peak_time', 'quantity', 'states_checked'])
    values = peaks.groupby('time').peak_quoted_usd.max() if len(peaks) else pd.Series(dtype=float)
    labels['witnessed_peak_quoted_usd'] = labels.time.map(values).fillna(0.0)
    labels['value_target'] = labels.witnessed_peak_quoted_usd.where(labels.label_known)
    if not labels.value_target.gt(0).equals(labels.target.eq(1)):
        raise ValueError('Occurrence and peak quote labels disagree')
    return (labels, signals, peaks)
