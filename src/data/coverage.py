"""Distinguish observed no-entry states from missing evidence over active windows."""
import json
import math
import numpy as np
import pandas as pd
from src.config import SPEC

UNKNOWN, BLOCKED, TRADABLE = 0, 1, 2

def parse_side(value, ascending):
    """An explicit [] is observed absence; null or broken JSON is unknown."""
    if value is None or (not isinstance(value, (str, list)) and pd.isna(value)):
        raise ValueError('Missing side')
    levels = json.loads(value) if isinstance(value, str) else value
    if not isinstance(levels, list):
        raise ValueError('Side is not a list')
    parsed = []
    for level in levels:
        price, size = float(level['price']), float(level['size'])
        if not math.isfinite(price) or not math.isfinite(size) or not 0 <= price <= 1 or size < 0:
            raise ValueError('Invalid price/size')
        if size:
            parsed.append((price, size))
    return sorted(parsed, key=lambda item: item[0], reverse=not ascending)


def classify_book(asks_raw, bids_raw, minimum, tick, quantities=SPEC.quantities):
    """Classify buy-entry feasibility, separately preserving observed blockers."""
    try:
        asks, bids = parse_side(asks_raw, True), parse_side(bids_raw, False)
    except (ValueError, TypeError, KeyError, OverflowError):
        return False, UNKNOWN, 'missing_or_malformed_book'
    try:
        metadata = math.isfinite(minimum) and math.isfinite(tick) and minimum > 0 and tick > 0
    except TypeError:
        metadata = False
    if not metadata:
        return False, UNKNOWN, 'invalid_metadata'
    if asks and bids and bids[0][0] > asks[0][0]:
        return False, UNKNOWN, 'crossed_book'
    if not asks:
        return False, BLOCKED, 'empty_asks'
    if not bids:
        return False, BLOCKED, 'empty_bids'
    if quantities is None:
        if tick > 1:
            return False, UNKNOWN, 'invalid_metadata'
        lower, cumulative = max(5, math.ceil(minimum)), 0.
        for price, size in asks:
            first = max(lower, math.floor(cumulative) + 1)
            cumulative += size
            if first <= math.floor(cumulative) and abs(price / tick - round(price / tick)) <= 1e-7:
                return True, TRADABLE, ''
        return False, BLOCKED, 'no_integer_quantity'
    # Preserve the exact baseline grid/tick test, including cumulative float sums.
    cumulative = 0.
    position = 0
    for quantity in quantities:
        while position < len(asks) and cumulative < quantity:
            cumulative += asks[position][1]
            position += 1
        if quantity >= minimum and cumulative >= quantity:
            price = asks[position - 1][0]
            if abs(price / tick - round(price / tick)) <= 1e-7:
                return True, TRADABLE, ''
    return False, BLOCKED, 'no_grid_quantity'


def merge_intervals(intervals):
    merged = []
    for a, b in sorted((int(a), int(b)) for a, b in intervals if a < b):
        if merged and a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def interval_fraction(intervals, windows, horizon):
    """Time covered by a union of half-open intervals, with no double counting."""
    starts = windows.as_unit('ns').asi8
    ends = starts + horizon.value
    covered = np.zeros(len(windows))
    for a, b in merge_intervals(intervals):
        left = np.searchsorted(ends, a, side='right')
        right = np.searchsorted(starts, b, side='left')
        covered[left:right] += np.maximum(0, np.minimum(ends[left:right], b) - np.maximum(starts[left:right], a))
    return np.clip(covered / horizon.value, 0., 1.)


def package_intervals(member_states, start, end):
    """Classify intervals as observed, blocked or unknown for buy entry.

One observed blocked leg rules out the entire package even if another leg is
unknown. Otherwise all legs must be observed.
"""
    lo, hi = pd.Timestamp(start).value, pd.Timestamp(end).value
    if lo >= hi:
        return [], [], []
    points = [np.array([lo, hi], dtype=np.int64)]
    for states in member_states:
        if states.empty:
            continue
        times = pd.DatetimeIndex(states.time).as_unit('ns').asi8
        points.append(times[(times > lo) & (times < hi)])
    grid = np.unique(np.concatenate(points))
    left, right = grid[:-1], grid[1:]
    all_seen = np.ones(len(left), dtype=bool)
    any_blocked = np.zeros(len(left), dtype=bool)
    for states in member_states:
        if states.empty:
            all_seen[:] = False
            continue
        times = pd.DatetimeIndex(states.time).as_unit('ns').asi8
        index = np.searchsorted(times, left, side='right') - 1
        present = (index >= 0) & (left < times[-1])
        safe = np.maximum(index, 0)
        values = states.state.to_numpy()[safe]
        all_seen &= present & (values != UNKNOWN)
        any_blocked |= present & (values == BLOCKED)
    unknown = ~all_seen & ~any_blocked
    partial = ~all_seen
    return list(zip(left[unknown], right[unknown])), list(zip(left[partial], right[partial])), list(zip(left[any_blocked], right[any_blocked]))

def classify_frame(frame, spec=SPEC):
    rows = [classify_book(r.asks_json, r.bids_json, r.min_order_size, r.tick_size, spec.quantities)
            for r in frame.itertuples(index=False)]
    states = pd.DataFrame(rows, columns=['size_qualified', 'state', 'reason'])
    states.insert(0, 'time', frame.snapshot_time.to_numpy())
    return states


def observation_fraction(plan, states, windows, spec=SPEC):
    unknown = []
    for ids, start, end in plan['packages']:
        start = max(pd.Timestamp(start), windows.min())
        end = min(pd.Timestamp(end), windows.max() + spec.horizon)
        if start < end:
            unknown.extend(package_intervals([states[t] for t in ids], start, end)[0])
    return 1 - interval_fraction(unknown, windows, spec.horizon)
