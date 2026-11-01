from __future__ import annotations
import json
import numpy as np
import pandas as pd
from FYP.data.books import RawGroup
from FYP.simulator.execution import Experiment, _usable, _exit_values
from FYP.simulator.reservations import quote_key
from FYP.simulator.sizing import choose_order

def run_scenario(signals: pd.DataFrame, group: RawGroup, experiment: Experiment, *, entry_end, context_end) -> pd.DataFrame:
    """Process one event chronologically, preserving reservations across folds.

    An individual FOK is *assumed* to fill if fresh displayed depth within its
    frozen price limit suffices at arrival. This is plausibility, not a fill
    probability estimate. Unknown book state cannot be interpreted as no fill.
    """
    if experiment.wait_s < 0 or experiment.delay_s < 0 or experiment.unwind_delay_s < 0:
        raise ValueError('Times must be nonnegative')
    if isinstance(experiment.monitoring_minutes, bool) or not isinstance(experiment.monitoring_minutes, int) or experiment.monitoring_minutes <= 0:
        raise ValueError('Monitoring duration must be positive integer minutes')
    if signals.event_slug.nunique() > 1:
        raise ValueError('Each scenario call must contain exactly one event')
    rows, reserved = ([], None)
    busy_until = None
    cutoff = pd.Timestamp(entry_end)
    exit_cutoff = pd.Timestamp(context_end)
    if exit_cutoff < cutoff:
        raise ValueError('Exit context cannot end before entries')
    for signal in signals.sort_values('start_time').itertuples(index=False):
        confirm = signal.start_time + pd.Timedelta(seconds=experiment.wait_s)
        arrival = confirm + pd.Timedelta(seconds=experiment.delay_s)
        unwind = arrival + pd.Timedelta(seconds=experiment.unwind_delay_s)
        row = {'event_slug': signal.event_slug, 'window': signal.window, 'decision_time': signal.time, 'start_time': signal.start_time, 'confirmation_time': confirm, 'arrival_time': arrival, 'unwind_time': unwind, 'status': 'not_confirmed', 'attempted': False, 'quantity': 0, 'n_members': 0, 'n_filled_legs': 0, 'quoted_gross_usd': np.nan, 'reserved_cash_usd': 0.0, 'limits_json': '[]', 'entry_cost_usd': np.nan, 'pnl_usd': np.nan, 'confirmation_max_age_s': np.nan, 'arrival_max_age_s': np.nan, 'filled_legs_json': '[]', 'leg_costs_json': '[]'}
        rows.append(row)
        if experiment.wait_s > 0 and confirm >= signal.end_time:
            continue
        monitoring_end = getattr(signal, 'monitoring_end_time', None)
        outside = arrival >= monitoring_end if monitoring_end is not None else arrival >= signal.time + pd.Timedelta(minutes=experiment.monitoring_minutes)
        if outside or arrival >= cutoff:
            row['status'] = 'outside_monitoring_window'
            continue
        if busy_until is not None and confirm < busy_until:
            row['status'] = 'in_flight'
            continue
        before = group.at(confirm)
        if before is None or not all((_usable(b, confirm, experiment.freshness_s) for b in before)):
            row['status'] = 'confirmation_unusable'
            continue
        stamps = tuple((b.stamp for b in before))
        row['n_members'] = len(before)
        row['confirmation_max_age_s'] = max(((confirm - stamp).total_seconds() for stamp in stamps))
        if reserved is not None and (not all((now > old for now, old in zip(stamps, reserved, strict=True)))):
            row['status'] = 'quote_reuse_blocked'
            continue
        order = choose_order(before, mode=experiment.sizing, min_gross=experiment.min_entry_gross_usd, max_quantity=experiment.max_quantity, cash_cap=experiment.cash_cap_usd, min_quantity=experiment.min_quantity, min_limit_margin=experiment.min_limit_margin_usd)
        if order is None:
            row['status'] = 'no_acceptable_size'
            continue
        q, limits = (order['quantity'], order['limits'])
        row.update({'attempted': True, 'quantity': q, 'quoted_gross_usd': order['quoted_gross_usd'], 'reserved_cash_usd': order['reserved_cash_usd'], 'limits_json': json.dumps(limits)})
        reserved, busy_until = (stamps, unwind)
        after = group.at(arrival)
        if after is not None:
            reserved = tuple((b.stamp for b in after))
            row['arrival_max_age_s'] = max(((arrival - b.stamp).total_seconds() for b in after))
        if after is None or not all((_usable(b, arrival, experiment.freshness_s) for b in after)):
            row['status'] = 'unknown_arrival'
            continue
        if not all((np.isfinite(b.min_order_size) and np.isfinite(b.tick_size) and (b.tick_size > 0) for b in after)):
            row['status'] = 'unknown_arrival'
            continue
        fills = []
        costs = np.zeros(len(after))
        for i, (book, limit) in enumerate(zip(after, limits, strict=True)):
            tick_valid = abs(limit / book.tick_size - round(limit / book.tick_size)) < 1e-07
            quote = book.asks.quote(q, limit=limit) if q >= book.min_order_size and tick_valid else None
            if quote is not None:
                fills.append(i)
                costs[i] = quote[0]
        row.update({'n_filled_legs': len(fills), 'entry_cost_usd': float(costs.sum()), 'filled_legs_json': json.dumps(fills), 'leg_costs_json': json.dumps(costs.tolist())})
        if not fills:
            row.update({'status': 'no_fill', 'pnl_usd': 0.0})
            continue
        exit_books = group.at(unwind) if unwind <= exit_cutoff else None
        exits = _exit_values(exit_books, q, unwind, experiment.freshness_s, len(after))
        if len(fills) == len(after):
            row.update({'status': 'complete', 'pnl_usd': float(q - costs.sum())})
        elif np.isfinite(exits[fills]).all():
            row.update({'status': 'partial_unwound', 'pnl_usd': float((exits[fills] - costs[fills]).sum())})
        else:
            row['status'] = 'partial_unknown_unwind'
    return pd.DataFrame(rows)

def run_event(signals: pd.DataFrame, groups: dict[str, RawGroup], pair_tokens: dict[str, tuple[str, str]], experiment: Experiment, *, entry_end, context_end) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One event-wide attempt in flight; rank simultaneous starts by entry gross.

    New signals only (no retries). Shared token ask versions are reserved across
    ALL pairs. No realised arrival outcomes enter the selection ranking.
    """
    reserved, busy_until, attempts, audit = ({}, None, [], [])
    if signals.empty:
        return (pd.DataFrame(), pd.DataFrame())
    if signals.event_slug.nunique() != 1:
        raise ValueError('One event per scheduler')
    signals = signals.assign(confirm=signals.start_time + pd.Timedelta(seconds=experiment.wait_s))
    for when, batch in signals.groupby('confirm', sort=True):
        candidates = []
        for _, signal in batch.sort_values('pair_id').iterrows():
            pid = signal.pair_id
            reason = None
            arrival = when + pd.Timedelta(seconds=experiment.delay_s)
            if experiment.wait_s > 0 and when >= signal.end_time:
                reason = 'not_confirmed'
            elif arrival >= min(signal.monitoring_end_time, pd.Timestamp(entry_end)):
                reason = 'outside_monitoring_window'
            elif busy_until is not None and when < busy_until:
                reason = 'event_in_flight'
            before = groups[pid].at(when) if reason is None else None
            if reason is None and (before is None or not all((_usable(b, when, experiment.freshness_s) for b in before))):
                reason = 'confirmation_unusable'
            if reason is None:
                for token, book in zip(pair_tokens[pid], before, strict=True):
                    if token in reserved:
                        stamp, key = reserved[token]
                        if book.stamp <= stamp or quote_key(book) == key:
                            reason = 'shared_quote_reserved'
                            break
            order = choose_order(before, mode=experiment.sizing, min_gross=experiment.min_entry_gross_usd, max_quantity=experiment.max_quantity, cash_cap=experiment.cash_cap_usd, min_quantity=experiment.min_quantity, min_limit_margin=experiment.min_limit_margin_usd) if reason is None else None
            if reason is None and order is None:
                reason = 'no_acceptable_size'
            audit.append(dict(pair_id=pid, event_slug=signal.event_slug, confirmation_time=when, reason=reason or 'candidate', quoted_gross_usd=None if order is None else order['quoted_gross_usd']))
            if reason is None:
                candidates.append((order['quoted_gross_usd'], pid, signal, before, len(audit) - 1))
        if not candidates:
            continue
        candidates.sort(key=lambda c: (-c[0], c[1]))
        for _, _, _, _, audit_index in candidates[1:]:
            audit[audit_index]['reason'] = 'competing_same_time'
        _, pid, signal, before, audit_index = candidates[0]
        result = run_scenario(pd.DataFrame([signal]), groups[pid], experiment, entry_end=entry_end, context_end=context_end)
        result['pair_id'] = pid
        attempts.append(result)
        audit[audit_index]['reason'] = 'selected'
        if bool(result.at[0, 'attempted']):
            arrival, unwind = (result.at[0, 'arrival_time'], result.at[0, 'unwind_time'])
            busy_until = unwind
            versions = groups[pid].at(unwind) or groups[pid].at(arrival) or before
            for token, book in zip(pair_tokens[pid], versions, strict=True):
                reserved[token] = (book.stamp, quote_key(book))
    return (pd.concat(attempts, ignore_index=True) if attempts else pd.DataFrame(), pd.DataFrame(audit))
