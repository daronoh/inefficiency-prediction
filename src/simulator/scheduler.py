"""Event-wide scheduling and shared-token liquidity reservations."""
import pandas as pd
from src.data.books import RawGroup
from src.simulator.sizing import choose_order
from src.simulator.execution import Experiment, run_scenario, _usable
from src.config import SPEC

def quote_key(book) -> tuple:
    """Identical ask quantities/prices are not new liquidity just due to timestamp."""
    return tuple(zip(book.asks.prices.tolist(), book.asks.sizes.tolist()))


def run_event(signals: pd.DataFrame, groups: dict[str, RawGroup], pair_tokens: dict[str, tuple[str, str]],
              experiment: Experiment) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One event-wide attempt in flight; rank simultaneous starts by entry gross.

    New signals only (no retries). Shared token ask versions are reserved across
    ALL pairs. No realised arrival outcomes enter the selection ranking.
    """
    reserved, busy_until, attempts, audit = {}, None, [], []
    if signals.empty:
        return pd.DataFrame(), pd.DataFrame()
    if signals.event_slug.nunique() != 1:
        raise ValueError("One event per scheduler")
    signals = signals.assign(confirm=signals.start_time + pd.Timedelta(seconds=experiment.wait_s))
    for when, batch in signals.groupby("confirm", sort=True):
        candidates = []
        for _, signal in batch.sort_values("pair_id").iterrows():
            pid = signal.pair_id
            reason = None
            arrival = when + pd.Timedelta(seconds=experiment.delay_s)
            if experiment.wait_s > 0 and when >= signal.end_time:
                reason = "not_confirmed"
            elif arrival >= min(signal.monitoring_end_time, pd.Timestamp(SPEC.lockbox_start)):
                reason = "outside_monitoring_window"
            elif busy_until is not None and when < busy_until:
                reason = "event_in_flight"
            before = groups[pid].at(when) if reason is None else None
            if reason is None and (before is None or not all(_usable(b, when, experiment.freshness_s) for b in before)):
                reason = "confirmation_unusable"
            if reason is None:
                for token, book in zip(pair_tokens[pid], before, strict=True):
                    if token in reserved:
                        stamp, key = reserved[token]
                        if book.stamp <= stamp or quote_key(book) == key:
                            reason = "shared_quote_reserved"
                            break
            order = choose_order(before, mode=experiment.sizing, min_gross=experiment.min_entry_gross_usd,
                                 max_quantity=experiment.max_quantity, cash_cap=experiment.cash_cap_usd,
                                 min_quantity=experiment.min_quantity,
                                 min_limit_margin=experiment.min_limit_margin_usd) if reason is None else None
            if reason is None and order is None:
                reason = "no_acceptable_size"
            audit.append(dict(pair_id=pid, event_slug=signal.event_slug, confirmation_time=when,
                              reason=reason or "candidate", quoted_gross_usd=None if order is None else order["quoted_gross_usd"]))
            if reason is None:
                candidates.append((order["quoted_gross_usd"], pid, signal, before, len(audit)-1))
        if not candidates:
            continue
        candidates.sort(key=lambda c: (-c[0], c[1]))
        for _, _, _, _, audit_index in candidates[1:]:
            audit[audit_index]["reason"] = "competing_same_time"
        _, pid, signal, before, audit_index = candidates[0]
        result = run_scenario(pd.DataFrame([signal]), groups[pid], experiment)
        result["pair_id"] = pid
        attempts.append(result)
        audit[audit_index]["reason"] = "selected"
        if bool(result.at[0, "attempted"]):
            arrival, unwind = result.at[0, "arrival_time"], result.at[0, "unwind_time"]
            busy_until = unwind
            # Busy period prevents use of these inspected future versions before
            # they become observable; reserves both submitted legs even on failure.
            versions = groups[pid].at(unwind) or groups[pid].at(arrival) or before
            for token, book in zip(pair_tokens[pid], versions, strict=True):
                reserved[token] = (book.stamp, quote_key(book))
    return (pd.concat(attempts, ignore_index=True) if attempts else pd.DataFrame(), pd.DataFrame(audit))
