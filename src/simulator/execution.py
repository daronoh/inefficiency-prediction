"""Causal displayed-depth order replay, including partial-basket losses.

This is an execution plausibility check, not a claim of exchange-confirmed fills.
"""
from __future__ import annotations
from dataclasses import dataclass
import json
import numpy as np
import pandas as pd
from src.data.books import RawBook, RawGroup
from src.simulator.sizing import choose_order
from src.config import SPEC
def failure_stress(asks, bids, q=100) -> dict:
    """One failed leg, others bought then unwound. Missing depth is not free."""
    asks, bids = np.asarray(asks, float), np.asarray(bids, float)
    if asks.shape != bids.shape or asks.ndim != 1 or len(asks) < 2:
        raise ValueError("Expected equal-length arrays with at least two members")
    pnls = []
    for failed in range(len(asks)):
        filled = np.arange(len(asks)) != failed
        if np.isfinite(asks[filled]).all() and np.isfinite(bids[filled]).all():
            pnls.append(float(q * (bids[filled] - asks[filled]).sum()))
    complete = len(pnls) == len(asks)
    return {
        "failure_cases_priceable": len(pnls),
        "all_failure_cases_priceable": complete,
        "failure_mean_pnl_usd": float(np.mean(pnls)) if complete else np.nan,
        "failure_worst_pnl_usd": min(pnls) if complete else np.nan,
    }

@dataclass(frozen=True)
class Experiment:
    wait_s: float = 0.0
    delay_s: float = 0.5
    sizing: str = "fixed"
    min_entry_gross_usd: float = 0.0
    freshness_s: float = 30.0
    max_quantity: int | None = 1000
    cash_cap_usd: float | None = 1000.0
    unwind_delay_s: float = 1.0
    min_quantity: int = 100
    min_limit_margin_usd: float | None = None
    monitoring_minutes: int = 60
    compute_failure_stress: bool = False


def _usable(book: RawBook, when: pd.Timestamp, freshness: float) -> bool:
    age = (when - book.stamp).total_seconds()
    return bool(book.clean and 0 <= age <= freshness)


def _exit_values(books: list[RawBook] | None, q: int, when: pd.Timestamp,
                 freshness: float, n: int) -> np.ndarray:
    """Price each possible unwind independently; unavailable exits remain NaN."""
    values = np.full(n, np.nan)
    if books is None:
        return values
    for i, book in enumerate(books):
        if not _usable(book, when, freshness) or not np.isfinite(book.min_order_size) or q < book.min_order_size:
            continue
        quote = book.bids.quote(q)
        if quote is not None:
            values[i] = quote[0]
    return values


def run_scenario(signals: pd.DataFrame, group: RawGroup,
                 experiment: Experiment) -> pd.DataFrame:
    """Process one event chronologically, preserving reservations across folds.

    An individual FOK is *assumed* to fill if fresh displayed depth within its
    frozen price limit suffices at arrival. This is plausibility, not a fill
    probability estimate. Unknown book state cannot be interpreted as no fill.
    """
    if experiment.wait_s < 0 or experiment.delay_s < 0 or experiment.unwind_delay_s < 0:
        raise ValueError("Times must be nonnegative")
    if isinstance(experiment.monitoring_minutes, bool) or not isinstance(experiment.monitoring_minutes, int) or experiment.monitoring_minutes <= 0:
        raise ValueError('Monitoring duration must be positive integer minutes')
    if signals.event_slug.nunique() > 1:
        raise ValueError("Each scenario call must contain exactly one event")
    rows, reserved = [], None
    busy_until = None
    cutoff = pd.Timestamp(SPEC.lockbox_start)
    for signal in signals.sort_values("start_time").itertuples(index=False):
        confirm = signal.start_time + pd.Timedelta(seconds=experiment.wait_s)
        arrival = confirm + pd.Timedelta(seconds=experiment.delay_s)
        unwind = arrival + pd.Timedelta(seconds=experiment.unwind_delay_s)
        row = {
            "event_slug": signal.event_slug, "window": signal.window,
            "decision_time": signal.time, "start_time": signal.start_time,
            "confirmation_time": confirm, "arrival_time": arrival, "unwind_time": unwind,
            "status": "not_confirmed", "attempted": False, "quantity": 0,
            "n_members": 0, "n_filled_legs": 0,
            "quoted_gross_usd": np.nan, "reserved_cash_usd": 0.0,
            "limits_json": "[]", "entry_cost_usd": np.nan, "pnl_usd": np.nan,
            "confirmation_max_age_s": np.nan, "arrival_max_age_s": np.nan,
            "filled_legs_json": "[]", "leg_costs_json": "[]",
            "forced_failure_cases_priceable": 0,
            "forced_failure_worst_pnl_usd": np.nan,
        }
        rows.append(row)
        # Five-second confirmation checks only whether a flip has occurred by
        # confirmation. Immediate entry NEVER filters on eventual duration/end.
        if experiment.wait_s > 0 and confirm >= signal.end_time:
            continue
        monitoring_end = getattr(signal, "monitoring_end_time", None)
        outside = (arrival >= monitoring_end if monitoring_end is not None
                   else arrival >= signal.time + pd.Timedelta(minutes=experiment.monitoring_minutes))
        if outside or arrival >= cutoff:
            row["status"] = "outside_monitoring_window"
            continue
        # A sequential loop may inspect the preceding attempt's arrival only
        # after that time has passed. No overlapping per-event attempts.
        if busy_until is not None and confirm < busy_until:
            row["status"] = "in_flight"
            continue
        before = group.at(confirm)
        if before is None or not all(_usable(b, confirm, experiment.freshness_s) for b in before):
            row["status"] = "confirmation_unusable"
            continue
        stamps = tuple(b.stamp for b in before)
        row["n_members"] = len(before)
        row["confirmation_max_age_s"] = max((confirm - stamp).total_seconds() for stamp in stamps)
        if reserved is not None and not all(now > old for now, old in zip(stamps, reserved, strict=True)):
            row["status"] = "quote_reuse_blocked"
            continue
        order = choose_order(before, mode=experiment.sizing,
            min_gross=experiment.min_entry_gross_usd, max_quantity=experiment.max_quantity,
            cash_cap=experiment.cash_cap_usd, min_quantity=experiment.min_quantity,
            min_limit_margin=experiment.min_limit_margin_usd)
        if order is None:
            row["status"] = "no_acceptable_size"
            continue
        q, limits = order["quantity"], order["limits"]
        row.update({"attempted": True, "quantity": q,
                    "quoted_gross_usd": order["quoted_gross_usd"],
                    "reserved_cash_usd": order["reserved_cash_usd"],
                    "limits_json": json.dumps(limits)})
        reserved, busy_until = stamps, unwind
        after = group.at(arrival)
        # Reserve arrived versions even if the state is stale/otherwise unknown;
        # this cannot affect a decision before the fixed in-flight period ends.
        if after is not None:
            reserved = tuple(b.stamp for b in after)
            row["arrival_max_age_s"] = max((arrival - b.stamp).total_seconds() for b in after)
        if after is None or not all(_usable(b, arrival, experiment.freshness_s) for b in after):
            row["status"] = "unknown_arrival"
            continue
        if not all(np.isfinite(b.min_order_size) and np.isfinite(b.tick_size) and b.tick_size > 0 for b in after):
            row["status"] = "unknown_arrival"
            continue
        # Do not inspect spell.end_time or require a positive arrival gap here:
        # these orders were already sent and can generate losses.
        fills = []
        costs = np.zeros(len(after))
        for i, (book, limit) in enumerate(zip(after, limits, strict=True)):
            tick_valid = abs(limit / book.tick_size - round(limit / book.tick_size)) < 1e-7
            quote = book.asks.quote(q, limit=limit) if q >= book.min_order_size and tick_valid else None
            if quote is not None:
                fills.append(i)
                costs[i] = quote[0]
        row.update({"n_filled_legs": len(fills), "entry_cost_usd": float(costs.sum()),
                    "filled_legs_json": json.dumps(fills), "leg_costs_json": json.dumps(costs.tolist())})
        if not fills:
            row.update({"status": "no_fill", "pnl_usd": 0.0})
            continue
        exit_books = group.at(unwind) if unwind < cutoff else None
        exits = _exit_values(exit_books, q, unwind, experiment.freshness_s, len(after))
        if len(fills) == len(after):
            row.update({"status": "complete", "pnl_usd": float(q - costs.sum())})
            if experiment.compute_failure_stress:
                stress = failure_stress(costs / q, exits / q, q)
                row["forced_failure_cases_priceable"] = stress["failure_cases_priceable"]
                row["forced_failure_worst_pnl_usd"] = stress["failure_worst_pnl_usd"]
        elif np.isfinite(exits[fills]).all():
            row.update({"status": "partial_unwound", "pnl_usd": float((exits[fills] - costs[fills]).sum())})
        else:
            row["status"] = "partial_unknown_unwind"
    return pd.DataFrame(rows)



def experiment(spec):
    return Experiment(wait_s=0., delay_s=spec.arrival_delay_s,
        sizing='adaptive', freshness_s=float('inf'),
        min_quantity=min(spec.quantities), max_quantity=max(spec.quantities),
        cash_cap_usd=spec.cash_cap_usd, unwind_delay_s=spec.unwind_delay_s,
        min_entry_gross_usd=spec.assumed_other_cost_usd,
        min_limit_margin_usd=spec.assumed_other_cost_usd,
        monitoring_minutes=spec.horizon_minutes)
