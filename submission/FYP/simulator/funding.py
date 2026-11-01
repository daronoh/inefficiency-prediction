from __future__ import annotations
import numpy as np
import pandas as pd
from FYP.simulator.execution import Experiment

def setup(delay, minutes=60):
    if not np.isfinite(delay) or delay < 0:
        raise ValueError('Finite nonnegative latency required')
    return Experiment(delay_s=delay, wait_s=0.0, sizing='adaptive', freshness_s=float('inf'), max_quantity=None, cash_cap_usd=None, unwind_delay_s=1.0, min_quantity=5, min_entry_gross_usd=0.0, min_limit_margin_usd=0.0, monitoring_minutes=minutes)

def funding_metrics(part):
    """Cash scenarios, not realised portfolio capital or turnover-based ROI.

    Pending limit reservations are released at arrival; known purchases consume
    cash. Completed baskets receive no settlement cash within the experiment.
    Known partial unwinds return their proceeds; unknown orders retain full
    reservations and unknown exits return nothing. Simultaneous submissions are
    charged before cash releases, conservatively avoiding intratimestamp netting.
    """
    if part.empty:
        return dict(peak_inflight_reservations_usd=0.0, no_settlement_funding_usd=0.0)
    events = []
    for row in part.loc[part.attempted].itertuples():
        reserve = float(row.reserved_cash_usd)
        events.append((row.confirmation_time, 0, reserve, reserve))
        events.append((row.unwind_time, 1, -reserve, 0.0))
        if pd.notna(row.entry_cost_usd):
            events.append((row.arrival_time, 1, 0.0, float(row.entry_cost_usd) - reserve))
            if row.status == 'partial_unwound' and pd.notna(row.pnl_usd):
                events.append((row.unwind_time, 1, 0.0, -float(row.entry_cost_usd + row.pnl_usd)))
    ledger = pd.DataFrame(events, columns=['time', 'phase', 'inflight', 'funding']).groupby(['time', 'phase'])[['inflight', 'funding']].sum().sort_index().cumsum()
    return dict(peak_inflight_reservations_usd=max(0.0, float(ledger.inflight.max())), no_settlement_funding_usd=max(0.0, float(ledger.funding.max())))
