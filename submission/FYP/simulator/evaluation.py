from __future__ import annotations
import numpy as np
from FYP.simulator.discovery import capacity_summary

def economic_summary(part, model, spec):
    row = capacity_summary(part, model, spec)
    taken = part.loc[part.attempted].copy() if len(part) else part
    if len(taken):
        taken['accounting_time'] = taken.arrival_time.where(taken.status.eq('complete'), taken.unwind_time)
        increments = taken.loc[taken.pnl_usd.notna()].groupby('accounting_time').pnl_usd.agg(['sum', 'size'])
        curve = (increments['sum'] - increments['size'] * spec.assumed_other_cost_usd).cumsum().to_numpy()
        with_zero = np.r_[0.0, curve]
        row['max_drawdown_usd'] = float(np.max(np.maximum.accumulate(with_zero) - with_zero))
        row['worst_attempt_usd'] = float(taken.pnl_usd.min())
    else:
        row['max_drawdown_usd'] = row['worst_attempt_usd'] = 0.0
    return row
