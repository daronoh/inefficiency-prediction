from __future__ import annotations
def summarize(details, model, scenario, delay, unwind, other_cost=0.0):
    taken = details.loc[details.attempted] if len(details) else details
    complete = taken.loc[taken.status.eq('complete')] if len(taken) else taken
    partial = taken.loc[taken.status.str.startswith('partial')] if len(taken) else taken
    return dict(model=model, scenario=scenario, arrival_delay_s=delay, unwind_delay_s=unwind, attempts=len(taken), completed=len(complete), partial_attempts=len(partial), unknown_attempts=int(taken.pnl_usd.isna().sum()) if len(taken) else 0, completed_profit_usd=float(complete.pnl_usd.sum()) if len(complete) else 0.0, partial_pnl_usd=float(partial.pnl_usd.sum()) if len(partial) else 0.0, known_pnl_usd=float(taken.pnl_usd.sum()) - len(taken) * other_cost if len(taken) else 0.0)
