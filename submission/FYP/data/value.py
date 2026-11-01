from __future__ import annotations
import pandas as pd
from FYP.data.books import RawGroup, ObservationGroup
from FYP.simulator.sizing import choose_order

def peak_quotes(plan, frames, signals):
    """Maximum one-basket margin per hour, never a sum of repeated snapshots.

    Re-evaluate every member update inside each previously qualified spell.
    Different ladder pairs may share liquidity: their maxima are not added.
    """
    columns = ['time', 'pair_id', 'peak_quoted_usd', 'peak_time', 'quantity', 'states_checked']
    if signals.empty:
        return pd.DataFrame(columns=columns)
    frames = {token: RawGroup([frame]).frames[0] for token, frame in frames.items()}
    rows = []
    for pair, part in signals.groupby('pair_id'):
        number = int(pair.rsplit(':', 1)[1])
        ids = plan['packages'][number][0]
        group = ObservationGroup([frames[token] for token in ids])
        updates = pd.DatetimeIndex([])
        for token in ids:
            updates = updates.union(pd.DatetimeIndex(frames[token].snapshot_time))
        for when, hour in part.groupby('time'):
            best, peak, quantity, checked = (0.0, pd.NaT, 0, 0)
            for spell in hour.itertuples():
                grid = pd.DatetimeIndex([spell.start_time]).union(updates[(updates > spell.start_time) & (updates < spell.end_time)]).sort_values()
                for time in grid:
                    books = group.at(time)
                    order = None if books is None else choose_order(books, min_quantity=5, max_quantity=None, cash_cap=None, min_limit_margin=0.0)
                    checked += 1
                    if order is None:
                        raise ValueError('Qualified spell contains an unqualified quote')
                    if order['quoted_gross_usd'] > best:
                        best, peak, quantity = (order['quoted_gross_usd'], time, order['quantity'])
            rows.append(dict(time=when, pair_id=pair, peak_quoted_usd=best, peak_time=peak, quantity=quantity, states_checked=checked))
    return pd.DataFrame(rows, columns=columns)
