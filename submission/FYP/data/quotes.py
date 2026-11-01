from __future__ import annotations
import numpy as np
from FYP.simulator.sizing import depth_quantities

def quoted_net(books, other_cost=0.0):
    """Best integer-depth dollar margin, including negative margins for ranking.

    This is a snapshot feature, not an execution filter. Like the original
    quoted-gap feature it does not require a positive worst-limit margin.
    """
    if len(books) < 2 or any((b is None or not b.clean for b in books)):
        return np.nan
    if any((not np.isfinite([b.min_order_size, b.tick_size]).all() or b.min_order_size <= 0 or (not 0 < b.tick_size <= 1) for b in books)):
        return np.nan
    q = depth_quantities(books, 5, None, None, None)
    if not len(q):
        return np.nan
    costs, limits = zip(*(b.asks._quotes(q) for b in books))
    valid = np.isfinite(costs).all(axis=0)
    for b, limit in zip(books, limits, strict=True):
        valid &= np.isclose(limit / b.tick_size, np.rint(limit / b.tick_size), rtol=0, atol=1e-07)
    return float(np.max((q - np.sum(costs, axis=0) - other_cost)[valid])) if valid.any() else np.nan
