"""Choose funded basket quantities and fixed limit prices from decision-time depth."""
import numpy as np
from src.data.books import RawBook


def depth_quantities(books, minimum, maximum, cash_cap, min_limit_margin):
    """Integer endpoints of constant-marginal-price depth intervals.

    Profit is linear inside each interval, so an optimum is at an endpoint.
    Cash and worst-limit-margin constraints can introduce additional endpoints.
    Memory scales with book levels, not the number of available shares.
    """
    lower = max(minimum, int(np.ceil(max(b.min_order_size for b in books))))
    upper = int(np.floor(min(b.asks.cumulative_size[-1] for b in books)))
    if maximum is not None:
        upper = min(upper, maximum)
    if upper < lower:
        return np.array([], dtype=float)
    boundaries = np.unique(np.r_[lower - 1, upper,
        np.concatenate([np.floor(b.asks.cumulative_size) for b in books])])
    boundaries = boundaries[(boundaries >= lower - 1) & (boundaries <= upper)]
    starts, ends = boundaries[:-1] + 1, boundaries[1:]
    marginal = np.sum([b.asks._quotes(starts)[1] for b in books], axis=0)
    if cash_cap is not None:
        positive = marginal > 0
        ends[positive] = np.minimum(ends[positive], np.floor((cash_cap + 1e-10) / marginal[positive]))
    if min_limit_margin is not None:
        positive = 1 - marginal > 0
        starts[positive] = np.maximum(starts[positive],
            np.floor((min_limit_margin + 1e-10) / (1 - marginal[positive])) + 1)
        starts[~positive] = upper + 1
    valid = starts <= ends
    return np.unique(np.r_[starts[valid], ends[valid]])

def choose_order(books: list[RawBook], mode: str = "adaptive", min_gross: float = 0,
                 max_quantity: int | None = 1000, cash_cap: float | None = 1000,
                 min_quantity: int = 100, min_limit_margin: float | None = None) -> dict | None:
    """Pick the highest displayed gross among permitted integer quantities.

    Fixed mode considers only min_quantity. Full depth and known minimum size /
    tick metadata are required for every leg. Reserve quantity times the sum
    of marginal limit prices, not merely the cheaper observed depth cost.
    None removes the corresponding cap; available depth still bounds size.
    Ties choose the smaller quantity. No arrival-time books are consulted.
    """
    if mode not in {"fixed", "adaptive"}:
        raise ValueError("Mode must be fixed or adaptive")
    if not np.isfinite(min_gross) or min_gross < 0 or (cash_cap is not None and (not np.isfinite(cash_cap) or cash_cap <= 0)):
        raise ValueError("Gross threshold must be nonnegative and cash cap positive")
    if max_quantity is not None and (isinstance(max_quantity, bool) or not isinstance(max_quantity, (int, np.integer)) or max_quantity < 1):
        raise ValueError("Maximum quantity must be a positive integer")
    if isinstance(min_quantity, bool) or not isinstance(min_quantity, (int, np.integer)) or min_quantity < 1:
        raise ValueError("Minimum quantity must be a positive integer")
    if min_limit_margin is not None and (not np.isfinite(min_limit_margin) or min_limit_margin < 0):
        raise ValueError("Minimum worst-limit margin must be nonnegative")
    if len(books) < 2 or any(not book.clean for book in books):
        return None
    if any(not np.isfinite([book.min_order_size, book.tick_size]).all()
           or book.min_order_size <= 0 or not 0 < book.tick_size <= 1 for book in books):
        return None
    if mode == 'fixed':
        quantities = np.asarray([min_quantity] if max_quantity is None or min_quantity <= max_quantity else [], dtype=float)
    elif max_quantity is not None and max_quantity <= 10000:
        quantities = np.arange(min_quantity, max_quantity + 1, dtype=float)
    else:
        quantities = depth_quantities(books, min_quantity, max_quantity, cash_cap, min_limit_margin)
    if not len(quantities):
        return None
    costs, limits = zip(*(book.asks._quotes(quantities) for book in books))
    costs, limits = np.asarray(costs), np.asarray(limits)
    gross = quantities - costs.sum(axis=0)
    reserved = quantities * limits.sum(axis=0)
    valid = np.isfinite(costs).all(axis=0) & (gross > 0) & (gross >= min_gross - 1e-10)
    if cash_cap is not None:
        valid &= (reserved <= cash_cap + 1e-10)
    if min_limit_margin is not None:
        valid &= quantities - reserved > min_limit_margin + 1e-10
    for member, book in enumerate(books):
        valid &= quantities >= book.min_order_size
        ticks = limits[member] / book.tick_size
        valid &= np.isclose(ticks, np.rint(ticks), rtol=0, atol=1e-7)
    candidates = np.flatnonzero(valid)
    if not len(candidates):
        return None
    best = float(np.max(gross[candidates]))
    # Float roundoff on an exactly flat gross plateau must not prefer more risk.
    chosen = int(candidates[np.flatnonzero(np.isclose(gross[candidates], best, rtol=0, atol=1e-9))[0]])
    return {
        "quantity": int(quantities[chosen]),
        "ask_costs": costs[:, chosen].tolist(),
        "limits": limits[:, chosen].tolist(),
        "quoted_gross_usd": float(gross[chosen]),
        "reserved_cash_usd": float(reserved[chosen]),
    }
