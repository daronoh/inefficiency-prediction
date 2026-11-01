from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import pandas as pd
from FYP.data.books import RawBook

@dataclass(frozen=True)
class Experiment:
    wait_s: float = 0.0
    delay_s: float = 0.1
    sizing: str = 'adaptive'
    min_entry_gross_usd: float = 0.0
    freshness_s: float = float('inf')
    max_quantity: int | None = None
    cash_cap_usd: float | None = None
    unwind_delay_s: float = 1.0
    min_quantity: int = 5
    min_limit_margin_usd: float | None = None
    monitoring_minutes: int = 60

def _usable(book: RawBook, when: pd.Timestamp, freshness: float) -> bool:
    age = (when - book.stamp).total_seconds()
    return bool(book.clean and 0 <= age <= freshness)

def _exit_values(books: list[RawBook] | None, q: int, when: pd.Timestamp, freshness: float, n: int) -> np.ndarray:
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
