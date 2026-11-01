from __future__ import annotations
def quote_key(book) -> tuple:
    """Identical ask quantities/prices are not new liquidity just due to timestamp."""
    return tuple(zip(book.asks.prices.tolist(), book.asks.sizes.tolist()))
