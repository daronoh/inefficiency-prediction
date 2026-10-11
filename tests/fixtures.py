"""Synthetic books shared by the research tests."""
import json
import pandas as pd
BASE = pd.Timestamp("2025-11-01T12:00:00Z")

def _time(seconds: float, base: pd.Timestamp = BASE) -> pd.Timestamp:
    return base + pd.Timedelta(seconds=seconds)


def _levels(levels: list[tuple[float, float]]) -> str:
    return json.dumps([{"price": str(price), "size": str(size)} for price, size in levels])


def _frame(rows: list[tuple], base: pd.Timestamp = BASE) -> pd.DataFrame:
    """Rows are time offset, ask levels, bid levels for one actual token."""
    return pd.DataFrame(
        [
            {
                "snapshot_time": _time(seconds, base),
                "indexed_at_ms": int(_time(seconds, base).timestamp() * 1000),
                "asks_json": _levels(asks),
                "bids_json": _levels(bids),
                "min_order_size": 5.0,
                "tick_size": 0.001,
            }
            for seconds, asks, bids in rows
        ]
    )
