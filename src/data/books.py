"""Small, backward-only raw-book helpers for the development sizing experiment.

Quotes describe displayed liquidity, not realised fills. Quantity and limit
prices are selected from the decision-time books; callers must keep them fixed
when checking later arrival books. This module never opens lockbox rows.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from src.config import SPEC, require_split_access
from src.paths import ORDERBOOK_DIR


class Ladder:
    """Sorted cumulative depth: ascending asks or descending bids."""

    def __init__(self, levels: list[dict], ascending: bool):
        self.ascending = bool(ascending)
        rows = []
        for level in levels:
            try:
                price, size = float(level["price"]), float(level["size"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Each level needs numeric price and size") from exc
            if not np.isfinite([price, size]).all() or not 0 <= price <= 1 or size < 0:
                raise ValueError("Book prices must be in [0, 1] and sizes nonnegative and finite")
            if size > 0:
                rows.append((price, size))
        rows.sort(key=lambda row: row[0], reverse=not self.ascending)
        self.prices = np.asarray([row[0] for row in rows], dtype=float)
        self.sizes = np.asarray([row[1] for row in rows], dtype=float)
        self.cumulative_size = np.cumsum(self.sizes)
        self.cumulative_value = np.cumsum(self.prices * self.sizes)
        if not np.isfinite(self.cumulative_size).all() or not np.isfinite(self.cumulative_value).all():
            raise ValueError("Cumulative book depth overflows")

    def __len__(self) -> int:
        return len(self.prices)

    def _quotes(self, quantities: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Vectorised unrestricted costs and marginal prices; NaN if too shallow."""
        costs = np.full(quantities.shape, np.nan)
        worst = np.full(quantities.shape, np.nan)
        if not len(self):
            return costs, worst
        indices = np.searchsorted(self.cumulative_size, quantities, side="left")
        available = indices < len(self)
        i = indices[available]
        before_size = np.r_[0.0, self.cumulative_size][i]
        before_value = np.r_[0.0, self.cumulative_value][i]
        worst[available] = self.prices[i]
        costs[available] = before_value + (quantities[available] - before_size) * self.prices[i]
        return costs, worst

    def quote(self, q: float, limit: float | None = None) -> tuple[float, float] | None:
        """Return full-size value and worst price, or None; never partial fills.

        An ask limit is a maximum buy price; a bid limit a minimum sell price.
        """
        if not np.isfinite(q) or q <= 0:
            raise ValueError("Quantity must be finite and positive")
        if limit is not None and (not np.isfinite(limit) or not 0 <= limit <= 1):
            raise ValueError("Limit must be a finite price in [0, 1]")
        costs, worst = self._quotes(np.asarray([q], dtype=float))
        if not np.isfinite(costs[0]):
            return None
        if limit is not None:
            if self.ascending and worst[0] > limit + 1e-12:
                return None
            if not self.ascending and worst[0] < limit - 1e-12:
                return None
        return float(costs[0]), float(worst[0])


@dataclass(frozen=True)
class RawBook:
    stamp: pd.Timestamp
    asks: Ladder
    bids: Ladder
    min_order_size: float
    tick_size: float

    @property
    def clean(self) -> bool:
        """Two-sided and not crossed; a locked book is allowed, as in labels."""
        return bool(len(self.asks) and len(self.bids) and self.bids.prices[0] <= self.asks.prices[0])


class RawGroup:
    """As-of member books, with at most 4,096 parsed snapshots cached.

    Equal snapshot timestamps keep the latest provider index, matching the
    existing primary snapshot-time reconstruction. This is not a provider-time
    availability test. Member quotes can carry forward only within the group's
    observed union extent; callers impose their own age ceiling.
    """

    COLUMNS = ["snapshot_time", "indexed_at_ms", "asks_json", "bids_json", "min_order_size", "tick_size"]
    CACHE_LIMIT = 4096

    def __init__(self, frames: list[pd.DataFrame]):
        if not frames:
            raise ValueError("A group needs member frames")
        self.frames = []
        self.times = []
        self.paths: list[Path] = []
        self._cache: OrderedDict[tuple[int, int], RawBook] = OrderedDict()
        for original in frames:
            frame = original.loc[:, self.COLUMNS].copy()
            frame["snapshot_time"] = pd.to_datetime(frame.snapshot_time, utc=True)
            if frame.snapshot_time.isna().any():
                raise ValueError("Snapshot times cannot be missing")
            frame = frame.loc[
                (frame.snapshot_time >= pd.Timestamp(SPEC.development_start))
                & (frame.snapshot_time < pd.Timestamp(SPEC.lockbox_start))
            ]
            frame["indexed_at_ms"] = pd.to_numeric(frame.indexed_at_ms, errors="raise")
            frame = frame.sort_values(
                ["snapshot_time", "indexed_at_ms"], kind="stable", na_position="first"
            ).drop_duplicates("snapshot_time", keep="last").reset_index(drop=True)
            self.frames.append(frame)
            self.times.append(pd.DatetimeIndex(frame.snapshot_time))
        self.last_time = max((times[-1] for times in self.times if len(times)), default=None)

    @classmethod
    def from_members(cls, group: pd.DataFrame) -> RawGroup:
        """Load only the named members' development rows, in supplied order."""
        require_split_access("development")
        tokens = group["yes_token_id"].astype(str).tolist()
        if not tokens or len(set(tokens)) != len(tokens) or any(not re.fullmatch(r"\d+", token) for token in tokens):
            raise ValueError("Expected unique numeric YES token IDs")
        paths = [ORDERBOOK_DIR / f"ob_{token}.parquet" for token in tokens]
        filters = [
            ("snapshot_time", ">=", pd.Timestamp(SPEC.development_start)),
            ("snapshot_time", "<", pd.Timestamp(SPEC.lockbox_start)),
        ]
        frames = [pd.read_parquet(path, columns=cls.COLUMNS, filters=filters) for path in paths]
        result = cls(frames)
        result.paths = paths
        return result

    @classmethod
    def load(cls, group: pd.DataFrame) -> RawGroup:
        return cls.from_members(group)

    @staticmethod
    def _levels(value) -> list[dict]:
        if value is None or (np.isscalar(value) and not isinstance(value, str) and pd.isna(value)):
            return []
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, list):
            raise ValueError("Book side must contain a JSON list of levels")
        return value

    def _book(self, member: int, row_number: int) -> RawBook:
        key = (member, row_number)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        row = self.frames[member].iloc[row_number]
        book = RawBook(
            stamp=row.snapshot_time,
            asks=Ladder(self._levels(row.asks_json), ascending=True),
            bids=Ladder(self._levels(row.bids_json), ascending=False),
            min_order_size=float(row.min_order_size) if pd.notna(row.min_order_size) else np.nan,
            tick_size=float(row.tick_size) if pd.notna(row.tick_size) else np.nan,
        )
        self._cache[key] = book
        if len(self._cache) > self.CACHE_LIMIT:
            self._cache.popitem(last=False)
        return book

    def at(self, when: pd.Timestamp) -> list[RawBook] | None:
        when = pd.Timestamp(when)
        if pd.isna(when) or when.tzinfo is None:
            raise ValueError("Query time must be timezone-aware and nonmissing")
        if self.last_time is None or when > self.last_time:
            return None
        rows = []
        for times in self.times:
            if not len(times) or when < times[0]:
                return None
            rows.append(int(times.searchsorted(when, side="right")) - 1)
        return [self._book(member, row) for member, row in enumerate(rows)]

class ObservationGroup(RawGroup):
    """Backward-only books, with each leg bounded by its own recording extent.

Keep pre-activation rows for as-of seeds and post-deadline rows for unwinds.
The scheduler enforces the entry deadline, independently of book availability.
"""
    def at(self, when):
        when = pd.Timestamp(when)
        if any(not len(times) or when > times[-1] for times in self.times):
            return None
        return super().at(when)
