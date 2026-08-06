"""Market-level YES/NO pair panel construction.

The two outcome tokens of a binary market are captured asynchronously, so the
intra-market inefficiency signal (sum of complementary quotes vs $1) can only be
computed after aligning both books in time. This module builds, per market:

- an *event-grid* panel: one row per book update on either side, each side's
  latest state as-of that time, with per-side staleness columns; and
- a *regular-grid* panel (default 1 minute): last-observation-carried-forward
  states on a fixed clock, again with staleness columns.

Staleness is never silently capped here: rows carry `y0_staleness_s` /
`y1_staleness_s`, and validity flags are provided at several caps so downstream
label code can run sensitivity analysis without rebuilding the panel.

NOTE: `valid_pair_*` means both sides are *fresh*, not that both books are
*tradeable* — a fresh snapshot can still be one-sided or crossed. Downstream
code must additionally filter on the per-side `flag_no_bids` / `flag_no_asks` /
`flag_crossed_book` columns.
"""

from datetime import timedelta
from pathlib import Path

import polars as pl

# Per-side book state carried into the panel.
SIDE_COLS = [
    "snapshot_time",
    "best_bid",
    "best_bid_size",
    "best_ask",
    "best_ask_size",
    "mid_price",
    "spread",
    "bid_depth_3",
    "ask_depth_3",
    "imbalance_3",
    "bid_depth_total",
    "ask_depth_total",
    "n_bid_levels",
    "n_ask_levels",
    "flag_no_bids",
    "flag_no_asks",
    "flag_crossed_book",
    "tick_size",
]

STALENESS_CAPS_S = (2, 5, 10, 30)


def _load_side(feat_path: Path, prefix: str) -> pl.DataFrame:
    """Load one token's feature table and rename to prefixed side columns."""
    df = (
        pl.read_parquet(feat_path)
        .select([c for c in SIDE_COLS if c != "snapshot_time"] + ["snapshot_time", "indexed_at_ms"])
        # Feature files store ms-precision datetimes; the regular grid is built
        # with datetime_range (us). Align units so as-of joins accept both.
        .with_columns(pl.col("snapshot_time").dt.cast_time_unit("us"))
        .sort("snapshot_time", "indexed_at_ms")
        # One state per timestamp: keep the latest-indexed row.
        .unique(subset=["snapshot_time"], keep="last")
        .drop("indexed_at_ms")
        .sort("snapshot_time")
    )
    return df.rename({c: f"{prefix}_{c}" for c in df.columns})


def _asof_align(side: pl.DataFrame, prefix: str, grid: pl.DataFrame) -> pl.DataFrame:
    """As-of (backward) join a side's states onto a time grid with staleness."""
    joined = grid.join_asof(
        side,
        left_on="time",
        right_on=f"{prefix}_snapshot_time",
        strategy="backward",
    )
    return joined.with_columns(
        ((pl.col("time") - pl.col(f"{prefix}_snapshot_time")).dt.total_milliseconds() / 1000.0)
        .alias(f"{prefix}_staleness_s")
    )


def _add_pair_columns(panel: pl.DataFrame) -> pl.DataFrame:
    """Derived pair-level quote-consistency columns and validity flags."""
    panel = panel.with_columns([
        (pl.col("y0_best_ask") + pl.col("y1_best_ask")).alias("sum_best_asks"),
        (pl.col("y0_best_bid") + pl.col("y1_best_bid")).alias("sum_best_bids"),
    ]).with_columns([
        # Positive => the naive buy-both-then-merge arb exists BEFORE costs.
        (1.0 - pl.col("sum_best_asks")).alias("long_arb_gap"),
        # Positive => the naive split-then-sell-both arb exists BEFORE costs.
        (pl.col("sum_best_bids") - 1.0).alias("short_arb_gap"),
    ])
    for cap in STALENESS_CAPS_S:
        panel = panel.with_columns(
            (
                (pl.col("y0_staleness_s") <= cap) & (pl.col("y1_staleness_s") <= cap)
            ).fill_null(False).alias(f"valid_pair_{cap}s")
        )
    return panel


def build_pair_panels(
    feat_path_y0: Path,
    feat_path_y1: Path,
    market_id: str,
    grid_interval: str = "1m",
):
    """Build (event_panel, grid_panel) for one binary market.

    y0/y1 are the outcome_index 0/1 tokens. Returns two polars DataFrames.
    """
    y0 = _load_side(Path(feat_path_y0), "y0")
    y1 = _load_side(Path(feat_path_y1), "y1")

    if y0.is_empty() or y1.is_empty():
        raise ValueError(f"market {market_id}: one side has no snapshots")

    # Event grid: union of both sides' update times.
    event_times = (
        pl.concat([
            y0.select(pl.col("y0_snapshot_time").alias("time")),
            y1.select(pl.col("y1_snapshot_time").alias("time")),
        ])
        .unique()
        .sort("time")
    )
    event_panel = _asof_align(y0, "y0", event_times)
    event_panel = _asof_align(y1, "y1", event_panel)
    event_panel = _add_pair_columns(event_panel).with_columns(
        pl.lit(market_id).alias("market_id")
    )

    # Regular grid over the overlap of both sides' coverage. Ceil the start to
    # the next whole minute so grid timestamps are wall-clock aligned and
    # therefore joinable across markets (ceiling, not flooring, keeps every grid
    # time inside both sides' coverage so backward as-of matches always exist).
    start = max(y0["y0_snapshot_time"].min(), y1["y1_snapshot_time"].min())
    if start.second or start.microsecond:
        start = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
    end = min(y0["y0_snapshot_time"].max(), y1["y1_snapshot_time"].max())
    if start >= end:
        grid_panel = None
    else:
        grid_times = pl.DataFrame({
            "time": pl.datetime_range(
                start=start,
                end=end,
                interval=grid_interval,
                time_zone="UTC",
                eager=True,
            )
        })
        grid_panel = _asof_align(y0, "y0", grid_times)
        grid_panel = _asof_align(y1, "y1", grid_panel)
        grid_panel = _add_pair_columns(grid_panel).with_columns(
            pl.lit(market_id).alias("market_id")
        )

    return event_panel, grid_panel
