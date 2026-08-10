"""negRisk group panel construction (N sibling markets on one timeline).

Generalizes the pair panel: a winner-take-all event's N member markets are
as-of aligned via their YES books (outcome-0 tokens; the NO book is the YES
book mirrored, so one side per market suffices). Two grids per event, as in
pairs.py: an event grid (union of all members' update times) and a wall-clock
grid (default 1 minute, start ceiled to a whole minute so timestamps join
across events).

Group-level quote-consistency signals (winner-take-all: exactly one member
resolves YES, so buying YES on ALL members pays exactly $1, and a NO basket on
any subset S pays at least |S|-1):

  long_gap  = 1 - sum(best_ask_i over ALL members)   > 0 => full-cover arb
  short_gap = sum(best_bid_i over quoted members) - 1 > 0 => subset NO arb

`long_gap` requires every member to have a live ask (it is null otherwise, and
permanently null for groups missing member data); `short_gap` is computable on
any subset, so members with missing books simply do not contribute.

Member columns are prefixed `m<i>_`; the (event, member index, market, token)
mapping is written to a companion members table by the runner. Staleness and
validity semantics follow pairs.py: per-member `m<i>_staleness_s`, group flags
`valid_group_{2,5,10,30}s` (ALL quoted members fresh). Freshness does not
imply tradeability — combine with the per-member book-quality flags.
"""

from datetime import timedelta
from pathlib import Path

import polars as pl

VWAP_SIZES = (10, 100, 1000)

# Per-member state carried into the panel (from the member's YES feat file).
MEMBER_COLS = [
    "snapshot_time",
    "best_bid",
    "best_bid_size",
    "best_ask",
    "best_ask_size",
    "spread",
    "bid_depth_3",
    "ask_depth_3",
    "bid_depth_total",
    "ask_depth_total",
    "flag_no_bids",
    "flag_no_asks",
    "flag_crossed_book",
] + [f"vwap_{side}_{q}" for side in ("ask", "bid") for q in VWAP_SIZES]

STALENESS_CAPS_S = (2, 5, 10, 30)


def _load_member(feat_path: Path, prefix: str) -> pl.DataFrame:
    df = (
        pl.read_parquet(feat_path)
        .select([c for c in MEMBER_COLS if c != "snapshot_time"]
                + ["snapshot_time", "indexed_at_ms"])
        .with_columns(pl.col("snapshot_time").dt.cast_time_unit("us"))
        .sort("snapshot_time", "indexed_at_ms")
        .unique(subset=["snapshot_time"], keep="last")
        .drop("indexed_at_ms")
        .sort("snapshot_time")
    )
    return df.rename({c: f"{prefix}_{c}" for c in df.columns})


def _asof_align(member: pl.DataFrame, prefix: str, grid: pl.DataFrame) -> pl.DataFrame:
    joined = grid.join_asof(
        member,
        left_on="time",
        right_on=f"{prefix}_snapshot_time",
        strategy="backward",
    )
    return joined.with_columns(
        ((pl.col("time") - pl.col(f"{prefix}_snapshot_time")).dt.total_milliseconds() / 1000.0)
        .alias(f"{prefix}_staleness_s")
    )


def _add_group_columns(panel: pl.DataFrame, prefixes: list, n_members_total: int,
                       full_cover_ok: bool = True) -> pl.DataFrame:
    """Group aggregate signals over the aligned member columns.

    `n_members_total` counts ALL members of the event (including any whose
    orderbook data is missing entirely) so that `long_gap` can never be
    computed from a partial group.
    """
    asks = [pl.col(f"{p}_best_ask") for p in prefixes]
    bids = [pl.col(f"{p}_best_bid") for p in prefixes]
    stal = [pl.col(f"{p}_staleness_s") for p in prefixes]

    n_asks = pl.sum_horizontal([a.is_not_null().cast(pl.Int32) for a in asks])
    n_bids = pl.sum_horizontal([b.is_not_null().cast(pl.Int32) for b in bids])

    # A member's book is "clean" when it is two-sided and not crossed. Phantom
    # quotes from resolved/dying members are one-sided, so requiring every
    # present member to be clean removes resolution-transition artifacts
    # (verified empirically: it eliminates all last-24h long-gap hits).
    clean = [
        (
            ~pl.col(f"{p}_flag_no_bids")
            & ~pl.col(f"{p}_flag_no_asks")
            & ~pl.col(f"{p}_flag_crossed_book")
        ).cast(pl.Int32)
        for p in prefixes
    ]
    n_present = pl.sum_horizontal(
        [pl.col(f"{p}_snapshot_time").is_not_null().cast(pl.Int32) for p in prefixes]
    )

    panel = panel.with_columns([
        n_asks.alias("n_asks_quoted"),
        n_bids.alias("n_bids_quoted"),
        (pl.sum_horizontal(clean) == n_present).alias("all_books_clean"),
        pl.sum_horizontal([a.fill_null(0.0) for a in asks]).alias("sum_asks_quoted"),
        pl.sum_horizontal([b.fill_null(0.0) for b in bids]).alias("sum_bids_quoted"),
        pl.max_horizontal(stal).alias("max_staleness_s"),
    ]).with_columns([
        # Full-cover long side: needs an ask on EVERY member of the event AND
        # an event known to be fully captured (see full_cover_ok).
        pl.when(full_cover_ok & (pl.col("n_asks_quoted") == n_members_total))
        .then(1.0 - pl.col("sum_asks_quoted"))
        .otherwise(None)
        .alias("long_gap"),
        # Subset short side: valid over whichever members are quoted.
        pl.when(pl.col("n_bids_quoted") > 0)
        .then(pl.col("sum_bids_quoted") - 1.0)
        .otherwise(None)
        .alias("short_gap"),
    ])
    # Executable (VWAP-at-size) group gaps + the fee basis Σ p(1−p) needed to
    # price a counterfactual taker fee (fee = feeRate × fee_basis per share).
    for q in VWAP_SIZES:
        va = [pl.col(f"{p}_vwap_ask_{q}") for p in prefixes]
        vb = [pl.col(f"{p}_vwap_bid_{q}") for p in prefixes]
        n_va = pl.sum_horizontal([a.is_not_null().cast(pl.Int32) for a in va])
        n_vb = pl.sum_horizontal([b.is_not_null().cast(pl.Int32) for b in vb])
        panel = panel.with_columns([
            # Full cover at size q: every member must be fillable for q shares.
            pl.when(full_cover_ok & (n_va == n_members_total))
            .then(1.0 - pl.sum_horizontal([a.fill_null(0.0) for a in va]))
            .otherwise(None)
            .alias(f"exec_long_gap_{q}"),
            pl.when(full_cover_ok & (n_va == n_members_total))
            .then(pl.sum_horizontal([(a * (1 - a)).fill_null(0.0) for a in va]))
            .otherwise(None)
            .alias(f"fee_basis_long_{q}"),
            # Subset short side at size q over members fillable for q shares.
            pl.when(n_vb > 0)
            .then(pl.sum_horizontal([b.fill_null(0.0) for b in vb]) - 1.0)
            .otherwise(None)
            .alias(f"exec_short_gap_{q}"),
            pl.when(n_vb > 0)
            .then(pl.sum_horizontal([(b * (1 - b)).fill_null(0.0) for b in vb]))
            .otherwise(None)
            .alias(f"fee_basis_short_{q}"),
            # Short-side book-quality gate: every member CONTRIBUTING to the
            # size-q short basket (non-null vwap_bid_q) must be two-sided and
            # non-crossed. One-sided resolution-phantom books contribute the
            # bulk of ungated short "opportunities" — this removes them.
            (pl.sum_horizontal([
                (vb[j].is_not_null() & ~(
                    ~pl.col(f"{p}_flag_no_bids")
                    & ~pl.col(f"{p}_flag_no_asks")
                    & ~pl.col(f"{p}_flag_crossed_book")
                )).cast(pl.Int32)
                for j, p in enumerate(prefixes)
            ]) == 0).alias(f"short_clean_{q}"),
        ])
    for cap in STALENESS_CAPS_S:
        panel = panel.with_columns(
            (pl.col("max_staleness_s") <= cap).fill_null(False).alias(f"valid_group_{cap}s")
        )
    return panel


def build_group_panels(
    feat_paths: dict,
    n_members_total: int,
    event_slug: str,
    grid_interval: str = "1m",
    full_cover_ok: bool = True,
):
    """Build (event_panel, grid_panel) for one winner-take-all event.

    feat_paths: {member_index: Path} for members WITH data; member_index is the
    stable index recorded in the members table (0..n_members_total-1, ordered
    by market_id). Members without data contribute no columns; long_gap stays
    null via the n_members_total gate.

    full_cover_ok: pass False when the event is NOT fully captured (winner
    missing from the dump, or any member lacks orderbook data). long_gap is
    then forced null in the panel itself — a partial group's ask-sum is not an
    arbitrage bound, and gating here means no downstream consumer can forget.
    """
    members = {}
    for idx in sorted(feat_paths):
        prefix = f"m{idx}"
        df = _load_member(Path(feat_paths[idx]), prefix)
        if not df.is_empty():
            members[prefix] = df
    if not members:
        raise ValueError(f"event {event_slug}: no member has any snapshots")

    prefixes = list(members)

    event_times = (
        pl.concat([
            df.select(pl.col(f"{p}_snapshot_time").alias("time"))
            for p, df in members.items()
        ])
        .unique()
        .sort("time")
    )
    event_panel = event_times
    for p, df in members.items():
        event_panel = _asof_align(df, p, event_panel)
    event_panel = _add_group_columns(event_panel, prefixes, n_members_total, full_cover_ok)
    event_panel = event_panel.with_columns(pl.lit(event_slug).alias("event_slug"))

    starts = [df[f"{p}_snapshot_time"].min() for p, df in members.items()]
    ends = [df[f"{p}_snapshot_time"].max() for p, df in members.items()]
    # Union coverage: members are born and die at different times; the grid
    # spans first-to-last observation, and per-member nulls/staleness convey
    # who was alive. (Intersection coverage can be empty for real groups.)
    start = min(starts)
    if start.second or start.microsecond:
        start = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
    end = max(ends)
    grid_panel = None
    if start < end:
        grid_times = pl.DataFrame({
            "time": pl.datetime_range(
                start=start, end=end, interval=grid_interval,
                time_zone="UTC", eager=True,
            )
        })
        for p, df in members.items():
            grid_times = _asof_align(df, p, grid_times)
        grid_panel = _add_group_columns(grid_times, prefixes, n_members_total, full_cover_ok)
        grid_panel = grid_panel.with_columns(pl.lit(event_slug).alias("event_slug"))

    return event_panel, grid_panel
