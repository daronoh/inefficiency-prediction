"""Stage 2: cost-aware opportunity spells, model labels, attrition layers.

Works on the group event panels (src/groups.py). An *opportunity spell* is a
maximal run of consecutive event-grid rows on which an executable gap measure
is positive net of a (counterfactual) taker fee:

    net_gap = exec_gap_q − feeRate × fee_basis_q      (feeRate 0 = historical)

Spell persistence = end_time − start_time (a single-row flicker has duration
0s and is dropped by any positive persistence threshold).

Staleness is deliberately NOT a hard gate on spells: Dome's capture is
change-triggered, so a carried-forward book state is the live book unless a
capture outage swallowed updates. Each spell records the max member staleness
observed inside it (`max_staleness_s`), and the attrition table reports a
fresh-only (≤30s) variant so both readings are visible.

Model labels live on the wall-clock grid: y = 1 iff a qualifying spell STARTS
in (t, t+H]. Every output row is tagged development/lockbox per the binding
evaluation protocol (DEV_END below); exploration must filter on it.
"""

import numpy as np
import pandas as pd
import polars as pl

DEV_END = pd.Timestamp("2026-01-08", tz="UTC")

SIZES = (10, 100, 1000)
FEE_RATES = (0.0, 0.04, 0.05, 0.07)
PERSISTENCE_S = (0.0, 5.0, 30.0)
HORIZONS_MIN = (15, 60)

GROUP_COLS = (
    ["time", "event_slug", "long_gap", "short_gap", "max_staleness_s", "all_books_clean"]
    + [f"{m}_{q}" for q in SIZES
       for m in ("exec_long_gap", "fee_basis_long", "exec_short_gap", "fee_basis_short",
                 "short_clean")]
)


def find_spells(panel: pl.DataFrame, gap_col: str, fee_basis_col: str,
                fee_rate: float, clean_col: str = None) -> pl.DataFrame:
    """Maximal runs of net_gap > 0 with per-spell metrics.

    clean_col names a boolean book-quality gate ANDed into the condition —
    'all_books_clean' for long spells (every present member two-sided and
    non-crossed), 'short_clean_<q>' for short spells (every CONTRIBUTING
    member clean). The gates remove resolution-transition phantom books.
    """
    cols = ["time", gap_col, fee_basis_col, "max_staleness_s"]
    if clean_col:
        cols.append(clean_col)
    df = panel.select(cols).with_columns(
        (pl.col(gap_col) - fee_rate * pl.col(fee_basis_col)).alias("net_gap")
    ).with_columns(
        (
            (pl.col("net_gap") > 0)
            & (pl.col(clean_col) if clean_col else pl.lit(True))
        ).fill_null(False).alias("cond")
    ).with_columns([
        (pl.col("cond") != pl.col("cond").shift(1, fill_value=False)).cum_sum().alias("run_id"),
        # A spell lives until the FLIP row: between event-grid rows every book
        # is unchanged by construction, so the condition provably holds until
        # the next row. next_time of the run's last row = the spell's open end.
        pl.col("time").shift(-1).fill_null(pl.col("time")).alias("next_time"),
    ])
    spells = (
        df.filter(pl.col("cond"))
        .group_by("run_id")
        .agg([
            pl.col("time").min().alias("start_time"),
            pl.col("time").max().alias("last_true_time"),
            pl.col("next_time").max().alias("end_time"),
            pl.len().alias("n_rows"),
            pl.col("net_gap").max().alias("max_net_gap"),
            pl.col("net_gap").mean().alias("mean_net_gap"),
            pl.col("max_staleness_s").max().alias("max_staleness_s"),
        ])
        .drop("run_id")
        .sort("start_time")
    )
    return spells.with_columns(
        ((pl.col("end_time") - pl.col("start_time")).dt.total_milliseconds() / 1000.0)
        .alias("duration_s")
    )


def spells_for_event(event_panel: pl.DataFrame, event_slug: str) -> pd.DataFrame:
    """All spell definitions for one event's panel -> tidy pandas frame."""
    out = []
    for side in ("long", "short"):
        for q in SIZES:
            gap_col = f"exec_{side}_gap_{q}"
            basis_col = f"fee_basis_{side}_{q}"
            if event_panel[gap_col].null_count() == len(event_panel):
                continue
            clean_col = "all_books_clean" if side == "long" else f"short_clean_{q}"
            for fee in FEE_RATES:
                sp = find_spells(event_panel, gap_col, basis_col, fee,
                                 clean_col=clean_col)
                if sp.is_empty():
                    continue
                pdf = sp.to_pandas()
                pdf["event_slug"] = event_slug
                pdf["side"] = side
                pdf["size_q"] = q
                pdf["fee_rate"] = fee
                out.append(pdf)
    if not out:
        return pd.DataFrame()
    res = pd.concat(out, ignore_index=True)
    res["split"] = np.where(res["start_time"] < DEV_END, "development", "lockbox")
    # A development spell whose open end crosses the boundary must not carry
    # lockbox time into development duration/attrition: clip at the boundary.
    straddle = (res["split"] == "development") & (res["end_time"] > DEV_END)
    if straddle.any():
        res.loc[straddle, "end_time"] = DEV_END
        res.loc[straddle, "duration_s"] = (
            (res.loc[straddle, "end_time"] - res.loc[straddle, "start_time"])
            .dt.total_seconds()
        )
    return res


def grid_labels_for_event(grid_times: pd.Series, spells: pd.DataFrame,
                          event_slug: str) -> pd.DataFrame:
    """Binary labels on the wall-clock grid: spell-start within (t, t+H].

    Label definition (frozen on development data): side=long, fee_rate=0,
    persistence >= 5 s, per size and horizon. Also emits the continuous
    forward max net gap for size 100.
    """
    # Keep the tz-aware dtype: .values would strip UTC and break comparisons
    # against the tz-aware DEV_END boundary.
    lab = pd.DataFrame({"time": pd.Series(grid_times).reset_index(drop=True)})
    lab["event_slug"] = event_slug
    if spells.empty:
        for q in SIZES:
            for h in HORIZONS_MIN:
                lab[f"y_long_{q}_{h}m"] = 0
        lab["max_gap_100_60m"] = 0.0
        lab["split"] = np.where(lab["time"] < DEV_END, "development", "lockbox")
        embargo_start = DEV_END - pd.Timedelta(minutes=max(HORIZONS_MIN))
        lab = lab[~((lab["split"] == "development") & (lab["time"] >= embargo_start))]
        return lab.reset_index(drop=True)
    for q in SIZES:
        qual = spells[
            (spells["side"] == "long") & (spells["size_q"] == q)
            & (spells["fee_rate"] == 0.0) & (spells["duration_s"] >= 5.0)
        ].sort_values("start_time")
        starts = qual["start_time"].values
        for h in HORIZONS_MIN:
            if len(starts) == 0:
                lab[f"y_long_{q}_{h}m"] = 0
                continue
            t = lab["time"].values
            hi = np.searchsorted(starts, t + np.timedelta64(h, "m"), side="right")
            lo = np.searchsorted(starts, t, side="right")
            lab[f"y_long_{q}_{h}m"] = (hi > lo).astype(np.int8)
        if q == 100 and len(qual):
            # forward-looking max net gap over the 60m horizon (continuous target)
            t = lab["time"].values
            gmax = np.zeros(len(t))
            s, e, g = qual["start_time"].values, qual["end_time"].values, qual["max_net_gap"].values
            for i, ti in enumerate(t):
                m = (s > ti) & (s <= ti + np.timedelta64(60, "m"))
                gmax[i] = g[m].max() if m.any() else 0.0
            lab["max_gap_100_60m"] = gmax
        elif q == 100:
            lab["max_gap_100_60m"] = 0.0
    lab["split"] = np.where(lab["time"] < DEV_END, "development", "lockbox")
    # Boundary embargo: development rows whose label window crosses into the
    # lockbox would let lockbox spells reach development labels. Drop them
    # (max horizon = 60m; ~0.02% of rows).
    embargo_start = DEV_END - pd.Timedelta(minutes=max(HORIZONS_MIN))
    lab = lab[~((lab["split"] == "development") & (lab["time"] >= embargo_start))]
    return lab.reset_index(drop=True)


def attrition_table(all_spells: pd.DataFrame, dev_only: bool = True) -> pd.DataFrame:
    """Layered survival counts — the thesis's headline exhibit.

    Rows = successive friction layers; columns = surviving spell counts,
    events touched, time-in-opportunity, size of the median surviving gap.
    """
    sp = all_spells[all_spells["split"] == "development"] if dev_only else all_spells

    layers = []

    def add(name, mask):
        s = sp[mask]
        layers.append({
            "layer": name,
            "n_spells": len(s),
            "n_events": s["event_slug"].nunique(),
            "total_minutes": round(s["duration_s"].sum() / 60.0, 1),
            "median_duration_s": round(s["duration_s"].median(), 1) if len(s) else 0.0,
            "median_max_gap": round(s["max_net_gap"].median(), 4) if len(s) else 0.0,
        })

    long0 = (sp["side"] == "long") & (sp["fee_rate"] == 0.0)
    for q in SIZES:
        add(f"executable at {q} shares (fee 0)", long0 & (sp["size_q"] == q))
    base = long0 & (sp["size_q"] == 100)
    for per in PERSISTENCE_S[1:]:
        add(f"100 sh + persist >= {per:.0f}s", base & (sp["duration_s"] >= per))
    for fee in FEE_RATES[1:]:
        add(f"100 sh + persist >= 5s + fee {fee:.2f}",
            (sp["side"] == "long") & (sp["size_q"] == 100) & (sp["fee_rate"] == fee)
            & (sp["duration_s"] >= 5.0))
    add("fresh only (staleness <= 30s), 100 sh, persist >= 5s, fee 0",
        base & (sp["duration_s"] >= 5.0) & (sp["max_staleness_s"] <= 30.0))
    add("short side, 100 sh, persist >= 5s, fee 0",
        (sp["side"] == "short") & (sp["size_q"] == 100) & (sp["fee_rate"] == 0.0)
        & (sp["duration_s"] >= 5.0))
    return pd.DataFrame(layers)
