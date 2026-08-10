"""Select an event-level market universe for the expanded study.

Replaces top-N-markets-by-final-volume selection with selection at the EVENT
level, so every chosen group is downloaded COMPLETE (all sibling markets,
including cheap tail outcomes — required for full-cover rebalancing labels).

Strata:
  negrisk  — negRisk (winner-take-all) events: rebalancing-arbitrage material.
  ladder   — non-negRisk events with >=3 markets sharing a question template
             that differs only in a date/number ("by January 31?", "reach $X"):
             monotone-implication (combinatorial) arbitrage material.
  serial   — serial 15-minute "Up or Down" crypto markets in a short date
             slice: the only genuinely fee-bearing markets in the sample
             window (taker fees since ~Oct 2025).
  control  — seeded random sample of otherwise-unselected mid-volume events,
             kept to measure what volume-based selection does to results.

(The original 70-market pilot is NOT forced in: pilot events that matter —
Super Bowl champion, Fed decisions, the Iran ladder, Bitcoin-January — qualify
on merit; the rest were single binaries with no cross-market structure.)

Selection caveats (documented, not hidden): event ranking uses lifetime volume
from the metadata dump — an ex-post quantity. It caps the *download* universe
(API budget); analyses should re-select point-in-time within the downloaded
data and use the control stratum to quantify residual selection effects.

Outputs (no downloading here — review the printed budget first):
  data/processed/event_universe.parquet             one row per selected market
  data/processed/universe_token_ids.parquet         combined token_id list
  data/processed/universe_token_ids_<stratum>.parquet   per-stratum lists

Download in stages (recommended: negrisk first, it carries the core labels):
  python data-pipeline/run_data_loader.py --token-ids-path data/processed/universe_token_ids_negrisk.parquet
"""

import argparse
import json
import random
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _resolve_project_root() -> Path:
    cwd = Path.cwd().resolve()
    return cwd.parent if cwd.name == "data-pipeline" else cwd


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.paths import MARKET_JSONL, EVENT_UNIVERSE, UNIVERSE_TOKEN_IDS

# Pilot calibration (140 tokens, 3.32M snapshots, top-70-by-volume markets)
# for the budget estimate. Snapshot counts scale with trading activity, so the
# pilot per-token rate is an UPPER bound for a broader (less active) universe;
# the "likely" figure scales it by the volume ratio, floored at 1k/token.
PILOT_SNAPSHOTS_PER_TOKEN = 23_735
PILOT_AVG_MARKET_VOLUME = 3.0e7  # approximate
API_PAGE_SIZE = 200
SERIAL_SNAPSHOTS_PER_TOKEN = 500  # 15-min lifetime; rough allowance


def parse_args():
    p = argparse.ArgumentParser(description="Select an event-level market universe.")
    p.add_argument("--window-start", type=str, default="2025-10-14",
                   help="Dome history start; markets must be alive after this.")
    p.add_argument("--window-end", type=str, default="2026-02-04",
                   help="Markets must have started before this.")
    p.add_argument("--top-negrisk", type=int, default=50)
    p.add_argument("--top-ladder", type=int, default=30)
    p.add_argument("--n-control", type=int, default=25)
    p.add_argument("--control-seed", type=int, default=42)
    p.add_argument("--min-event-volume", type=float, default=1e6,
                   help="Event lifetime-volume floor for negrisk/ladder ranking.")
    p.add_argument("--max-event-size", type=int, default=35,
                   help="Skip events with more markets than this (64-outcome "
                        "poker brackets etc. add download cost, not signal).")
    p.add_argument("--serial-assets", type=str, default="bitcoin,ethereum")
    p.add_argument("--serial-start", type=str, default="2026-01-12")
    p.add_argument("--serial-days", type=int, default=3)
    return p.parse_args()


def _ts(value):
    return pd.to_datetime(value, utc=True, errors="coerce")


_TEMPLATE_STRIP = re.compile(
    r"\$?[\d,.]+%?|january|february|march|april|may|june|july|august|september"
    r"|october|november|december|monday|tuesday|wednesday|thursday|friday"
    r"|saturday|sunday",
    re.IGNORECASE,
)


def question_template(q: str) -> str:
    """Strip numbers/dates so ladder siblings collapse to one template."""
    return re.sub(r"\s+", " ", _TEMPLATE_STRIP.sub("#", q or "")).strip().lower()


def scan_markets(jsonl_path: Path, w0: pd.Timestamp, w1: pd.Timestamp) -> pd.DataFrame:
    rows = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                mk = json.loads(line)
            except json.JSONDecodeError:
                continue
            if mk.get("enableOrderBook") is not True:
                continue
            start = _ts(mk.get("startDate"))
            end = _ts(mk.get("closedTime"))
            if pd.isna(end):
                end = _ts(mk.get("endDate"))
            # Life must overlap the download window. NOTE: resolution is NOT
            # required — labels are quote-based, and requiring closed_time
            # biases the sample toward short-lived markets.
            if pd.isna(start) or pd.isna(end) or start >= w1 or end <= w0:
                continue
            tokens = mk.get("clobTokenIds")
            if isinstance(tokens, str):
                try:
                    tokens = json.loads(tokens)
                except json.JSONDecodeError:
                    tokens = []
            if not tokens:
                continue
            evs = mk.get("events") or []
            if isinstance(evs, str):
                try:
                    evs = json.loads(evs)
                except json.JSONDecodeError:
                    evs = []
            ev = evs[0] if evs else {}
            mid = str(mk.get("id"))
            prices = mk.get("outcomePrices")
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except json.JSONDecodeError:
                    prices = []
            prices = [float(x) for x in (prices or [])]
            rows.append({
                "market_id": mid,
                "question": mk.get("question"),
                "slug": mk.get("slug"),
                # singleton pseudo-event when gamma provides none
                "event_slug": ev.get("slug") or f"market-{mid}",
                "event_title": ev.get("title"),
                "neg_risk": bool(mk.get("negRisk")),
                "volume": float(mk.get("volume") or 0.0),
                "start_date": start,
                "end_date": end,
                "n_tokens": len(tokens),
                "token_ids": json.dumps([str(t) for t in tokens]),
                "is_serial": "up or down" in (mk.get("question") or "").lower(),
                # Match-style events ("X vs Y", per-game props) share question
                # templates but have NO monotone implication structure — they
                # must never enter the ladder stratum (semantic audit 2026-08-08
                # caught 4 LoL BO5 bundles doing exactly that).
                "is_match": bool(re.search(
                    r"\bvs\.?\b",
                    f"{mk.get('question') or ''} {ev.get('title') or ''}".lower(),
                )),
                # The dump holds only resolved markets, so outcomePrices are
                # final: outcome 0 at ~1.0 means this market resolved YES.
                "resolved_yes": bool(prices) and prices[0] >= 0.99,
            })
    return pd.DataFrame(rows)


def main():
    args = parse_args()
    w0 = pd.Timestamp(args.window_start, tz="UTC")
    w1 = pd.Timestamp(args.window_end, tz="UTC")

    print(f"Scanning {MARKET_JSONL} for markets alive in [{w0.date()}, {w1.date()}]...")
    df = scan_markets(MARKET_JSONL, w0, w1)
    print(f"In-window orderbook markets: {len(df):,}")

    df["template"] = df["question"].map(question_template)

    # ---- event-level table (serial markets handled as their own stratum) ----
    nonserial = df[~df["is_serial"]]
    ev = nonserial.groupby("event_slug").agg(
        n_markets=("market_id", "size"),
        event_volume=("volume", "sum"),
        neg_risk=("neg_risk", "max"),
        is_match=("is_match", "max"),
        n_templates=("template", "nunique"),
        max_template_group=("template", lambda s: s.value_counts().iloc[0]),
    )
    ev["is_ladder"] = (
        (~ev["neg_risk"]) & (~ev["is_match"]) & (ev["max_template_group"] >= 3)
    )

    # ---- strata selection ----
    eligible = ev[
        (ev["event_volume"] >= args.min_event_volume)
        & (ev["n_markets"] <= args.max_event_size)
    ]

    negrisk_sel = (
        eligible[eligible["neg_risk"] & (eligible["n_markets"] >= 3)]
        .sort_values("event_volume", ascending=False)
        .head(args.top_negrisk)
        .index
    )
    ladder_sel = (
        eligible[eligible["is_ladder"] & ~eligible.index.isin(negrisk_sel)]
        .sort_values("event_volume", ascending=False)
        .head(args.top_ladder)
        .index
    )

    taken = set(negrisk_sel) | set(ladder_sel)
    control_pool = ev[
        (~ev.index.isin(taken))
        & ev["event_volume"].between(1e5, 5e6)
        & ev["n_markets"].between(2, args.max_event_size)
    ].index.tolist()
    rng = random.Random(args.control_seed)
    control_sel = rng.sample(sorted(control_pool), min(args.n_control, len(control_pool)))

    # ---- serial crypto slice ----
    assets = [a.strip().lower() for a in args.serial_assets.split(",") if a.strip()]
    s0 = pd.Timestamp(args.serial_start, tz="UTC")
    s1 = s0 + pd.Timedelta(days=args.serial_days)
    serial = df[
        df["is_serial"]
        & df["question"].str.lower().str.contains("|".join(assets), regex=True)
        & (df["start_date"] >= s0) & (df["start_date"] < s1)
    ]

    # ---- assemble: complete groups for every selected event ----
    def stratum_of(slug: str) -> str:
        if slug in set(negrisk_sel):
            return "negrisk"
        if slug in set(ladder_sel):
            return "ladder"
        return "control"

    selected_events = set(negrisk_sel) | set(ladder_sel) | set(control_sel)
    universe = nonserial[nonserial["event_slug"].isin(selected_events)].copy()
    universe["stratum"] = universe["event_slug"].map(stratum_of)

    serial = serial.copy()
    serial["stratum"] = "serial"
    universe = pd.concat([universe, serial], ignore_index=True)

    # Group-completeness flag (decision 2026-08-08: keep incomplete groups,
    # short-side labels only). A winner-take-all group is complete iff its
    # winning market is in the dump; events that concluded after the dump froze
    # (~Feb 4 2026) lack their surviving markets, so full-cover long-side
    # rebalancing labels are invalid there. Subset short-side labels never
    # reference missing outcomes and remain valid on every group.
    winner_present = universe.groupby("event_slug")["resolved_yes"].transform("max")
    universe["winner_present"] = winner_present
    universe["supports_full_cover"] = (universe["stratum"] == "negrisk") & winner_present

    # ---- outputs ----
    EVENT_UNIVERSE.parent.mkdir(parents=True, exist_ok=True)
    universe.drop(columns=["template"], errors="ignore").to_parquet(EVENT_UNIVERSE, index=False)

    token_ids = sorted({t for s in universe["token_ids"] for t in json.loads(s)})
    pd.DataFrame({"token_id": token_ids}).to_parquet(UNIVERSE_TOKEN_IDS, index=False)

    for stratum, grp in universe.groupby("stratum"):
        st_tokens = sorted({t for s in grp["token_ids"] for t in json.loads(s)})
        st_path = UNIVERSE_TOKEN_IDS.with_name(
            UNIVERSE_TOKEN_IDS.stem + f"_{stratum}" + UNIVERSE_TOKEN_IDS.suffix
        )
        pd.DataFrame({"token_id": st_tokens}).to_parquet(st_path, index=False)

    # ---- budget report ----
    print(f"\nSaved {len(universe):,} markets -> {EVENT_UNIVERSE}")
    print(f"Saved {len(token_ids):,} token ids -> {UNIVERSE_TOKEN_IDS}\n")

    neg = universe[universe["stratum"] == "negrisk"]
    n_groups = neg["event_slug"].nunique()
    n_full = neg[neg["supports_full_cover"]]["event_slug"].nunique()
    print(f"negRisk groups with winner present (full-cover-capable): {n_full}/{n_groups}")
    incomplete = neg[~neg["supports_full_cover"]].groupby("event_slug")["event_title"].first()
    for slug, title in incomplete.items():
        print(f"  short-side-only: {title} ({slug})")
    print()
    summary = universe.groupby("stratum").agg(
        events=("event_slug", "nunique"),
        markets=("market_id", "size"),
        tokens=("n_tokens", "sum"),
        volume=("volume", "sum"),
    )
    print(summary.to_string(float_format=lambda x: f"{x:,.0f}"))

    print("\nBudget estimate per stratum (likely / upper bound):")
    total_likely = total_upper = 0.0
    for stratum, grp in universe.groupby("stratum"):
        n_tok = int(grp["n_tokens"].sum())
        if stratum == "serial":
            likely = upper = n_tok * SERIAL_SNAPSHOTS_PER_TOKEN
        else:
            avg_vol = grp["volume"].mean()
            per_tok = np.clip(
                PILOT_SNAPSHOTS_PER_TOKEN * avg_vol / PILOT_AVG_MARKET_VOLUME,
                1_000, PILOT_SNAPSHOTS_PER_TOKEN,
            )
            likely = n_tok * per_tok
            upper = n_tok * PILOT_SNAPSHOTS_PER_TOKEN
        total_likely += likely
        total_upper += upper
        print(f"  {stratum:<8} {n_tok:>6,} tokens  ~{likely/1e6:6.1f}M / {upper/1e6:6.1f}M snapshots")
    print(f"  TOTAL    {len(token_ids):>6,} tokens  ~{total_likely/1e6:6.1f}M / {total_upper/1e6:6.1f}M snapshots")
    print(f"  API requests ~{total_likely/API_PAGE_SIZE/1e3:,.0f}k / {total_upper/API_PAGE_SIZE/1e3:,.0f}k"
          f"  |  disk ~{total_likely/3_322_971*748/1024:,.1f} / {total_upper/3_322_971*748/1024:,.1f} GB")
    print(f"  (pilot reference: 140 tokens -> 3.32M snapshots, 748 MB)")
    print("\nNo data was downloaded. Review the numbers, then download in stages:")
    for stratum in universe["stratum"].unique():
        st_path = UNIVERSE_TOKEN_IDS.with_name(
            UNIVERSE_TOKEN_IDS.stem + f"_{stratum}" + UNIVERSE_TOKEN_IDS.suffix
        )
        print(f"  python data-pipeline/run_data_loader.py --token-ids-path {st_path}")


if __name__ == "__main__":
    main()
