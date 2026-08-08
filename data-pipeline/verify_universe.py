"""Verify the event universe against the raw JSONL: invariants + eyeball samples.

Re-derives the selection's stated rules directly from the metadata dump and
checks the stored parquet against them. Run it any time you want to audit the
stratification; every check prints PASS or FAIL with details.

  python data-pipeline/verify_universe.py
"""

import json
import random
import sys
from pathlib import Path

import pandas as pd


def _resolve_project_root() -> Path:
    cwd = Path.cwd().resolve()
    return cwd.parent if cwd.name == "data-pipeline" else cwd


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "data-pipeline"))

from src.paths import MARKET_JSONL, EVENT_UNIVERSE
from select_event_universe import question_template

WINDOW = (pd.Timestamp("2025-10-14", tz="UTC"), pd.Timestamp("2026-02-04", tz="UTC"))

_failures = []


def check(name: str, ok: bool, detail: str = ""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        _failures.append(name)


def scan_candidates():
    """Independent pass over the JSONL applying the stated candidate rules."""
    w0, w1 = WINDOW
    rows = []
    with open(MARKET_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            mk = json.loads(line)
            if mk.get("enableOrderBook") is not True:
                continue
            start = pd.to_datetime(mk.get("startDate"), utc=True, errors="coerce")
            end = pd.to_datetime(mk.get("closedTime"), utc=True, errors="coerce")
            if pd.isna(end):
                end = pd.to_datetime(mk.get("endDate"), utc=True, errors="coerce")
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
                "question": mk.get("question") or "",
                "event_slug": ev.get("slug") or f"market-{mid}",
                "neg_risk": bool(mk.get("negRisk")),
                "volume": float(mk.get("volume") or 0.0),
                "start_date": start,
                "is_serial": "up or down" in (mk.get("question") or "").lower(),
                "resolved_yes": bool(prices) and prices[0] >= 0.99,
            })
    return pd.DataFrame(rows)


def main():
    u = pd.read_parquet(EVENT_UNIVERSE)
    print(f"Universe: {len(u):,} markets, strata: {u['stratum'].value_counts().to_dict()}\n")

    print(f"Scanning {MARKET_JSONL} (independent candidate pass)...")
    cand = scan_candidates()
    print(f"Candidates in window: {len(cand):,}\n")

    # V1: strata are disjoint and every market has exactly one stratum
    check("V1 one stratum per market", u["market_id"].is_unique and u["stratum"].notna().all())
    ev_strata = u[u["stratum"] != "serial"].groupby("event_slug")["stratum"].nunique()
    check("V1b one stratum per event", (ev_strata == 1).all())

    # V2: every negrisk event has the negRisk flag in the RAW data
    neg_slugs = set(u.loc[u["stratum"] == "negrisk", "event_slug"])
    raw_neg = cand.groupby("event_slug")["neg_risk"].max()
    bad = [s for s in neg_slugs if not raw_neg.get(s, False)]
    check("V2 negrisk events carry raw negRisk=true", not bad, f"violations: {bad[:5]}")

    # V3: complete groups — every candidate sibling of a selected event is included
    sel_slugs = set(u.loc[u["stratum"] != "serial", "event_slug"])
    cand_counts = cand[~cand["is_serial"]].groupby("event_slug")["market_id"].nunique()
    uni_counts = u[u["stratum"] != "serial"].groupby("event_slug")["market_id"].nunique()
    incomplete = {s: (uni_counts.get(s, 0), int(cand_counts.get(s, 0)))
                  for s in sel_slugs if uni_counts.get(s, 0) != cand_counts.get(s, 0)}
    check("V3 groups are complete", not incomplete, f"universe vs jsonl: {dict(list(incomplete.items())[:5])}")

    # V4: quota/floor/size conformance
    ev = u[u["stratum"] != "serial"].groupby(["event_slug", "stratum"]).agg(
        n=("market_id", "size"), vol=("volume", "sum")).reset_index()
    for st, quota in [("negrisk", 50), ("ladder", 30), ("control", 25)]:
        n_ev = (ev["stratum"] == st).sum()
        check(f"V4 {st} event count <= {quota}", n_ev <= quota, f"got {n_ev}")
    check("V4 size cap <= 35", (ev["n"] <= 35).all(),
          str(ev[ev["n"] > 35][["event_slug", "n"]].values[:3]))
    core = ev[ev["stratum"].isin(["negrisk", "ladder"])]
    check("V4 negrisk/ladder volume floor >= $1M", (core["vol"] >= 1e6).all())
    ctrl = ev[ev["stratum"] == "control"]
    check("V4 control volume in [100k, 5M]",
          ctrl["vol"].between(1e5, 5e6).all(), str(ctrl[~ctrl["vol"].between(1e5, 5e6)]["event_slug"].tolist()))

    # V5: no eligible negrisk event with more volume was skipped
    cand_ev = cand[~cand["is_serial"]].groupby("event_slug").agg(
        n=("market_id", "size"), vol=("volume", "sum"), neg=("neg_risk", "max"))
    eligible = cand_ev[(cand_ev["neg"]) & (cand_ev["n"] >= 3) & (cand_ev["n"] <= 35) & (cand_ev["vol"] >= 1e6)]
    top50 = set(eligible.sort_values("vol", ascending=False).head(50).index)
    check("V5 negrisk selection == independent top-50 by volume", top50 == neg_slugs,
          f"missing: {list(top50 - neg_slugs)[:3]} extra: {list(neg_slugs - top50)[:3]}")

    # V6: ladder events satisfy the template rule and are not negrisk
    lad = u[u["stratum"] == "ladder"]
    ok_lad = True
    for slug, grp in lad.groupby("event_slug"):
        tmpl_max = grp["question"].map(question_template).value_counts().iloc[0]
        if tmpl_max < 3 or grp["neg_risk"].max():
            ok_lad = False
            print(f"       ladder violation: {slug} (max template group {tmpl_max}, neg={grp['neg_risk'].max()})")
    check("V6 ladder events: >=3 shared-template members, non-negrisk", ok_lad)

    # V7: serial slice
    ser = u[u["stratum"] == "serial"]
    in_slice = ser["start_date"].between(pd.Timestamp("2026-01-12", tz="UTC"),
                                         pd.Timestamp("2026-01-15", tz="UTC"))
    has_asset = ser["question"].str.lower().str.contains("bitcoin|ethereum")
    is_ud = ser["question"].str.lower().str.contains("up or down")
    check("V7 serial: up-or-down BTC/ETH within Jan 12-15", (in_slice & has_asset & is_ud).all())

    # V8: supports_full_cover consistency with raw outcomePrices
    raw_winner = cand.groupby("event_slug")["resolved_yes"].max()
    for slug in sorted(neg_slugs):
        stored = bool(u.loc[u["event_slug"] == slug, "supports_full_cover"].max())
        derived = bool(raw_winner.get(slug, False))
        if stored != derived:
            check(f"V8 supports_full_cover {slug}", False, f"stored {stored} vs raw {derived}")
            break
    else:
        check("V8 supports_full_cover matches raw outcomePrices (all 50)", True)

    # Eyeball samples
    print("\n--- eyeball samples (judge these yourself) ---")
    rng = random.Random(0)
    for st in ["negrisk", "ladder", "control", "serial"]:
        grp = u[u["stratum"] == st]
        titles = grp.groupby("event_slug")["event_title"].first().dropna().tolist() or grp["question"].tolist()
        picks = rng.sample(titles, min(6, len(titles)))
        print(f"\n{st}:")
        for t in picks:
            print(f"  - {t}")

    print(f"\n{'ALL CHECKS PASSED' if not _failures else 'FAILURES: ' + ', '.join(_failures)}")
    sys.exit(1 if _failures else 0)


if __name__ == "__main__":
    main()
