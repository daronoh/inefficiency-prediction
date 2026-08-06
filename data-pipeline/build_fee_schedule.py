"""Build a dated per-market fee schedule for the selected markets.

Three sources, combined into data/processed/fee_schedule.parquet:

1. The local markets JSONL (reproducible): event title/category per market.
2. The documented fee rollout (dated regime table below): category -> feeRate
   over time. Polymarket taker fee = shares * feeRate * p * (1-p); makers pay 0.
3. The live CLOB fee-rate endpoint (best effort, may be unreachable offline):
   current feeRateBps per token, to validate the regime table.

Rollout dates below are from public reporting; ones marked approximate should be
re-verified before the thesis quotes them.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _resolve_project_root() -> Path:
    cwd = Path.cwd().resolve()
    return cwd.parent if cwd.name == "data-pipeline" else cwd


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.paths import MARKET_JSONL, PROCESSED_DIR, FILTERED_TOKEN_IDS, FEE_SCHEDULE

CLOB_FEE_URL = "https://clob.polymarket.com/fee-rate"

# category bucket -> list of (effective_from ISO date, feeRate); intervals apply
# from that date until the next entry. Categories absent here are fee-free.
FEE_REGIME = {
    "crypto": [("2026-01-01", 0.07)],           # all crypto markets (~Jan 2026; 15-min crypto had taker fees from ~Oct 2025)
    "sports": [("2026-02-18", 0.03), ("2026-07-01", 0.05)],  # hike date approximate (Jul 2026)
    "economics": [("2026-03-30", 0.05)],
    "culture": [("2026-03-30", 0.05)],
    "weather": [("2026-03-30", 0.05)],
    "other": [("2026-03-30", 0.05)],
    "finance": [("2026-03-30", 0.04)],
    "politics": [("2026-03-30", 0.04)],
    "tech": [("2026-03-30", 0.04)],
    "mentions": [("2026-03-30", 0.04)],
    "geopolitics": [],                           # permanently fee-free
}

# Keyword rules mapping raw event categories/titles to fee buckets. Anything
# unmatched comes out as "unknown" and must be classified by hand.
BUCKET_KEYWORDS = [
    ("crypto", ["crypto", "bitcoin", "btc", "ethereum", "eth ", "solana", "xrp", "dogecoin",
                "market cap", "fdv", "launch a token"]),
    ("sports", ["sport", "nba", "nfl", "nhl", "mlb", "epl", "premier league", "ufc", "soccer",
                "football", "basketball", "hockey", "baseball", "tennis", "golf", "f1",
                " vs. ", "champions league", "fc win"]),
    ("mentions", ["mention", "tweet", "truth social", "say ", "says "]),
    ("geopolitics", ["geopolit", "war", "ceasefire", "ukraine", "gaza", "iran", "nato",
                     "maduro", "cartel", "khamenei"]),
    ("politics", ["politic", "election", "president", "senate", "congress", "mayor", "primary",
                  "shutdown"]),
    ("finance", ["finance", "fed ", "interest rate", "stock", "s&p", "nasdaq", "ipo"]),
    ("economics", ["econom", "inflation", "cpi", "gdp", "jobs report", "unemployment"]),
    ("tech", ["tech", " ai ", "openai", "spacex launch", "iphone", "tesla", "self driving"]),
    ("culture", ["culture", "movie", "oscar", "grammy", "music", "celebrity", "box office",
                 "stranger things", "episode released"]),
    ("weather", ["weather", "temperature", "hurricane", "rainfall"]),
]


def classify_bucket(*texts: str) -> str:
    blob = " ".join(t for t in texts if t).lower()
    for bucket, kws in BUCKET_KEYWORDS:
        if any(kw in blob for kw in kws):
            return bucket
    return "unknown"


def fee_rate_at(bucket: str, ts: pd.Timestamp) -> float:
    rate = 0.0
    for start, r in FEE_REGIME.get(bucket, []):
        if ts >= pd.Timestamp(start, tz="UTC"):
            rate = r
    return rate


def extract_event_metadata(jsonl_path: Path, market_ids: set) -> dict:
    """Scan the (large) markets JSONL once, keeping event info for selected ids."""
    out = {}
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                market = json.loads(line)
            except json.JSONDecodeError:
                continue
            mid = str(market.get("id"))
            if mid not in market_ids:
                continue
            events = market.get("events") or []
            if isinstance(events, str):
                try:
                    events = json.loads(events)
                except json.JSONDecodeError:
                    events = []
            ev = events[0] if events else {}
            out[mid] = {
                "question": market.get("question"),
                "event_title": ev.get("title"),
                "event_slug": ev.get("slug"),
                "event_category_raw": ev.get("category"),
                "event_tags": json.dumps([t.get("label") for t in (ev.get("tags") or []) if isinstance(t, dict)]),
                "gamma_fee_raw": market.get("fee"),
            }
            if len(out) == len(market_ids):
                break
    return out


def fetch_live_fee_rate(token_id: str, timeout: float = 10.0):
    try:
        import requests
        resp = requests.get(CLOB_FEE_URL, params={"token_id": token_id}, timeout=timeout)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Build the per-market fee schedule table.")
    parser.add_argument("--market-jsonl", type=Path, default=MARKET_JSONL)
    parser.add_argument("--tokens-path", type=Path, default=PROCESSED_DIR / "tokens.parquet")
    parser.add_argument("--filtered-tokens-path", type=Path, default=FILTERED_TOKEN_IDS)
    parser.add_argument("--output", type=Path, default=FEE_SCHEDULE)
    parser.add_argument("--sample-end", type=str, default="2026-02-04",
                        help="End of the orderbook sample, for the in-sample fee columns.")
    parser.add_argument("--skip-live", action="store_true", help="Skip the live CLOB endpoint.")
    args = parser.parse_args()

    token_ids = set(pd.read_parquet(args.filtered_tokens_path)["token_id"].astype(str))
    tokens_df = pd.read_parquet(args.tokens_path)
    tokens_df = tokens_df[tokens_df["token_id"].astype(str).isin(token_ids)]
    market_ids = set(tokens_df["market_id"].astype(str))
    # one representative token per market for the live endpoint
    rep_token = (
        tokens_df.assign(token_id=tokens_df["token_id"].astype(str))
        .sort_values("outcome_index")
        .groupby("market_id")["token_id"].first()
        .to_dict()
    )

    print(f"Scanning {args.market_jsonl} for {len(market_ids)} markets...")
    meta = extract_event_metadata(args.market_jsonl, market_ids)
    print(f"Found event metadata for {len(meta)} markets")

    now = pd.Timestamp(datetime.now(timezone.utc))
    sample_end = pd.Timestamp(args.sample_end, tz="UTC")

    rows = []
    for mid in sorted(market_ids):
        m = meta.get(mid, {})
        bucket = classify_bucket(
            m.get("event_category_raw") or "",
            m.get("event_title") or "",
            m.get("question") or "",
            m.get("event_tags") or "",
        )
        live = None
        if not args.skip_live:
            live = fetch_live_fee_rate(rep_token.get(mid, ""))
        starts = FEE_REGIME.get(bucket, [])
        rows.append({
            "market_id": mid,
            "question": m.get("question"),
            "event_title": m.get("event_title"),
            "event_category_raw": m.get("event_category_raw"),
            "event_tags": m.get("event_tags"),
            "category_bucket": bucket,
            "gamma_fee_raw": str(m.get("gamma_fee_raw")),
            "fee_rate_current": fee_rate_at(bucket, now),
            "fee_rate_at_sample_end": fee_rate_at(bucket, sample_end),
            "fee_free_in_sample": fee_rate_at(bucket, sample_end) == 0.0,
            "fee_start_date": starts[0][0] if starts else None,
            "clob_fee_rate_live": json.dumps(live) if live is not None else None,
            # NOTE: base_fee scale vs the docs' feeRate is unverified (closed
            # markets return their final fee state); treat as raw evidence.
            "clob_base_fee": (live or {}).get("base_fee"),
        })

    df = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    print(f"Saved {len(df)} rows -> {args.output}")

    print("\nBucket counts:")
    print(df["category_bucket"].value_counts().to_string())
    print("\nFee-free during sample:", int(df["fee_free_in_sample"].sum()), "/", len(df))
    n_live = df["clob_fee_rate_live"].notna().sum()
    print(f"Live CLOB fee-rate responses: {n_live}/{len(df)}")
    unknown = df[df["category_bucket"] == "unknown"]
    if len(unknown):
        print("\nUNKNOWN bucket (classify manually):")
        print(unknown[["market_id", "question"]].to_string(index=False))


if __name__ == "__main__":
    main()
