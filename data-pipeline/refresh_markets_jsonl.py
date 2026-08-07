"""Refresh the Polymarket markets metadata dump from the public Gamma API.

The original `polymarket_markets_1y.jsonl` is a frozen snapshot (~Feb 2026);
anything created or updated after it — including the fee rollout's per-market
`feesEnabled` flips — is invisible to the universe selector. This script
re-scrapes every market whose end date falls on/after the Dome history start
via the keyset (cursor) endpoint and writes a fresh JSONL in the exact shape
of the original dump (one raw Gamma market object per line), so all existing
parsers work unchanged.

No API key required — Gamma is public. Safe to re-run; writes to a temp file
and renames into place only on success.

Usage:
  python data-pipeline/refresh_markets_jsonl.py
  python data-pipeline/refresh_markets_jsonl.py --out data/polymarket_markets_fresh.jsonl
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests


def _resolve_project_root() -> Path:
    cwd = Path.cwd().resolve()
    return cwd.parent if cwd.name == "data-pipeline" else cwd


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.paths import DATA_DIR

KEYSET_URL = "https://gamma-api.polymarket.com/markets/keyset"
PAGE_LIMIT = 100  # server-side cap
RETRYABLE = {429, 500, 502, 503, 504}


def parse_args():
    p = argparse.ArgumentParser(description="Re-scrape Gamma market metadata to JSONL.")
    p.add_argument("--end-date-min", type=str, default="2025-10-14T00:00:00Z",
                   help="Only markets ending on/after this (Dome history start).")
    p.add_argument("--out", type=Path, default=DATA_DIR / "polymarket_markets_fresh.jsonl")
    p.add_argument("--sleep", type=float, default=0.2, help="Delay between pages (s).")
    p.add_argument("--max-retries", type=int, default=6)
    p.add_argument("--max-pages", type=int, default=None,
                   help="Stop after N pages (testing only).")
    return p.parse_args()


def fetch_page(session, params, max_retries):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(KEYSET_URL, params=params, timeout=30)
            if resp.status_code in RETRYABLE:
                raise requests.HTTPError(f"retryable status={resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == max_retries:
                raise
            backoff = min(60, 2 ** attempt)
            print(f"  retry {attempt}/{max_retries} in {backoff}s ({e})")
            time.sleep(backoff)


def main():
    args = parse_args()
    tmp = args.out.with_suffix(".jsonl.partial")
    args.out.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    params = {"limit": PAGE_LIMIT, "end_date_min": args.end_date_min}
    n_rows = 0
    n_pages = 0
    seen_ids = set()
    t0 = time.time()

    with open(tmp, "w", encoding="utf-8") as f:
        while True:
            payload = fetch_page(session, params, args.max_retries)
            markets = payload.get("markets") or []
            if not markets:
                break
            for mk in markets:
                mid = str(mk.get("id"))
                if mid in seen_ids:
                    continue
                seen_ids.add(mid)
                f.write(json.dumps(mk, separators=(",", ":")) + "\n")
                n_rows += 1
            n_pages += 1
            if n_pages % 100 == 0:
                rate = n_rows / max(time.time() - t0, 1)
                print(f"  {n_pages:,} pages, {n_rows:,} markets ({rate:,.0f}/s)")
            cursor = payload.get("next_cursor")
            if not cursor:
                break
            params["next_cursor"] = cursor
            if args.max_pages and n_pages >= args.max_pages:
                print(f"Stopping at --max-pages={args.max_pages}")
                break
            time.sleep(args.sleep)

    if n_rows == 0:
        tmp.unlink(missing_ok=True)
        raise SystemExit("Scrape returned no markets; leaving existing files untouched.")

    tmp.replace(args.out)
    mb = args.out.stat().st_size / 1e6
    print(f"Done: {n_rows:,} markets, {n_pages:,} pages, {mb:,.0f} MB -> {args.out}")
    print(f"Elapsed: {(time.time() - t0)/60:.1f} min")


if __name__ == "__main__":
    main()
