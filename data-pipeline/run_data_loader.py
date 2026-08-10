"""Stage 0: build metadata tables and download orderbook snapshots.

Two jobs, both driven by the frozen markets JSONL and the universe token lists:

1. Ensure `markets.parquet` / `tokens.parquet` exist (parsed from the JSONL).
   They are rebuilt automatically when missing, or on --rebuild-tables.
2. Download per-token orderbook history from the Dome API for a token list
   (default: the full universe list; pass a per-stratum list to stage the
   download). Requires the DOME_API_KEY environment variable.

Typical usage:
  python data-pipeline/run_data_loader.py --token-ids-path data/processed/universe_token_ids_negrisk.parquet --skip-existing
  python data-pipeline/run_data_loader.py --skip-download          # tables only
"""

import argparse
import asyncio
import sys
from pathlib import Path

import pandas as pd


def _resolve_project_root() -> Path:
    cwd = Path.cwd().resolve()
    return cwd.parent if cwd.name == "data-pipeline" else cwd


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data_loader import read_market_to_df, read_token_to_df, extract_orderbook_from_ids_async
from src.paths import (
    MARKET_JSONL,
    MARKETS_PARQUET,
    TOKENS_PARQUET,
    ORDERBOOK_DIR,
    UNIVERSE_TOKEN_IDS,
    ensure_dirs,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build market/token tables and download orderbook snapshots."
    )
    parser.add_argument(
        "--token-ids-path", type=Path, default=UNIVERSE_TOKEN_IDS,
        help="Token_id parquet to download (default: full universe list; "
             "pass a universe_token_ids_<stratum>.parquet to stage).",
    )
    parser.add_argument("--start-date", type=str, default="2025-10-14",
                        help="Snapshot window start (Dome history floor).")
    parser.add_argument("--end-date", type=str, default="2026-03-31",
                        help="Snapshot window end (every universe market resolves before this).")
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip tokens already on disk (resumable downloads).")
    parser.add_argument("--skip-download", action="store_true",
                        help="Only ensure the metadata tables exist.")
    parser.add_argument("--rebuild-tables", action="store_true",
                        help="Re-parse markets/tokens tables even if present.")
    return parser.parse_args()


def ensure_tables(rebuild: bool) -> None:
    if not rebuild and MARKETS_PARQUET.exists() and TOKENS_PARQUET.exists():
        return
    print(f"Parsing {MARKET_JSONL} ...")
    markets_df = read_market_to_df(MARKET_JSONL)
    markets_df.to_parquet(MARKETS_PARQUET, index=False)
    print(f"Saved {len(markets_df):,} markets -> {MARKETS_PARQUET}")
    tokens_df = read_token_to_df(MARKET_JSONL)
    tokens_df.to_parquet(TOKENS_PARQUET, index=False)
    print(f"Saved {len(tokens_df):,} tokens -> {TOKENS_PARQUET}")


def main():
    args = parse_args()
    ensure_dirs()

    ensure_tables(rebuild=args.rebuild_tables)
    if args.skip_download:
        print("Skipping orderbook download (--skip-download).")
        return

    token_ids = sorted(set(pd.read_parquet(args.token_ids_path)["token_id"].astype(str)))
    print(f"Loaded {len(token_ids)} token ids from {args.token_ids_path}")

    if args.skip_existing:
        existing = {p.stem.replace("ob_", "") for p in ORDERBOOK_DIR.glob("ob_*.parquet")}
        kept = [t for t in token_ids if t not in existing]
        print(f"--skip-existing: {len(token_ids) - len(kept)} already on disk, {len(kept)} to download")
        token_ids = kept

    if not token_ids:
        print("Nothing to download.")
        return

    asyncio.run(
        extract_orderbook_from_ids_async(
            token_ids=token_ids,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=ORDERBOOK_DIR,
            max_concurrent=args.max_concurrent,
        )
    )


if __name__ == "__main__":
    main()
