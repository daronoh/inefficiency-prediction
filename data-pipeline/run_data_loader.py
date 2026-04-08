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
    PROCESSED_DIR,
    ORDERBOOK_DIR,
    FILTERED_TOKEN_IDS,
    ensure_dirs,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Build market/token tables and download orderbook snapshots.")
    parser.add_argument("--market-jsonl", type=Path, default=MARKET_JSONL)
    parser.add_argument("--markets-path", type=Path, default=PROCESSED_DIR / "markets.parquet")
    parser.add_argument("--tokens-path", type=Path, default=PROCESSED_DIR / "tokens.parquet")
    parser.add_argument("--filtered-tokens-path", type=Path, default=FILTERED_TOKEN_IDS)
    parser.add_argument("--raw-orderbook-dir", type=Path, default=ORDERBOOK_DIR)

    parser.add_argument("--min-end-date", type=str, default="2026-01-01")
    parser.add_argument("--top-markets", type=int, default=70)

    parser.add_argument("--start-date", type=str, default="2025-10-14")
    parser.add_argument("--end-date", type=str, default="2026-03-31")
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--skip-download", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dirs()
    args.raw_orderbook_dir.mkdir(parents=True, exist_ok=True)

    print(f"Using project root: {PROJECT_ROOT}")

    # 1) Markets table
    markets_df = read_market_to_df(args.market_jsonl)
    print("markets_df shape:", markets_df.shape)
    markets_df.to_parquet(args.markets_path, index=False)
    print(f"Saved: {args.markets_path}")

    # 2) Select relevant markets
    working = markets_df.dropna(subset=["question", "market_id", "end_date", "closed_time"]).copy()
    working["end_date_ts"] = pd.to_datetime(working["end_date"], errors="coerce", utc=True)
    min_end_ts = pd.Timestamp(args.min_end_date, tz="UTC")

    markets_filtered_df = (
        working[working["end_date_ts"] > min_end_ts]
        .sort_values("volume", ascending=False)
        .head(args.top_markets)
    )
    relevant_market_ids = set(markets_filtered_df["market_id"].astype(str))
    print(f"Selected markets: {len(relevant_market_ids)}")

    # 3) Tokens table
    tokens_df = read_token_to_df(args.market_jsonl)
    print("tokens_df shape:", tokens_df.shape)
    tokens_df.to_parquet(args.tokens_path, index=False)
    print(f"Saved: {args.tokens_path}")

    # 4) Filter token IDs by selected markets
    tokens_filtered_df = tokens_df[tokens_df["market_id"].astype(str).isin(relevant_market_ids)].copy()
    relevant_token_ids = sorted(set(tokens_filtered_df["token_id"].astype(str)))

    pd.DataFrame({"token_id": relevant_token_ids}).to_parquet(args.filtered_tokens_path, index=False)
    print(f"Saved token ID list ({len(relevant_token_ids)}): {args.filtered_tokens_path}")

    # 5) Download orderbooks
    if args.skip_download:
        print("Skipping orderbook download (--skip-download).")
        return

    asyncio.run(
        extract_orderbook_from_ids_async(
            token_ids=relevant_token_ids,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=args.raw_orderbook_dir,
            max_concurrent=args.max_concurrent,
        )
    )


if __name__ == "__main__":
    main()