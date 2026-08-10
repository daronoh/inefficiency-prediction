"""Stage 1a: per-token orderbook feature tables.

Reads each token's raw book snapshots (data/raw_orderbook/ob_*.parquet) and
writes a model-ready feature table (data/processed/orderbook/feat_*.parquet):
top-of-book prices/sizes, spread, depth, imbalance, data-quality flags,
leakage-safe metadata, and one-lag deltas. See src/features.py.

Usage:
  python data-pipeline/run_feature_pipeline.py
  python data-pipeline/run_feature_pipeline.py --token-ids-path data/processed/universe_token_ids_negrisk.parquet
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


def _resolve_project_root() -> Path:
    cwd = Path.cwd().resolve()
    return cwd.parent if cwd.name == "data-pipeline" else cwd


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.features import build_token_feature_table_from_parquet
from src.paths import (
    TOKENS_PARQUET,
    ORDERBOOK_DIR,
    FEATURE_DIR,
    UNIVERSE_TOKEN_IDS,
    ensure_dirs,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Build per-token orderbook feature parquet files.")
    parser.add_argument(
        "--token-ids-path", type=Path, default=UNIVERSE_TOKEN_IDS,
        help="Token_id parquet to process (default: full universe list).",
    )
    parser.add_argument("--depth-n", type=int, default=3)
    parser.add_argument("--keep-json-cols", action="store_true",
                        help="Retain raw bids/asks JSON columns in the output.")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dirs()

    relevant = set(pd.read_parquet(args.token_ids_path)["token_id"].astype(str))
    tokens_df = pd.read_parquet(TOKENS_PARQUET)
    tokens_df = tokens_df[tokens_df["token_id"].astype(str).isin(relevant)]
    print(f"Token ids: {len(relevant)} | metadata rows: {len(tokens_df):,}")

    files = [p for p in sorted(ORDERBOOK_DIR.glob("ob_*.parquet"))
             if p.stem.replace("ob_", "") in relevant]
    if not files:
        print(f"No matching ob_*.parquet in {ORDERBOOK_DIR} — download orderbooks first.")
        return
    print(f"Files to process: {len(files)}")

    success = failed = 0
    for i, file_path in enumerate(files, start=1):
        output_path = FEATURE_DIR / f"feat_{file_path.stem.replace('ob_', '')}.parquet"
        print(f"[{i}/{len(files)}] {file_path.name}")
        try:
            feat_df = build_token_feature_table_from_parquet(
                input_path=file_path,
                output_path=output_path,
                depth_n=args.depth_n,
                drop_json_cols=not args.keep_json_cols,
                token_meta_df=tokens_df,
            )
            print(f"Saved {len(feat_df):,} rows -> {output_path.name}")
            success += 1
        except Exception as e:
            print(f"Failed on {file_path.name}: {e}")
            failed += 1

    print(f"Done. Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    main()
