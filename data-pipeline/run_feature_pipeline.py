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
    PROCESSED_DIR,
    ORDERBOOK_DIR,
    FILTERED_TOKEN_IDS,
    ensure_dirs,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Build per-token orderbook feature parquet files.")
    parser.add_argument("--tokens-path", type=Path, default=PROCESSED_DIR / "tokens.parquet")
    parser.add_argument("--filtered-tokens-path", type=Path, default=FILTERED_TOKEN_IDS)
    parser.add_argument("--raw-orderbook-dir", type=Path, default=ORDERBOOK_DIR)
    parser.add_argument("--feat-orderbook-dir", type=Path, default=PROCESSED_DIR / "orderbook")
    parser.add_argument("--depth-n", type=int, default=3)
    parser.add_argument("--drop-json-cols", action="store_true", default=True)
    parser.add_argument("--process-all-files", action="store_true", help="Ignore filtered token list and process all ob_*.parquet files.")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dirs()
    args.feat_orderbook_dir.mkdir(parents=True, exist_ok=True)

    print(f"Using project root: {PROJECT_ROOT}")

    # Read filtered token IDs
    relevant_token_ids = set(pd.read_parquet(args.filtered_tokens_path)["token_id"].astype(str))
    print(f"Loaded filtered token IDs: {len(relevant_token_ids)}")

    # Read token metadata
    tokens_df = pd.read_parquet(args.tokens_path)
    tokens_df = tokens_df[tokens_df["token_id"].astype(str).isin(relevant_token_ids)]
    print(f"Token metadata rows after filter: {len(tokens_df):,}")

    files = sorted(args.raw_orderbook_dir.glob("ob_*.parquet"))
    if not files:
        print(f"No files found in {args.raw_orderbook_dir} matching ob_*.parquet")
        return

    if not args.process_all_files:
        files = [p for p in files if p.stem.replace("ob_", "") in relevant_token_ids]

    print(f"Files to process: {len(files)}")

    success = 0
    failed = 0

    for i, file_path in enumerate(files, start=1):
        token_stub = file_path.stem.replace("ob_", "")
        output_path = args.feat_orderbook_dir / f"feat_{token_stub}.parquet"

        print(f"[{i}/{len(files)}] Processing {file_path.name}")
        try:
            feat_df = build_token_feature_table_from_parquet(
                input_path=file_path,
                output_path=output_path,
                depth_n=args.depth_n,
                drop_json_cols=args.drop_json_cols,
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