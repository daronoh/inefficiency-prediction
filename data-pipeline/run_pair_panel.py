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

from src.pairs import build_pair_panels
from src.paths import (
    PROCESSED_DIR,
    FEATURE_DIR,
    PAIR_DIR,
    FILTERED_TOKEN_IDS,
    ensure_dirs,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build market-level YES/NO pair panels from per-token feature files."
    )
    parser.add_argument("--tokens-path", type=Path, default=PROCESSED_DIR / "tokens.parquet")
    parser.add_argument("--filtered-tokens-path", type=Path, default=FILTERED_TOKEN_IDS)
    parser.add_argument("--feat-orderbook-dir", type=Path, default=FEATURE_DIR)
    parser.add_argument("--pair-dir", type=Path, default=PAIR_DIR)
    parser.add_argument("--grid-interval", type=str, default="1m")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dirs()
    event_dir = args.pair_dir / "event"
    grid_dir = args.pair_dir / f"grid_{args.grid_interval}"
    event_dir.mkdir(parents=True, exist_ok=True)
    grid_dir.mkdir(parents=True, exist_ok=True)

    # Remove previous outputs so a market that fails/skips on this run cannot
    # silently keep a stale panel from an earlier run.
    stale = list(event_dir.glob("pair_*.parquet")) + list(grid_dir.glob("pair_*.parquet"))
    for p in stale:
        p.unlink()
    if stale:
        print(f"Cleared {len(stale)} existing panel files")

    relevant_token_ids = set(
        pd.read_parquet(args.filtered_tokens_path)["token_id"].astype(str)
    )
    tokens_df = pd.read_parquet(args.tokens_path)
    tokens_df = tokens_df[tokens_df["token_id"].astype(str).isin(relevant_token_ids)]

    # market_id -> {outcome_index: token_id}
    pairs = (
        tokens_df.assign(token_id=tokens_df["token_id"].astype(str))
        .groupby("market_id")
        .apply(lambda g: dict(zip(g["outcome_index"], g["token_id"])), include_groups=False)
        .to_dict()
    )
    print(f"Markets to pair: {len(pairs)}")

    success = 0
    skipped = 0
    for i, (market_id, sides) in enumerate(sorted(pairs.items()), start=1):
        if 0 not in sides or 1 not in sides:
            print(f"[{i}/{len(pairs)}] market {market_id}: missing an outcome token, skipping")
            skipped += 1
            continue
        p0 = args.feat_orderbook_dir / f"feat_{sides[0]}.parquet"
        p1 = args.feat_orderbook_dir / f"feat_{sides[1]}.parquet"
        if not p0.exists() or not p1.exists():
            print(f"[{i}/{len(pairs)}] market {market_id}: feature file missing, skipping")
            skipped += 1
            continue
        try:
            event_panel, grid_panel = build_pair_panels(
                p0, p1, str(market_id), grid_interval=args.grid_interval
            )
        except Exception as e:
            print(f"[{i}/{len(pairs)}] market {market_id}: FAILED ({e})")
            skipped += 1
            continue

        event_panel.write_parquet(event_dir / f"pair_{market_id}.parquet")
        n_grid = 0
        if grid_panel is not None:
            grid_panel.write_parquet(grid_dir / f"pair_{market_id}.parquet")
            n_grid = len(grid_panel)
        print(
            f"[{i}/{len(pairs)}] market {market_id}: "
            f"{len(event_panel):,} event rows, {n_grid:,} grid rows"
        )
        success += 1

    print(f"Done. Success: {success}, Skipped/Failed: {skipped}")
    print(f"Event panels: {event_dir}")
    print(f"Grid panels:  {grid_dir}")


if __name__ == "__main__":
    main()
