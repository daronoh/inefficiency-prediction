"""Stage 1c: negRisk group panels — N sibling markets aligned per event.

For every negrisk event in the universe, aligns all member markets' YES books
on shared timelines and computes the group arbitrage signals (long_gap /
short_gap). Writes, per event, an event-grid and a wall-clock-grid panel, plus
one members table mapping (event, member index) -> market/token/question with
data-availability flags. See src/groups.py for semantics.

Usage:
  python data-pipeline/run_group_panel.py
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

from src.groups import build_group_panels
from src.paths import (
    TOKENS_PARQUET,
    EVENT_UNIVERSE,
    FEATURE_DIR,
    PROCESSED_DIR,
    ensure_dirs,
)

GROUP_DIR = PROCESSED_DIR / "groups"


def parse_args():
    parser = argparse.ArgumentParser(description="Build negRisk group panels.")
    parser.add_argument("--stratum", type=str, default="negrisk",
                        help="Universe stratum to build group panels for.")
    parser.add_argument("--grid-interval", type=str, default="1m")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dirs()
    event_dir = GROUP_DIR / "event"
    grid_dir = GROUP_DIR / f"grid_{args.grid_interval}"
    event_dir.mkdir(parents=True, exist_ok=True)
    grid_dir.mkdir(parents=True, exist_ok=True)

    stale = list(event_dir.glob("group_*.parquet")) + list(grid_dir.glob("group_*.parquet"))
    for p in stale:
        p.unlink()
    if stale:
        print(f"Cleared {len(stale)} existing panel files")

    universe = pd.read_parquet(EVENT_UNIVERSE)
    universe = universe[universe["stratum"] == args.stratum].copy()
    universe["market_id"] = universe["market_id"].astype(str)

    tokens = pd.read_parquet(TOKENS_PARQUET)
    tokens["market_id"] = tokens["market_id"].astype(str)
    tokens["token_id"] = tokens["token_id"].astype(str)
    yes_tokens = (
        tokens[tokens["outcome_index"] == 0]
        .set_index("market_id")["token_id"]
        .to_dict()
    )

    member_rows = []
    n_events = universe["event_slug"].nunique()
    success = skipped = 0
    for i, (slug, grp) in enumerate(sorted(universe.groupby("event_slug")), start=1):
        grp = grp.sort_values("market_id").reset_index(drop=True)
        feat_paths = {}
        for idx, row in grp.iterrows():
            token = yes_tokens.get(row["market_id"])
            feat = FEATURE_DIR / f"feat_{token}.parquet" if token else None
            has_data = feat is not None and feat.exists()
            if has_data:
                feat_paths[idx] = feat
            member_rows.append({
                "event_slug": slug,
                "member_idx": idx,
                "market_id": row["market_id"],
                "question": row["question"],
                "yes_token_id": token,
                "has_data": has_data,
                "supports_full_cover": bool(row.get("supports_full_cover", False)),
            })
        n_total = len(grp)
        if not feat_paths:
            print(f"[{i}/{n_events}] {slug}: no member data at all, skipping")
            skipped += 1
            continue
        full_cover_ok = bool(grp["supports_full_cover"].max()) and len(feat_paths) == n_total
        try:
            event_panel, grid_panel = build_group_panels(
                feat_paths, n_total, slug, grid_interval=args.grid_interval,
                full_cover_ok=full_cover_ok,
            )
        except Exception as e:
            print(f"[{i}/{n_events}] {slug}: FAILED ({e})")
            skipped += 1
            continue

        event_panel.write_parquet(event_dir / f"group_{slug}.parquet")
        n_grid = 0
        if grid_panel is not None:
            grid_panel.write_parquet(grid_dir / f"group_{slug}.parquet")
            n_grid = len(grid_panel)
        missing = n_total - len(feat_paths)
        note = f" ({missing} member(s) without data -> long_gap disabled)" if missing else ""
        print(f"[{i}/{n_events}] {slug}: {n_total} members, "
              f"{len(event_panel):,} event rows, {n_grid:,} grid rows{note}")
        success += 1

    members = pd.DataFrame(member_rows)
    members_path = GROUP_DIR / "members.parquet"
    members.to_parquet(members_path, index=False)
    print(f"\nDone. Success: {success}, Skipped/Failed: {skipped}")
    print(f"Members table: {members_path} ({len(members)} rows, "
          f"{int((~members['has_data']).sum())} without data)")


if __name__ == "__main__":
    main()
