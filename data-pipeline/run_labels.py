"""Stage 2 runner: spells, grid labels, and the attrition table.

Reads the group panels, writes:
  data/processed/labels/spells.parquet       one row per opportunity spell
  data/processed/labels/labels_grid.parquet  binary + continuous targets on the 1m grid
  data/processed/labels/attrition_dev.parquet  layered survival (development only)

Prints the development-sample attrition table. Lockbox rows are written (they
are needed for the final evaluation) but never summarized here.

Usage:
  python data-pipeline/run_labels.py
"""

import sys
from pathlib import Path

import pandas as pd
import polars as pl


def _resolve_project_root() -> Path:
    cwd = Path.cwd().resolve()
    return cwd.parent if cwd.name == "data-pipeline" else cwd


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.labels import GROUP_COLS, spells_for_event, grid_labels_for_event, attrition_table
from src.paths import PROCESSED_DIR, ensure_dirs

GROUP_DIR = PROCESSED_DIR / "groups"
LABEL_DIR = PROCESSED_DIR / "labels"


def main():
    ensure_dirs()
    LABEL_DIR.mkdir(parents=True, exist_ok=True)

    event_files = sorted((GROUP_DIR / "event").glob("group_*.parquet"))
    if not event_files:
        raise SystemExit("No group panels found — run run_group_panel.py first.")

    all_spells = []
    all_labels = []
    for i, f in enumerate(event_files, start=1):
        slug = f.stem.replace("group_", "")
        panel = pl.read_parquet(f, columns=GROUP_COLS)
        spells = spells_for_event(panel, slug)
        gf = GROUP_DIR / "grid_1m" / f.name
        if gf.exists():
            grid_times = pl.read_parquet(gf, columns=["time"]).to_pandas()["time"]
            labels = grid_labels_for_event(grid_times, spells, slug)
            all_labels.append(labels)
        if len(spells):
            all_spells.append(spells)
        print(f"[{i}/{len(event_files)}] {slug}: {len(spells)} spell rows")

    spells = pd.concat(all_spells, ignore_index=True) if all_spells else pd.DataFrame()
    labels = pd.concat(all_labels, ignore_index=True) if all_labels else pd.DataFrame()
    spells.to_parquet(LABEL_DIR / "spells.parquet", index=False)
    labels.to_parquet(LABEL_DIR / "labels_grid.parquet", index=False)

    att = attrition_table(spells, dev_only=True)
    att.to_parquet(LABEL_DIR / "attrition_dev.parquet", index=False)

    print(f"\nSpells: {len(spells):,} rows -> {LABEL_DIR / 'spells.parquet'}")
    print(f"Labels: {len(labels):,} rows -> {LABEL_DIR / 'labels_grid.parquet'}")
    dev = labels[labels["split"] == "development"] if len(labels) else labels
    if len(dev):
        rates = {c: round(dev[c].mean(), 5) for c in dev.columns if c.startswith("y_")}
        print(f"Development label base rates ({len(dev):,} grid rows): {rates}")
    print("\nATTRITION TABLE (development sample only):")
    print(att.to_string(index=False))


if __name__ == "__main__":
    main()
