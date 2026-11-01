"""Portable paths; FYP_ROOT optionally selects a separate run workspace."""
import os
from pathlib import Path
PROJECT_ROOT=Path(os.environ.get('FYP_ROOT',Path(__file__).resolve().parents[1])).resolve()
DATA_DIR=PROJECT_ROOT/'data'
PROCESSED_DIR=DATA_DIR/'processed'
EVENT_UNIVERSE=PROJECT_ROOT/'inputs/events.parquet'
MARKETS_PARQUET=PROJECT_ROOT/'inputs/markets.parquet'
