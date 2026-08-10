import os
from pathlib import Path

# Project root = inefficiency-prediction/
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# data/ is git-ignored and may live outside this checkout (e.g. when running from a
# git worktree). Override with the DATA_ROOT environment variable in that case.
DATA_DIR = Path(os.environ.get("DATA_ROOT", str(PROJECT_ROOT / "data")))
PROCESSED_DIR = DATA_DIR / "processed"
ORDERBOOK_DIR = DATA_DIR / "raw_orderbook"
FEATURE_DIR = PROCESSED_DIR / "orderbook"
PAIR_DIR = PROCESSED_DIR / "pairs"

MARKET_JSONL = DATA_DIR / "polymarket_markets_1y.jsonl"

MARKETS_PARQUET = PROCESSED_DIR / "markets.parquet"
TOKENS_PARQUET = PROCESSED_DIR / "tokens.parquet"

# Canonical study universe (written by select_event_universe.py).
EVENT_UNIVERSE = PROCESSED_DIR / "event_universe.parquet"
UNIVERSE_TOKEN_IDS = PROCESSED_DIR / "universe_token_ids.parquet"

FEE_SCHEDULE = PROCESSED_DIR / "fee_schedule.parquet"

def ensure_dirs() -> None:
    for p in [DATA_DIR, PROCESSED_DIR, ORDERBOOK_DIR, FEATURE_DIR, PAIR_DIR]:
        p.mkdir(parents=True, exist_ok=True)
