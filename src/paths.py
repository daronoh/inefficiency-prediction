from pathlib import Path

# Project root = inefficiency-prediction/
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
ORDERBOOK_DIR = DATA_DIR / "orderbook"
FEATURE_DIR = PROCESSED_DIR / "orderbook"

MARKET_JSONL = DATA_DIR / "polymarket_markets_1y.jsonl"

# User-defined token filter output/input shared by both notebooks
FILTERED_TOKEN_IDS = PROCESSED_DIR / "70_markets_token_ids.parquet"

def ensure_dirs() -> None:
    for p in [DATA_DIR, PROCESSED_DIR, ORDERBOOK_DIR, FEATURE_DIR]:
        p.mkdir(parents=True, exist_ok=True)