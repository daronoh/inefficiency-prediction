"""Repository paths; DATA_ROOT may point to an external copy of the dataset."""
import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / '.env', override=False)
DATA_DIR = Path(os.environ.get('DATA_ROOT', str(PROJECT_ROOT / 'data'))).resolve()
RAW_DIR = DATA_DIR / 'raw'
ORDERBOOK_DIR = RAW_DIR / 'orderbooks'
LADDER_BOOK_DIR = RAW_DIR / 'ladder_orderbooks'
METADATA_DIR = DATA_DIR / 'metadata'
PROCESSED_DIR = DATA_DIR / 'processed'
MARKET_JSONL = RAW_DIR / 'polymarket_markets_1y.jsonl'
MARKETS_PARQUET = METADATA_DIR / 'markets.parquet'
TOKENS_PARQUET = METADATA_DIR / 'tokens.parquet'
EVENT_UNIVERSE = METADATA_DIR / 'event_universe.parquet'
FEATURES_DIR = PROCESSED_DIR / 'features'
FEATURES = FEATURES_DIR / 'hourly_features.parquet'
LABELS_DIR = PROCESSED_DIR / 'labels'
RESULTS_DIR = PROJECT_ROOT / 'results' / 'baseline'
REPORTS_DIR = PROJECT_ROOT / 'docs' / 'reports'


def book_path(token):
    token = str(token)
    if not token.isdecimal():
        raise ValueError('Token IDs must be numeric')
    for directory in (LADDER_BOOK_DIR, ORDERBOOK_DIR):
        path = directory / f'ob_{token}.parquet'
        if path.exists():
            return path
    return LADDER_BOOK_DIR / f'ob_{token}.parquet'


def data_relative(path):
    return Path(path).resolve().relative_to(DATA_DIR).as_posix()


def resolve_data_path(path):
    result = (DATA_DIR / path).resolve()
    if not result.is_relative_to(DATA_DIR):
        raise ValueError('Input path is outside DATA_ROOT')
    return result
