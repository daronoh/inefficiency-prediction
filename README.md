# Inefficiency Prediction (Polymarket Orderbook Pipeline)

## Overview
This repository serves as a framework for working with orderbook data on polymarket, specifically with the intention of predicting inefficiency in markets.

Pipeline flow:

1. Read market JSONL and create market/token tables.
2. Download per-token orderbook snapshots to parquet.
3. Compute snapshot + time-series features per token.

Core implementation lives in [src/data_loader.py](src/data_loader.py) and [src/features.py](src/features.py).  
Execution/orchestration is in [data-pipeline/data_loader.ipynb](data-pipeline/data_loader.ipynb) and [data-pipeline/feature.ipynb](data-pipeline/feature.ipynb).

---

## Prerequisites

- Python **3.12+** (notebook metadata shows 3.12.6)
- Install dependencies from [requirements.txt](requirements.txt):
  - `numpy`, `pandas`, `requests`, `aiohttp`, `polars`, `ipykernel`, `pyarrow`
- Input JSONL files in `data/`:
  - `data/polymarket_markets_1y.jsonl` (required)
- Jupyter environment (VS Code notebook support is fine)

Install:

```bash
pip install -r requirements.txt
```

---

### `data-pipeline/data_loader.ipynb`
**Reads**
- `data/polymarket_markets_1y.jsonl`
- (optionally) existing parquet files if rerunning cells

**Writes**
- `data/processed/markets.parquet`
- `data/processed/tokens.parquet`
- `data/orderbook/ob_<token_id>.parquet` (one orderbook snapshot table per token)
- `data/processed/70_markets_token_ids.parquet` (**user-filtered token list**; name/content depends on the user)

---

### `data-pipeline/feature.ipynb`
**Reads**
- `data/orderbook/ob_<token_id>.parquet`
- `data/processed/70_markets_token_ids.parquet` (same file that refers to the **user-filtered token list**)
- (optionally) `data/processed/tokens.parquet` for metadata joins

**Writes**
- `data/processed/orderbook/feat_<token_id>.parquet`


---

### Directory-level summary

**Required starting input**
- `data/polymarket_markets_1y.jsonl`

**Pipeline-generated intermediates**
- `data/processed/markets.parquet`
- `data/processed/tokens.parquet`
- `data/orderbook/ob_<token_id>.parquet`
- `data/processed/70_markets_token_ids.parquet` (or equivalent user-defined filtered token-id file)

**Final outputs**
- `data/processed/orderbook/feat_<token_id>.parquet`

---

## How to Run

## 1) Build base data tables + orderbook snapshots
Open and run [data-pipeline/data_loader.ipynb](data-pipeline/data_loader.ipynb) top-to-bottom.

This notebook:
- Reads markets with [`src.data_loader.read_market_to_df`](src/data_loader.py)
- Reads token rows with [`src.data_loader.read_token_to_df`](src/data_loader.py)
- Pulls orderbook snapshots and saves per-token parquet files (under `data/orderbook/`)

## 2) Build token feature parquet files
Open and run [data-pipeline/feature.ipynb](data-pipeline/feature.ipynb) top-to-bottom.

This notebook:
- Loads relevant token IDs
- Iterates over `data/orderbook/ob_*.parquet`
- Calls [`src.features.build_token_feature_table_from_parquet`](src/features.py)
- Writes `feat_<token_id>.parquet` under `data/processed/orderbook/`

---

## Notes

- The `data/` directory is git-ignored in [.gitignore](.gitignore), so local parquet/jsonl files are not committed.
- If you rerun notebooks, existing parquet files may be overwritten.
- extracting orderbooks from ids calls an external API directly, which may end up taking a significant amount of time, subjective to the amount of markets we are querying.