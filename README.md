# Inefficiency Prediction (Polymarket Orderbook Pipeline)

## Overview
This repository serves as a framework for working with orderbook data on polymarket, specifically with the intention of predicting inefficiency in markets.

Pipeline flow:

1. Read market JSONL and create market/token tables.
2. Download per-token orderbook snapshots to parquet.
3. Compute snapshot + time-series features per token.
4. Build market-level YES/NO pair panels (as-of-aligned complementary books).
5. Build the per-market fee schedule (category buckets + dated fee regime + live CLOB rates).

Core implementation lives in [src/data_loader.py](src/data_loader.py), [src/features.py](src/features.py) and [src/pairs.py](src/pairs.py).  
Execution/orchestration is in the `data-pipeline/` runner scripts (the notebooks mirror steps 1–3 but the scripts are canonical).

### Environment variables

- `DOME_API_KEY` (**required for orderbook download**): Dome API key. The key is no longer
  committed; the previously committed key must be treated as leaked and rotated.
- `DATA_ROOT` (optional): absolute path of the `data/` directory if it lives outside this
  checkout (e.g. when running from a git worktree). Defaults to `<repo>/data`.

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
- `data/raw_orderbook/ob_<token_id>.parquet` (one orderbook snapshot table per token)
- `data/processed/70_markets_token_ids.parquet` (**user-filtered token list**; name/content depends on the user)

---

### `data-pipeline/feature.ipynb`
**Reads**
- `data/raw_orderbook/ob_<token_id>.parquet`
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
- `data/raw_orderbook/ob_<token_id>.parquet`
- `data/processed/70_markets_token_ids.parquet` (or equivalent user-defined filtered token-id file)

**Final outputs**
- `data/processed/orderbook/feat_<token_id>.parquet` (per-token features)
- `data/processed/pairs/event/pair_<market_id>.parquet` (event-grid YES/NO pair panel)
- `data/processed/pairs/grid_1m/pair_<market_id>.parquet` (1-minute-grid pair panel)
- `data/processed/fee_schedule.parquet` (per-market fee categories/rates)

### Research-validity notes (changed 2026-08)

- `time_to_expiry_hours` now uses the **scheduled** `end_date` only. The realised
  `closed_time` is not knowable at snapshot time (lookahead) and is no longer used.
- Lifetime `volume` / `volume_clob` are no longer attached as features (they are totals
  as of the metadata dump, i.e. lookahead). Market *selection* still uses them — an
  acknowledged ex-post selection limitation.
- Feature tables keep `indexed_at_time` / `indexed_at_ms` (Dome ingestion clock) as the
  honest "data available at" timestamps, carry data-quality flags
  (`flag_no_bids`, `flag_no_asks`, `flag_crossed_book`), and deduplicate snapshots on
  `(token_id, snapshot_timestamp_ms)` keeping the latest-indexed row.
- **Mirror-book caveat:** the YES and NO books of one binary market are complementary
  views of the same order pool (`ask_NO = 1 − bid_YES` holds exactly in ~99.999% of
  aligned snapshots). Intra-market `BUY_YES + BUY_NO < 1` therefore cannot occur except
  as capture-desync noise; inefficiency labels must be constructed **across** markets
  (negRisk event groups, logically related markets).
- **Time-to-expiry semantics:** 11.4% of snapshot rows fall *after* the scheduled
  `end_date` (markets trade until actual resolution), so `time_to_expiry_hours` is
  negative there and `log_time_to_expiry_hours` clips those rows to 0. Conversely some
  markets resolve months before `end_date` (long-dated futures), so the feature can
  greatly overstate remaining life. Both are inherent to using only ex-ante information.
- **Availability clock quirks:** ~0.12% of rows have `indexed_at_ms` earlier than
  `snapshot_timestamp_ms` (upstream clock skew), and indexing lag can reach ~33 min.
  Latency-sensitive evaluation should treat `indexed_at` as the data-available clock
  and be robust to these outliers.
- **negRisk columns:** the per-snapshot `orderbook_neg_risk` feed value flaps to False
  on a minority of snapshots for some negRisk markets; the metadata `neg_risk` column
  (from gamma) is authoritative.
- **Pair-panel validity flags:** `valid_pair_{2,5,10,30}s` mean both sides are *fresh*,
  not *tradeable* — additionally filter on `flag_no_bids` / `flag_no_asks` /
  `flag_crossed_book` per side before computing labels.
- **Fee status:** the per-market in-sample fee authority is gamma's `feesEnabled`
  (all 70 selected markets: **fee-free for their entire sampled life** — the 2026
  category fee rollout applied to serial 15-minute crypto markets first and reached
  other categories only after this sample ends). The regime table in
  `build_fee_schedule.py` provides category-level *counterfactual current* rates;
  `clob_base_fee` from the live endpoint is retroactive current config, not
  historical fee state.

---

## How to Run

## 1) Build base data tables + orderbook snapshots (script)
Run:

```bash
python .\data-pipeline\run_data_loader.py
```

What it does:
- Reads `data/polymarket_markets_1y.jsonl`
- Builds and writes:
  - `data/processed/markets.parquet`
  - `data/processed/tokens.parquet`
- Filters top markets (default: top 70 by volume, end date after `2026-01-01`)
- Writes filtered token IDs to:
  - `data/processed/70_markets_token_ids.parquet`
- Downloads orderbook snapshots per token to:
  - `data/raw_orderbook/ob_<token_id>.parquet`

Useful options:
```bash
python .\data-pipeline\run_data_loader.py --skip-download
python .\data-pipeline\run_data_loader.py --top-markets 100 --max-concurrent 12
python .\data-pipeline\run_data_loader.py --start-date 2025-10-14 --end-date 2026-03-31
```

## 2) Build token feature parquet files (script)
Run:

```bash
python .\data-pipeline\run_feature_pipeline.py
```

What it does:
- Reads:
  - `data/raw_orderbook/ob_<token_id>.parquet`
  - `data/processed/70_markets_token_ids.parquet`
  - `data/processed/tokens.parquet`
- Builds per-token feature files:
  - `data/processed/orderbook/feat_<token_id>.parquet`

Useful options:
```bash
python .\data-pipeline\run_feature_pipeline.py --depth-n 5
python .\data-pipeline\run_feature_pipeline.py --process-all-files
python .\data-pipeline\run_feature_pipeline.py --keep-json-cols
```

## 3) Build market-level YES/NO pair panels

```bash
python .\data-pipeline\run_pair_panel.py
```

What it does:
- As-of aligns each market's two outcome-token books (event grid = union of both sides'
  update times, plus a regular 1-minute grid), with per-side staleness columns and
  `valid_pair_{2,5,10,30}s` flags.
- Adds pair-level quote-consistency columns: `sum_best_asks`, `sum_best_bids`,
  `long_arb_gap` (= 1 − sum of asks), `short_arb_gap` (= sum of bids − 1).
- Writes `data/processed/pairs/event/pair_<market_id>.parquet` and
  `data/processed/pairs/grid_1m/pair_<market_id>.parquet`.

## 4) Build the fee schedule

```bash
python .\data-pipeline\build_fee_schedule.py
```

What it does:
- Scans the markets JSONL for the selected markets' event metadata, classifies each
  market into a fee category bucket, applies the dated 2025–26 fee-rollout table, and
  (best effort) queries `https://clob.polymarket.com/fee-rate` for live rates.
- Writes `data/processed/fee_schedule.parquet`.

## Optional: Notebook workflow
You can still run:
- `data-pipeline/data_loader.ipynb`
- `data-pipeline/feature.ipynb`

The scripts above are CLI equivalents of those notebooks.

---

## Notes

- The `data/` directory is git-ignored in [.gitignore](.gitignore), so local parquet/jsonl files are not committed.
- If you rerun notebooks, existing parquet files may be overwritten.
- extracting orderbooks from ids calls an external API directly, which may end up taking a significant amount of time, subjective to the amount of markets we are querying.