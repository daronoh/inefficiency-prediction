# Inefficiency Prediction — Polymarket Orderbook Pipeline

Research pipeline for the CP4101 dissertation: predicting short-horizon,
net-of-cost pricing inefficiency in Polymarket winner-take-all (negRisk) event
groups. This README is the **user guide** (what each file does, why, how to run
it). Research context, established findings, and the work plan live in
[DISSERTATION_PLAN.md](DISSERTATION_PLAN.md).

## Setup

- Python 3.12 venv at `<repo>/.venv` (`pip install -r requirements.txt`).
- Environment variables:
  - `DOME_API_KEY` — required only for orderbook downloads (Dome API). Never
    commit it; the previously committed key is treated as leaked.
  - `DATA_ROOT` — absolute path of the `data/` directory if running from a git
    worktree or another checkout. Defaults to `<repo>/data`.

All scripts read/write locations from [src/paths.py](src/paths.py) — there are
no per-script path flags. The only path most commands accept is
`--token-ids-path`, selecting *which tokens/markets* to operate on (default:
the full universe list; pass a per-stratum list to stage work).

## Source files (`src/`)

| File | What it does | Why |
|---|---|---|
| [paths.py](src/paths.py) | Single source of truth for every data location; `DATA_ROOT` override; `ensure_dirs()` | So a directory move is a one-line change and worktrees can reach the data |
| [data_loader.py](src/data_loader.py) | Parses the markets JSONL into markets/tokens tables; async Dome API downloader (pagination, retry/backoff, concurrency) | Turns the raw metadata dump + external API into on-disk parquet |
| [features.py](src/features.py) | Per-snapshot book features (best bid/ask, mid, spread, depth, imbalance, quality flags), leakage-safe metadata (scheduled-end expiry only; no lifetime volume), snapshot dedup, one-lag deltas | The model-ready per-token representation; every choice here is leakage-audited |
| [pairs.py](src/pairs.py) | Aligns a market's YES/NO books on shared timelines (event grid + wall-clock grid) with staleness columns and naive arb-gap measures | Alignment/QA layer; note YES/NO books are mirrored, so real labels live on *group* panels (src/groups.py, upcoming) |

## Pipeline scripts (`data-pipeline/`)

Run them in this order. All accept `--help`.

### 1. `select_event_universe.py` — choose the study universe (already run; outputs are frozen)
Scans the JSONL for markets alive in the study window and selects **complete
event groups** by stratum: `negrisk` (winner-take-all groups → rebalancing-arb
labels), `ladder` (date/price ladders → implication-arb extension), `control`
(seeded random mid-volume events → selection-bias check), `serial` (fee-bearing
15-min crypto slice → fee-era comparison). Flags each group's completeness
(`supports_full_cover`). Prints a download budget; downloads nothing.
```bash
python data-pipeline/select_event_universe.py
```
Outputs: `event_universe.parquet`, `universe_token_ids[_<stratum>].parquet`.
Re-running with unchanged arguments reproduces the frozen selection exactly.

### 2. `run_data_loader.py` — metadata tables + orderbook download
Rebuilds `markets.parquet`/`tokens.parquet` from the JSONL when missing (or on
`--rebuild-tables`), then downloads each token's full book-snapshot history.
```bash
python data-pipeline/run_data_loader.py --token-ids-path data/processed/universe_token_ids_negrisk.parquet --skip-existing
```
- `--skip-existing` makes downloads resumable (re-run the same command after
  any interruption) and never re-fetches tokens already on disk.
- `--skip-download` = tables only. Window defaults (2025-10-14 → 2026-03-31)
  cover every universe market's full life; you rarely need to change them.
- Recommended order: negrisk → ladder → control (serial deferred).

### 3. `run_feature_pipeline.py` — per-token feature tables
```bash
python data-pipeline/run_feature_pipeline.py
```
Reads `ob_*.parquet`, writes `feat_*.parquet`. `--keep-json-cols` retains the
raw book JSON; `--depth-n` changes the top-N depth window.

### 4. `run_pair_panel.py` — YES/NO pair panels (diagnostic layer)
```bash
python data-pipeline/run_pair_panel.py
```
Event-grid + 1-minute-grid panels per market with staleness/validity columns
and `long_arb_gap`/`short_arb_gap`. Clears its previous outputs each run.

### 5. `build_fee_schedule.py` — per-market fee reference
```bash
python data-pipeline/build_fee_schedule.py
```
Category buckets (heuristic keywords — review the printed assignments when the
universe changes), the dated 2025–26 fee-rollout table (counterfactual current
rates), gamma `feesEnabled` (the in-sample fee authority), and best-effort live
CLOB rates. `--skip-live` for offline runs. **Do not delete the output** —
`fee_schedule.parquet` contains a non-reproducible live-endpoint snapshot and
is the evidence behind every fee claim in the thesis.

### Notebooks
`data_loader.ipynb` / `feature.ipynb` are **legacy** mirrors of the old
70-market pilot flow and predate the current scripts. Kept for reference only.

## Data directory (`data/`, git-ignored)

| Path | Contents | Regenerable? |
|---|---|---|
| `polymarket_markets_1y.jsonl` | Frozen Gamma dump, 356,736 **resolved** markets (~Feb 4 2026); final outcomePrices | **No — keep an offsite backup** |
| `processed/event_universe.parquet` | The frozen study selection: 2,227 markets with event, stratum, `supports_full_cover` | Yes (selector, deterministic) |
| `processed/universe_token_ids[_stratum].parquet` | Download/processing lists (4,454 tokens total) | Yes (selector) |
| `processed/fee_schedule.parquet` | Per-market fee reference incl. one-time live snapshot (2026-08-08) | **Partially — live columns are not** |
| `processed/markets.parquet`, `tokens.parquet` | Flattened metadata (1 row per market / per token) | Yes (`run_data_loader.py`) |
| `raw_orderbook/ob_<token>.parquet` | Full book snapshots per token (bids/asks JSON) | Yes (re-download, costs API quota) |
| `processed/orderbook/feat_<token>.parquet` | Per-token feature tables | Yes |
| `processed/pairs/{event,grid_1m}/` | Pair panels | Yes |

## Research-validity notes (the short version)

- **Mirror books:** `ask_NO = 1 − bid_YES` exactly — intra-market YES+NO arb is
  structurally impossible; labels are cross-market (negRisk groups).
- **Leakage rules baked into features:** scheduled `end_date` only (11.4% of
  rows trade past it — negative expiry is real, and `log_time_to_expiry_hours`
  clips it to 0); no lifetime volume; `indexed_at` kept as the honest
  "data available at" clock; `flag_no_bids/asks/crossed_book` mark degenerate
  books — filter on them before computing anything from quotes.
- **Pair/group validity flags mean "fresh", not "tradeable"** — combine
  `valid_pair_*s` with the book-quality flags.
- **Fees:** the study sample is fee-free in-sample (gamma `feesEnabled=false`
  for every negrisk/ladder/control market); fee analysis is a counterfactual
  threshold sweep. Details and caveats: DISSERTATION_PLAN §2–3.
- **Evaluation protocol:** development = 2025-10-14→2026-01-07; lockbox
  holdout = 2026-01-08→2026-02-04, touched once at the end. Binding; see
  DISSERTATION_PLAN §4.
