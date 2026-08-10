# DATA_DICTIONARY — every derived column, its formula, and where to verify it

*One section per dataset, in pipeline order. Each derived column: what it
represents, how it's calculated, and the defining code (file:function) so you
can check the source yourself. Raw passthrough columns (ids, timestamps,
questions) are listed only when their semantics are non-obvious.*

---

## 1. `processed/markets.parquet` / `processed/tokens.parquet`

Flattened Gamma metadata (one row per market / per outcome token). Built by
`read_market_to_df` / `read_token_to_df` in [src/data_loader.py](src/data_loader.py).

| Column | Meaning / derivation |
|---|---|
| `fee_decimal` | Gamma `fee` parsed; legacy 18-decimal fixed point (`2e16 → 0.02`) via `parse_fee_decimal`. All-NaN for modern markets — superseded by fee_schedule.parquet |
| `outcome_price_initial` (tokens) | Final resolution price of this outcome from `outcomePrices` (the dump is resolved-only, so 1.0 = this outcome won) |
| `outcome_index` (tokens) | Position in the market's outcome list; index 0 = "Yes" on all negRisk markets (verified for all 949) |
| `n_outcomes`, `outcomes_raw`, `outcome_prices_raw` | Count and JSON re-dumps of the outcome lists |

```python
# src/data_loader.py — parse_fee_decimal
i = int(s)
return i / 1e18 if i > 1 else float(i)
```

## 2. `processed/event_universe.parquet`

The frozen study selection (one row per selected market). Built by
[select_event_universe.py](data-pipeline/select_event_universe.py).

| Column | Meaning / derivation |
|---|---|
| `event_slug` | `events[0].slug` from Gamma; missing → singleton `market-<id>` |
| `is_serial` | `"up or down"` occurs in the question (case-insensitive) |
| `is_match` | regex `\bvs\.?\b` in question or event title — match-style events are barred from the ladder stratum |
| `resolved_yes` | final `outcomePrices[0] >= 0.99` (this market resolved YES) |
| `stratum` | `negrisk` (raw negRisk flag, ≥3 members, ≤35, ≥$1M, top-50 by volume) → `ladder` (non-negrisk, non-match, ≥3 same-template members, top-30) → `control` (seed-42 sample of $100k–$5M leftovers) → `serial` |
| `winner_present` | any member of the event has `resolved_yes` |
| `supports_full_cover` | `stratum == "negrisk" AND winner_present` — long-side labels are only valid here |

```python
# data-pipeline/select_event_universe.py — question_template (ladder detection)
_TEMPLATE_STRIP = re.compile(r"\$?[\d,.]+%?|january|february|...|sunday", re.I)
return re.sub(r"\s+", " ", _TEMPLATE_STRIP.sub("#", q or "")).strip().lower()
```

Verification: `python data-pipeline/verify_universe.py` re-derives all of this
from the raw JSONL (8 PASS/FAIL invariants).

## 3. `processed/fee_schedule.parquet`

One row per universe market. Built by [build_fee_schedule.py](data-pipeline/build_fee_schedule.py).

| Column | Meaning / derivation |
|---|---|
| `category_bucket` | keyword classifier over question/title (heuristic; review on resample) |
| `fees_enabled_gamma` | Gamma `feesEnabled` — the **in-sample fee authority** (false = the market never charged fees while alive) |
| `fee_rate_current` / `fee_rate_at_market_end` / `fee_rate_at_sample_end` | dated regime table `FEE_REGIME` evaluated at now / market close / sample end — **counterfactual category rates**, not per-market history |
| `fee_free_in_sample` | `not feesEnabled` (fallback: regime rate at market close == 0) |
| `clob_base_fee` | live CLOB endpoint snapshot (2026-08-08) — retroactive current config, NOT historical |

```python
# data-pipeline/build_fee_schedule.py — fee_rate_at
for start, r in sorted(FEE_REGIME.get(bucket, [])):
    if ts >= pd.Timestamp(start, tz="UTC"):
        rate = r
```

## 4. `processed/orderbook/feat_<token_id>.parquet` (per-token features, 40 cols)

One row per deduplicated book snapshot. Built by
`extract_snapshot_features` + `attach_token_metadata` +
`add_token_time_series_features` in [src/features.py](src/features.py).

| Column | Meaning / derivation |
|---|---|
| `best_bid`, `best_ask` (+ sizes) | max-price bid / min-price ask level and its resting size |
| `mid_price` | `(best_bid + best_ask) / 2`; null if either side empty |
| `spread` | `best_ask − best_bid` (negative ⇒ crossed) |
| `bid/ask_depth_3` | shares summed over the top 3 price levels per side |
| `imbalance_3`, `imbalance_total` | `(bid_depth − ask_depth) / (bid_depth + ask_depth)` at top-3 / full book |
| `n_bid/ask_levels` | number of price levels per side |
| `flag_no_bids/asks`, `flag_crossed_book` | book-quality flags (empty side / `best_bid > best_ask`) |
| `vwap_ask_{10,100,1000}` | average fill price to BUY q shares walking asks lowest-first; NaN if the book holds < q shares |
| `vwap_bid_{10,100,1000}` | average fill price to SELL q shares walking bids highest-first; NaN likewise |
| `time_to_expiry_hours` | `end_date − snapshot_time` (scheduled end only — `closed_time` would leak; 11.4% of rows are negative because markets trade past schedule) |
| `log_time_to_expiry_hours` | `log1p(clip(tte, 0))` — clips the negative mass to 0 |
| `delta_best_bid/ask`, `delta_mid_price` | change vs the previous snapshot of the same token |
| `seconds_since_prev_snapshot` | inter-snapshot gap |
| `indexed_at_time/_ms` | Dome ingestion clock — honest "data available at" time |

```python
# src/features.py — get_vwap_by_shares (the executable-price walk)
while ti < len(remaining) and cum_sh + sz >= remaining[ti]:
    need = remaining[ti] - cum_sh
    out[remaining[ti]] = (cum_cost + need * px) / remaining[ti]
    ti += 1
```
Hand-check: a book of asks 5@0.50, 20@0.55, 100@0.60 gives
`vwap_ask_10 = (5·0.50 + 5·0.55)/10 = 0.525`.

## 5. `processed/pairs/{event,grid_1m}/pair_<market_id>.parquet`

YES/NO alignment panels (diagnostic layer). Built by [src/pairs.py](src/pairs.py).
`y0_*/y1_*` = outcome-0/1 token state as-of each row's `time` (backward as-of
join); each side carries `*_staleness_s` = `time − that side's snapshot_time`.

| Column | Meaning / derivation |
|---|---|
| `sum_best_asks` / `sum_best_bids` | `y0 + y1` ask / bid sums |
| `long_arb_gap` | `1 − sum_best_asks` (>0 ⇒ buy-both-merge arb before costs; structurally ≤ 0 up to desync — mirror books) |
| `short_arb_gap` | `sum_best_bids − 1` (>0 ⇒ split-and-sell arb before costs) |
| `valid_pair_{2,5,10,30}s` | both sides' staleness ≤ cap (fresh ≠ tradeable — combine with the flag columns) |

## 6. `processed/groups/{event,grid_1m}/group_<event_slug>.parquet`

The core research panels: all N members of one winner-take-all event, aligned.
Built by [src/groups.py](src/groups.py); member `i`'s YES-book state appears as
`m<i>_*` columns (mapping in `groups/members.parquet`).

| Column | Meaning / derivation |
|---|---|
| `m<i>_staleness_s` | `time − m<i>_snapshot_time`; null before the member's first snapshot |
| `n_asks_quoted` / `n_bids_quoted` | members with a non-null best ask / bid at this row |
| `sum_asks_quoted` / `sum_bids_quoted` | fill-null(0) sums of quoted members' best asks / bids |
| `long_gap` | `1 − sum_asks_quoted`, but ONLY when `full_cover_ok` (event fully captured: `supports_full_cover` and every member has data) AND every member quotes an ask; else null. Positive ⇒ buy every outcome for < $1 guaranteed payout |
| `short_gap` | `sum_bids_quoted − 1` over quoted members (subset-NO basket guarantees ≥ \|S\|−1); null only when zero bids |
| `exec_long_gap_{q}` | `1 − Σ vwap_ask_q_i` — the EXECUTABLE version at q shares; same full-cover gate plus every member fillable for q shares |
| `exec_short_gap_{q}` | `Σ vwap_bid_q_i − 1` over members fillable for q shares |
| `fee_basis_long/short_{q}` | `Σ p_i(1−p_i)` over the same legs, where p = the leg's VWAP — multiply by a feeRate to price the counterfactual taker fee per share |
| `all_books_clean` | every PRESENT member is two-sided and non-crossed — the gate that removes resolution-transition phantoms |
| `max_staleness_s`, `valid_group_{caps}s` | max member staleness; all-fresh flags (nulls of unborn members are skipped — see plan §"F2") |

```python
# src/groups.py — the two core signals
pl.when(full_cover_ok & (pl.col("n_asks_quoted") == n_members_total))
  .then(1.0 - pl.col("sum_asks_quoted")).otherwise(None).alias("long_gap"),
pl.when(pl.col("n_bids_quoted") > 0)
  .then(pl.col("sum_bids_quoted") - 1.0).otherwise(None).alias("short_gap"),
```

Verified: an independent re-implementation matched every stored value on a
full event (287,627 rows × 67 cols) and all gating rules across all 100 panels.

## 7. `processed/groups/members.parquet`

| Column | Meaning |
|---|---|
| `member_idx` | the `i` in `m<i>_*`, contiguous per event, ordered by market_id |
| `yes_token_id` | the market's outcome-0 token whose feat file feeds the panel |
| `has_data` | feat file exists (13 members false: 12 Bolivia pre-window + 1 LoL) |
| `supports_full_cover` | copied from the universe (event-level) |

## 8. `processed/labels/` (Stage 2 outputs — [src/labels.py](src/labels.py))

**`spells.parquet`** — one row per opportunity spell per definition:

| Column | Meaning / derivation |
|---|---|
| `side`, `size_q`, `fee_rate` | which measure: long/short, VWAP size q, counterfactual feeRate |
| net gap (condition) | `exec_gap_q − fee_rate × fee_basis_q > 0`; long side additionally requires `all_books_clean` on every row |
| `start_time`, `end_time`, `duration_s` | maximal consecutive run of the condition; duration in SECONDS (a one-row flicker = 0 s) |
| `n_rows`, `max/mean_net_gap`, `max_staleness_s` | spell metrics |
| `split` | `development` (< 2026-01-08) / `lockbox` by start_time |

**`labels_grid.parquet`** — model targets on the wall-clock 1-min grid:

| Column | Meaning |
|---|---|
| `y_long_{q}_{15m,60m}` | 1 iff a qualifying long spell (fee 0, clean books, duration ≥ 5 s) STARTS in `(t, t+H]` |
| `max_gap_100_60m` | max net gap of qualifying 100-share spells starting in the next 60 m (0 if none) |
| `split` | development / lockbox — all exploration must filter `development` |

**`attrition_dev.parquet`** — the layered survival table (development only):
spells/events/minutes surviving each successive friction layer (size →
persistence → counterfactual fee rates → freshness variant).

```python
# src/labels.py — spell detection core
(pl.col("net_gap") > 0) & (pl.col("all_books_clean") if require_clean_books else pl.lit(True))
...
(pl.col("cond") != pl.col("cond").shift(1, fill_value=False)).cum_sum().alias("run_id")
```
