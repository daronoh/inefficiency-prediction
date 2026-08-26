# DATA_DICTIONARY — stored datasets, columns, formulas, and status

**Artifact audit date:** 26 August 2026

This document describes the data currently stored in this checkout and the
code that defines each derived field. Counts are artifact facts, not planned
targets. Datasets that do not yet exist are listed separately in Section 12.

## 0. Conventions and important warnings

### 0.1 Status vocabulary

- **Materialised:** the output file currently exists.
- **Implemented:** source code exists, whether or not its output is present.
- **Artifact-checked:** current file count, row count, and/or schema was read.
- **Independently reviewed:** project notes record a separate
  reimplementation, but its code/report is not retained as a repeatable test.
- **Not implemented:** neither a current output nor its producing stage exists.

### 0.2 Time conventions

- **snapshot_time** is the exchange/book timestamp carried by the Dome record.
- **indexed_at_time** is when Dome indexed the record and is the honest Dome
  availability timestamp.
- Current group panels use indexed_at_ms only to resolve duplicate records at
  the same snapshot timestamp, then drop it and align on snapshot_time.
- Therefore, all current group panels, spells, and labels use an
  **exchange-snapshot-time convention**, not a demonstrated
  Dome-availability-time convention.
- If indexed_at becomes the deployment clock, all downstream panels, labels,
  attrition, eligibility, and latency results must be rebuilt and reviewed.

All timestamps described below are UTC unless stated otherwise.

### 0.3 Price-gap units

Fields named long_gap, short_gap, exec_long_gap_q, exec_short_gap_q, and
net-gap spell statistics are **per-share values in dollars**.

For trade size \(q\), total basket value is:

\[
q\times\text{per-share gap}.
\]

The exec prefix means feasible against the **displayed historical depth** at
the requested size. It does not prove simultaneous multi-leg fills after
latency.

### 0.4 Leakage warning

The Gamma dump is a final-state, resolved-market snapshot. Final outcome,
realised close time, lifetime volume, and dump-time quote fields are ex-post
and must not be model features. Scheduled end date, category, and event
membership are safe only if their historical values were unchanged; verify,
reconstruct, or exclude them in a strict ablation.

## 1. Artifact inventory

| Dataset | Current materialisation | Grain |
|---|---:|---|
| data/polymarket_markets_1y.jsonl | 356,736 JSON records, about 1.9 GB | Gamma market |
| processed/markets.parquet | 356,736 rows × 27 columns | Market |
| processed/tokens.parquet | 713,472 × 20 | Outcome token |
| processed/event_universe.parquet | 2,238 × 17 | Selected market |
| processed/universe_token_ids*.parquet | 4,476 total token IDs | Token |
| processed/fee_schedule.parquet | 2,227 × 17; stale membership | Fee-reference market |
| raw_orderbook/ob_*.parquet | 1,872 files; 9,460,388 rows | Token book snapshot |
| processed/orderbook/feat_*.parquet | 1,872 files; 9,460,388 rows × 40 columns | Token book snapshot |
| processed/pairs/... | No stored Parquet files | Code-defined diagnostic only |
| processed/groups/event/... | 50 files; 4,225,314 rows | Any-member update |
| processed/groups/grid_1m/... | 50 files; 2,216,159 rows | Event-minute |
| processed/groups/members.parquet | 949 × 7 | negRisk event member |
| processed/labels/spells.parquet | 33,166 × 13 | Opportunity spell definition |
| processed/labels/labels_grid.parquet | 2,215,679 × 10 | Event-minute target row |
| processed/labels/attrition_dev.parquet | 10 × 6 | Development friction definition |
| Model matrices, scores, shortlists, trade/backtest ledgers | Not implemented | See Section 12 |

## 2. Raw Gamma dump

### data/polymarket_markets_1y.jsonl

Irreplaceable frozen input containing 356,736 resolved market records. Keep an
offsite copy.

Important semantics:

- every stored market is closed in the dump;
- outcomePrices are final resolution values;
- volume fields are lifetime totals;
- closedTime is realised and therefore future information at earlier
  snapshots;
- dump-time bid/ask/trade fields are not historical snapshot features; and
- final event membership may not equal membership known at every earlier time.

The file is parsed by read_market_to_df and read_token_to_df in
[src/data_loader.py](src/data_loader.py).

## 3. Flattened metadata tables

### 3.1 processed/markets.parquet

**Current shape:** 356,736 rows × 27 columns.
**Grain:** one Gamma market.

| Column family | Columns and meaning |
|---|---|
| Identity/text | market_id, condition_id, question, slug, description |
| Dates | start_date, end_date, closed_time; stored as strings in this table |
| Final-state flags | active, closed, archived, accepting_orders, enable_order_book |
| Structure | neg_risk, n_outcomes |
| Legacy fee | fee_decimal; parsed from the old 18-decimal representation and generally null for modern markets |
| Ex-post activity | volume, volume_clob; lifetime totals, selection/audit only |
| Dump-time state | market_best_bid, market_best_ask, last_trade_price; not historical features |
| Resolution | resolution_source, uma_resolution_status |
| Metadata clocks | created_at, updated_at |
| Outcome JSON | outcomes_raw, outcome_prices_raw |

fee_decimal parsing is defined by parse_fee_decimal in
[src/data_loader.py](src/data_loader.py). Values greater than 1 are divided by
\(10^{18}\).

### 3.2 processed/tokens.parquet

**Current shape:** 713,472 rows × 20 columns.
**Grain:** one outcome token; exactly two stored tokens per market.

| Column | Meaning |
|---|---|
| market_id, token_id | Parent market and CLOB token identifiers |
| outcome_index | Position in the outcome list; index 0 is YES for all 949 selected negRisk markets |
| outcome | Outcome name |
| outcome_price_initial | Misleading legacy name: this is the **final resolution price** from the resolved-only dump |
| question, slug, description | Copied market text |
| start_date, end_date, closed_time | Copied strings; closed_time is ex-post |
| state/structure fields | active, closed, archived, accepting_orders, enable_order_book, neg_risk |
| fee_decimal | Legacy Gamma fee field |
| volume, volume_clob | Lifetime totals; do not use as point-in-time predictors |

Built by read_token_to_df in [src/data_loader.py](src/data_loader.py).

## 4. Frozen study universe and token lists

### 4.1 processed/event_universe.parquet

**Current shape:** 2,238 rows × 17 columns.
**Grain:** one selected binary market.

| Stratum | Event slugs | Markets | Tokens |
|---|---:|---:|---:|
| negrisk | 50 | 949 | 1,898 |
| ladder | 30 | 361 | 722 |
| control | 25 | 176 | 352 |
| serial | 752 | 752 | 1,504 |
| **Total** | **857** | **2,238** | **4,476** |

Built by [data-pipeline/select_event_universe.py](data-pipeline/select_event_universe.py).

| Column | Meaning / derivation |
|---|---|
| market_id, question, slug | Market identifiers and text |
| event_slug | events[0].slug; missing events become market-&lt;id&gt; singleton slugs |
| event_title | Event title from Gamma |
| neg_risk | Raw Gamma negRisk flag |
| volume | Lifetime market volume used for frozen sample selection, never a prediction feature |
| start_date, end_date | Parsed UTC metadata dates from the final dump |
| n_tokens | Number of outcome tokens, normally two |
| token_ids | JSON-serialised token-ID list |
| is_serial | True when “up or down” appears in the question |
| is_match | True when a question/title contains a “vs” match pattern; excluded from ladder |
| resolved_yes | Final outcomePrices[0] at least 0.99 |
| stratum | negrisk, ladder, control, or serial |
| winner_present | Any selected sibling has resolved_yes=true |
| supports_full_cover | stratum is negrisk and winner_present=true |

supports_full_cover is a **retrospective sample-quality field** based on final
outcomes. It is not a deployable model feature.

Forty-eight negRisk groups, containing 910 markets, have
supports_full_cover=true. After missing Dome histories are considered, only 46
groups are effectively usable for long-side construction.

[data-pipeline/verify_universe.py](data-pipeline/verify_universe.py) re-derives
the selection invariants from the raw JSONL. This verifier is retained and
runnable.

### 4.2 Token-list files

Each file has one token_id column and one token per row:

| File | Rows |
|---|---:|
| universe_token_ids.parquet | 4,476 |
| universe_token_ids_negrisk.parquet | 1,898 |
| universe_token_ids_ladder.parquet | 722 |
| universe_token_ids_control.parquet | 352 |
| universe_token_ids_serial.parquet | 1,504 |

These lists select which token histories the downloader processes.

## 5. Fee reference

### processed/fee_schedule.parquet

**Current shape:** 2,227 rows × 17 columns.
**Intended grain:** one fee-reference row per selected market.
**Current status:** membership predates universe v1.1.

Membership reconciliation:

- 2,205 rows overlap the current 2,238-market universe;
- 33 current ladder markets are missing;
- 22 obsolete ladder markets remain;
- negRisk coverage is 949/949;
- control coverage is 176/176;
- serial coverage is 752/752; and
- ladder coverage is 328/361.

Preserve the existing live-response fields before rebuilding membership.

Built by [data-pipeline/build_fee_schedule.py](data-pipeline/build_fee_schedule.py).

| Column | Meaning / derivation |
|---|---|
| market_id, question, event_title, event_slug | Market/event identity |
| event_category_raw | Presently null in the stored table |
| event_tags | Serialised Gamma tags |
| category_bucket | Heuristic keyword category; review whenever membership changes |
| gamma_fee_raw | Raw Gamma fee value |
| fees_enabled_gamma | Best in-sample fee authority |
| closed_time | Final realised close time; used for fee-regime reference, not prediction |
| fee_rate_current | Regime rate current **when built on 8 August 2026**, not current today |
| fee_rate_at_market_end | Category-regime lookup at realised close |
| fee_rate_at_sample_end | Category-regime lookup at the configured sample endpoint |
| fee_free_in_sample | Negation of fees_enabled_gamma, with regime fallback |
| fee_start_date | Configured category fee start |
| clob_fee_rate_live | Raw one-time live endpoint response |
| clob_base_fee | One-time 8 August 2026 CLOB base-fee snapshot |

Stored fee evidence:

- fees_enabled_gamma=false for 1,659 rows and true for 568;
- all covered negRisk, control, and ladder rows are false;
- serial contains 568 true and 184 false;
- clob_base_fee is 0 for 1,616 rows and 1000 for 611 rows.

Current/live CLOB fields are not historical per-market fee records.
Counterfactual category rates must be labelled as such.

## 6. Raw Dome order books

### raw_orderbook/ob_&lt;token_id&gt;.parquet

**Current materialisation:** 1,872 files, 9,460,388 rows.
**Grain:** one unique token/book snapshot timestamp.
**Observed coverage:** 14 October 2025 19:37:42 to 3 February 2026 08:22:38.

Only the negRisk stratum is downloaded. Coverage is 1,872/1,898 tokens. The
26 missing token files are both outcomes of 13 markets: 12 Bolivia markets and
one League of Legends market.

Downloaded by the Dome client in [src/data_loader.py](src/data_loader.py).

| Column | Meaning |
|---|---|
| token_id | Requested CLOB token |
| snapshot_time | Exchange/book timestamp |
| snapshot_timestamp_ms | Millisecond integer form of snapshot_time |
| indexed_at_time | Dome ingestion/availability timestamp |
| indexed_at_ms | Millisecond integer form of indexed_at_time |
| market_hash | Dome market identifier |
| asset_id | Dome asset identifier |
| tick_size | Minimum price increment in that snapshot |
| min_order_size | Minimum order quantity |
| orderbook_neg_risk | Snapshot-level negRisk flag |
| bids_json | Serialised bid levels |
| asks_json | Serialised ask levels |

The feed is change-triggered. Silence may mean an unchanged book, but it can
also conceal capture outages.

Observed indexed-at delay relative to snapshot time:

- median: 286 ms;
- 95th percentile: 1,745 ms;
- maximum: 1,975,716 ms; and
- 12,436 rows are slightly negative because of clock skew, minimum −171 ms.

These statistics make the final availability-clock choice economically
material.

## 7. Per-token snapshot features

### processed/orderbook/feat_&lt;token_id&gt;.parquet

**Current materialisation:** 1,872 files, 9,460,388 rows, 40 columns.
**Grain:** one unique token/snapshot timestamp.

Every current raw file has one feature file and the total row counts match.
No duplicate token/snapshot timestamps remain in the stored corpus.

Built by extract_snapshot_features, attach_token_metadata, and
add_token_time_series_features in [src/features.py](src/features.py).

| Feature family | Columns | Derivation |
|---|---|---|
| Identity/time | token_id, snapshot_time, snapshot_timestamp_ms, indexed_at_time, indexed_at_ms | Raw passthrough |
| Book configuration | tick_size, min_order_size, orderbook_neg_risk | Raw passthrough |
| Best quotes | best_bid, best_bid_size, best_ask, best_ask_size | Maximum bid / minimum ask and resting quantity |
| Price state | mid_price, spread | \((bid+ask)/2\); ask minus bid |
| Top-three depth | bid_depth_3, ask_depth_3 | Sum shares over best three levels |
| Total depth | bid_depth_total, ask_depth_total | Sum all displayed shares by side |
| Imbalance | imbalance_3, imbalance_total | \((bidDepth-askDepth)/(bidDepth+askDepth)\) |
| Shape | n_bid_levels, n_ask_levels | Number of price levels |
| Quality | flag_no_bids, flag_no_asks, flag_crossed_book | Empty side or best_bid greater than best_ask |
| Displayed VWAP | vwap_ask_10/100/1000, vwap_bid_10/100/1000 | Walk asks low-to-high or bids high-to-low; null if insufficient depth |
| Attached metadata | market_id, outcome_index, neg_risk | Joined from tokens.parquet |
| Scheduled expiry | time_to_expiry_hours | final-dump end_date minus snapshot_time |
| Expiry transform | log_time_to_expiry_hours | log1p of time-to-expiry clipped below at zero |
| One-update dynamics | delta_best_bid, delta_best_ask, delta_mid_price | Difference from previous snapshot of the token |
| Update interval | seconds_since_prev_snapshot | Elapsed snapshot time since previous row |

Current expiry audit:

- 562,832 of 9,460,388 rows, 5.949%, have negative
  time_to_expiry_hours because trading continued after scheduled end;
- 53,980 rows have null scheduled expiry; and
- the older 11.4% documentation figure is not valid for the current corpus.

Both clocks remain present here, but downstream group construction currently
uses snapshot_time. end_date comes from final-state metadata; verify that it
was not revised historically or exclude the feature in a strict ablation.

## 8. YES/NO pair panels — implemented format, not materialised

### processed/pairs/{event,grid_1m}/pair_&lt;market_id&gt;.parquet

Producer: [src/pairs.py](src/pairs.py) and
[data-pipeline/run_pair_panel.py](data-pipeline/run_pair_panel.py).

**Current status:** no pair Parquet files or populated pair output directories
are present.

If generated, the format contains backward-as-of aligned outcome-0 and
outcome-1 token state:

| Column | Meaning |
|---|---|
| y0_*, y1_* | Member feature state prefixed by outcome index |
| y0/y1_staleness_s | Panel time minus source snapshot time |
| sum_best_asks, sum_best_bids | Complementary-token quote sums |
| long_arb_gap | 1 minus sum_best_asks |
| short_arb_gap | sum_best_bids minus 1 |
| valid_pair_2s/5s/10s/30s | Both source rows within the staleness cap |

The pair layer is diagnostic because YES/NO books mirror one order pool. The
previously recorded 99.999% mirror result is not currently reproducible from
stored pair artifacts.

## 9. negRisk group membership and panels

### 9.1 processed/groups/members.parquet

**Current shape:** 949 rows × 7 columns.
**Grain:** one selected negRisk market/member.

| Column | Meaning |
|---|---|
| event_slug | Parent negRisk group |
| member_idx | Stable zero-based member index, ordered by market_id |
| market_id | Binary market |
| question | Member question |
| yes_token_id | Outcome-0/YES token feeding the group panel |
| has_data | Whether the YES feature file exists |
| supports_full_cover | Retrospective universe flag copied to every member |

936 members have data and 13 do not.

Four groups have long-side gaps permanently disabled in the current
construction:

- bolivia-presidential-election — incomplete Dome members;
- lol-worlds-2025-winner — incomplete Dome member;
- super-bowl-champion-2026-731 — winner absent from frozen dump; and
- uefa-champions-league-winner — winner absent from frozen dump.

### 9.2 Group panel files

| Path | Files | Total rows | Grain |
|---|---:|---:|---|
| processed/groups/event/group_&lt;event_slug&gt;.parquet | 50 | 4,225,314 | Union of all available member update times |
| processed/groups/grid_1m/group_&lt;event_slug&gt;.parquet | 50 | 2,216,159 | One wall-clock minute over union coverage |

Built by [src/groups.py](src/groups.py) and
[data-pipeline/run_group_panel.py](data-pipeline/run_group_panel.py).

Schemas vary because groups have different numbers of members with data:

- 89 to 709 columns;
- 22 distinct stored schemas; and
- exact width \(=29+20D\), where \(D\) is members with a feature file.

Each present member \(i\) contributes these m&lt;i&gt;_ fields:

- best_bid, best_bid_size, best_ask, best_ask_size;
- spread;
- bid_depth_3, ask_depth_3, bid_depth_total, ask_depth_total;
- flag_no_bids, flag_no_asks, flag_crossed_book;
- vwap_ask_10/100/1000 and vwap_bid_10/100/1000;
- snapshot_time; and
- staleness_s.

Fixed group fields:

| Column | Meaning / derivation |
|---|---|
| time | Event update time or one-minute grid time |
| event_slug | Group identifier |
| n_asks_quoted, n_bids_quoted | Present members with non-null best quote |
| sum_asks_quoted, sum_bids_quoted | Quote sums; null member values contribute zero |
| max_staleness_s | Maximum staleness among present member states |
| all_books_clean | Every **present** member is two-sided and non-crossed |
| long_gap | \(1-\sum ask_i\), only when full_cover_ok and every total member quotes |
| short_gap | \(\sum bid_i-1\) across quoted members |
| exec_long_gap_q | \(1-\sum VWAPAsk_{i,q}\), requiring full cover and every member fillable |
| fee_basis_long_q | \(\sum p_i(1-p_i)\) for the long legs |
| exec_short_gap_q | \(\sum VWAPBid_{i,q}-1\) over fillable contributors |
| fee_basis_short_q | \(\sum p_i(1-p_i)\) for short contributors |
| short_clean_q | Every member contributing to the size-\(q\) short basket is clean |
| valid_group_2s/5s/10s/30s | max_staleness_s within the cap |

q is one of 10, 100, or 1,000 shares.

Critical caveats:

- all_books_clean and valid_group_* operate over **present** members and can
  be true before every event member has appeared;
- they are quality/freshness fields, not point-in-time eligibility;
- valid_group_* skips unborn-member nulls;
- long_gap has a separate complete-coverage gate;
- backward-as-of state is carried indefinitely; and
- group files no longer contain indexed_at, so they cannot reproduce Dome
  availability without rebuilding.

Project notes record an independent Stage 1 reimplementation, but its
verifier/report is not retained as an automated test.

## 10. Opportunity spells

### processed/labels/spells.parquet

**Current shape:** 33,166 rows × 13 columns.
**Grain:** one maximal positive-net-gap run for one event, side, size, and fee
rate.

Built by [src/labels.py](src/labels.py) and
[data-pipeline/run_labels.py](data-pipeline/run_labels.py).

Stored split/side counts:

| Split | Long rows | Short rows | Total |
|---|---:|---:|---:|
| development | 1,562 | 25,357 | 26,919 |
| lockbox, mechanically materialised | 100 | 6,147 | 6,247 |
| **Total** | **1,662** | **31,504** | **33,166** |

The row condition is:

\[
\operatorname{net\_gap}
=\operatorname{exec\_gap}_{q}
-\operatorname{fee\_rate}\times\operatorname{fee\_basis}_{q}>0.
\]

Long spells also require all_books_clean. Short spells require
short_clean_q for the contributing size-\(q\) members.

| Column | Meaning |
|---|---|
| start_time | First true event-grid row |
| last_true_time | Last event-grid row where the condition is true |
| end_time | Next event-grid update after last_true_time; the flip/open-end row |
| duration_s | end_time minus start_time in seconds |
| n_rows | Number of true event-grid rows |
| max_net_gap, mean_net_gap | Per-share gap statistics across true rows |
| max_staleness_s | Maximum group/member staleness recorded during the spell |
| event_slug | Group |
| side | long or short |
| size_q | 10, 100, or 1,000 shares |
| fee_rate | 0, 0.04, 0.05, or 0.07 |
| split | development or lockbox by start_time |

Spell time is interpreted as the half-open interval
\([start\_time,end\_time)\): the carried-forward book remains unchanged until
the next event-grid update. Therefore, a non-terminal one-row spell normally
has positive duration. A terminal positive row with no later update is
truncated at its own timestamp. In the current artifact, all 7,355 one-row
spells have positive duration and no stored spell has zero duration.

Development spells crossing 8 January 2026 00:00 are clipped at the boundary
and duration is recomputed. The lockbox rows are mechanically stored; their
presence is not a model/backtest evaluation.

For total simulated value at size \(q\), multiply max_net_gap or the
appropriate execution-time per-share gap by \(q\).

## 11. Forward labels and development attrition

### 11.1 processed/labels/labels_grid.parquet

**Current shape:** 2,215,679 rows × 10 columns.
**Grain:** event group × wall-clock minute.
**Observed time:** 14 October 2025 19:38 to 3 February 2026 08:22.

| Split | Rows | Event groups |
|---|---:|---:|
| development | 1,989,260 | 48 |
| lockbox, mechanically materialised | 226,419 | 10 |

The label table is 480 rows smaller than the source one-minute panels because
development rows whose maximum one-hour target window crosses the split are
embargoed.

| Column | Meaning |
|---|---|
| time, event_slug | Unique event-minute key in the stored table |
| y_long_10_15m / y_long_10_60m | Qualifying 10-share spell starts in \((t,t+H]\) |
| y_long_100_15m / y_long_100_60m | Qualifying 100-share spell starts in \((t,t+H]\) |
| y_long_1000_15m / y_long_1000_60m | Qualifying 1,000-share spell starts in \((t,t+H]\) |
| max_gap_100_60m | Maximum max_net_gap among qualifying 100-share spells starting in the next hour; zero if none |
| split | development when time is before 8 January 2026 00:00, otherwise lockbox |

A qualifying binary-label spell is:

- side=long;
- fee_rate=0;
- duration_s at least 5;
- already subject to long-side displayed-depth and all_books_clean rules; and
- starts strictly after \(t\) and no later than \(t+H\).

The table has **no eligibility column**. The four permanently disabled groups
listed in Section 9 currently contribute only long-label zeros and must not be
ordinary primary-model negatives.

Primary-target denominators:

| Population | Development groups | Rows | Positives | Positive rate |
|---|---:|---:|---:|---:|
| Stored all-panel table | 48 | 1,989,260 | 12,562 | 0.6315% |
| Permanent effective-coverage filter | 44 | 1,699,115 | 12,562 | 0.7393% |
| Diagnostic: all member states have appeared | — | 1,410,583 | 12,562 | 0.8906% |

The last row is only a book-readiness diagnostic, not yet a defensible live
eligibility definition. supports_full_cover is an ex-post filter and must not
be a model feature.

Only eight of the ten lockbox groups are effectively long-side eligible. Six
were observed in development and two are new. split is a column, not access
control: lockbox outcomes remain quarantined from model and strategy choices.

### 11.2 processed/labels/attrition_dev.parquet

**Current shape:** 10 rows × 6 columns.
**Grain:** one predefined development-only friction definition.

| Column | Meaning |
|---|---|
| layer | Human-readable definition |
| n_spells | Spell count under that definition |
| n_events | Unique event groups represented |
| total_minutes | Sum of spell duration_s divided by 60 |
| median_duration_s | Median spell duration |
| median_max_gap | Median per-share max_net_gap |

Stored values:

| Definition | Spells | Events | Total minutes | Median duration s | Median max gap |
|---|---:|---:|---:|---:|---:|
| Executable at 10 shares, fee 0 | 518 | 11 | 8,679.9 | 134.6 | 0.0050 |
| Executable at 100 shares, fee 0 | 405 | 11 | 4,806.9 | 100.8 | 0.0050 |
| Executable at 1,000 shares, fee 0 | 219 | 8 | 2,208.6 | 96.0 | 0.0040 |
| 100 shares, persist at least 5 s | 344 | 10 | 4,804.9 | 174.7 | 0.0050 |
| 100 shares, persist at least 30 s | 271 | 10 | 4,788.2 | 300.0 | 0.0050 |
| 100 shares, at least 5 s, fee 0.04 | 58 | 9 | 698.4 | 202.5 | 0.0045 |
| 100 shares, at least 5 s, fee 0.05 | 38 | 9 | 455.9 | 238.3 | 0.0080 |
| 100 shares, at least 5 s, fee 0.07 | 26 | 8 | 208.9 | 185.8 | 0.0099 |
| Fresh at most 30 s, 100 shares, at least 5 s, fee 0 | 22 | 6 | 38.9 | 15.3 | 0.0068 |
| Short side, 100 shares, at least 5 s, fee 0 | 1,498 | 46 | 6,304.6 | 28.7 | 0.0020 |

The rows are not one strict spell cohort. Sizes are alternative definitions,
and changing a fee threshold can split or merge maximal runs. Interpret each
row as its own predefined filter/definition.

## 12. Required future datasets — not yet implemented

No model matrix, selector output, or backtest ledger currently exists. The
following are recommended data contracts; names and fields are provisional
until the relevant stage is implemented. Update this dictionary before using
them in the lockbox evaluation.

### 12.1 Eligibility table

**Provisional path:** processed/modeling/eligibility.parquet
**Grain:** decision time × event group.

Required fields:

- eligible and explicit reason flags;
- point-in-time membership/version;
- full-cover and tradability evidence available at that time;
- broad-tier feature availability;
- event, market, token, and subscription monitoring cost; and
- availability-clock convention.

### 12.2 Fixed-width event-minute features

**Provisional path:** processed/modeling/event_minute_features.parquet
**Grain:** event group × decision time.

Required fields:

- fixed-width point-in-time group aggregates;
- feature_cutoff_time and available_at;
- availability-clock convention;
- source/member coverage flags;
- no final-outcome, realised-close, or lifetime-volume predictors; and
- metadata-history/ablation flags.

### 12.3 Training matrix

**Provisional path:** processed/modeling/training_matrix.parquet
**Grain:** eligible event group × decision time.

Required fields:

- unique event-time key;
- fixed features and joined targets;
- split and walk-forward fold IDs;
- eligibility version;
- embargo status; and
- feature/model configuration version.

### 12.4 Scores and shortlists

**Provisional paths:** processed/modeling/scores.parquet and
processed/modeling/shortlists.parquet.

Scores require event/time, method, model/config version, out-of-fold score,
rank, selected flag, budget, and selection validity interval. Random methods
also require replicate ID and seed.

Shortlists require the selector decision plus realised matched costs in event,
market, token, subscription, and time units.

### 12.5 Canonical opportunity ledger

**Provisional path:** processed/backtest/opportunity_ledger.parquet
**Grain:** one unique qualifying spell independent of selector.

Required fields include stable spell ID, event, start/end, size, displayed
value, availability timestamp, and eligibility. This table prevents one spell
from being counted repeatedly across positive label rows.

### 12.6 Trade ledger

**Provisional path:** processed/backtest/trade_ledger.parquet
**Grain:** selector/run/scenario × attempted unique spell.

Required fields:

- selector, model version, random replicate/seed, fold, and budget;
- detection and delayed entry times;
- execution scenario and availability clock;
- each basket leg, requested/filled shares, price, and depth consumed;
- partial-fill/failure and unwind reason;
- fees, merge result, merge latency/failure;
- capital reserved and released;
- gross P&L, net P&L, and failure code; and
- stable link to the canonical opportunity spell.

### 12.7 Backtest summary and frozen manifest

**Provisional path:** processed/backtest/backtest_summary.parquet
**Grain:** method/run/fold/budget/execution scenario.

Required summary fields include cumulative/net P&L, monitored event/token
hours, unique spells/value captured, capital utilisation, maximum exposure,
ROI, drawdown, monitoring cost, and model-minus-random comparisons.

The final evaluation also requires a committed or hashed manifest recording:

- data and code hashes;
- clock and eligibility rules;
- feature/model versions;
- cadence, shortlist duration, budget, and cost unit;
- size, fee, latency, partial-fill, unwind, and merge assumptions;
- headline metrics and uncertainty method; and
- lockbox access record.

## 13. Reproducibility notes

- event_universe selection has a retained, runnable verifier.
- Stage 1 group panels and Stage 2 labels are independently reviewed according
  to project notes, but the separate verifier code/reports are not retained.
- Pair outputs and the earlier mirror-book verification artifact are absent.
- requirements.txt currently contains the data stack only; modelling,
  plotting, and testing dependencies must be added when those stages exist.
- Update this dictionary whenever a stored schema, eligibility definition,
  timing convention, or economic ledger changes.
