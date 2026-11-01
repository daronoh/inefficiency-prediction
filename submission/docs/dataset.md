# Dataset and terminology

An **event** is one item in the hourly ranking, such as a particular Fed decision.
A **market** asks one binary question within that event. A **token** is its YES
or NO share, with its own order book. A **basket** is a validated combination of
tokens whose joint payoff is at least $1 per complete share. A **ladder pair**
contains a broader and a narrower condition. A **candidate event** is simply an
event eligible to enter a particular scan's ranking; it does not imply profit.

A **scan** is the scheduled observation at which all eligible events receive
features and a score. The top N are selected for the following **monitoring
window**, currently one hour. A **signal** is a recorded interval where a basket
passes the full depth-based opportunity test. An **attempt** is the simulator's
order submission. Signals, attempts, fills and profits are different quantities.

## Raw inputs

`inputs/cohort.json` identifies the frozen event membership, token-file paths
and audited basket validity intervals. It contains 46 negRisk events and five
ladder events. It is retrospective contract metadata, not a historical
point-in-time catalogue. Complete-set verification and ladder validity are
assumed under this audited manifest; a negRisk flag alone is insufficient.

`inputs/raw_inventory.csv` lists the 994 required token histories, their first
and last timestamps, sizes and SHA-256 digests. `data/raw/orderbooks/` contains
the corresponding saved Dome parquet files, approximately 1.01 GB in total.
No credentials or active provider account is required for reproduction.

Each token book has these columns:

| Field | Meaning |
| --- | --- |
| `snapshot_time` | Source timestamp used for alignment, in UTC |
| `indexed_at_ms` | Provider index time used to break duplicate-source-time ties |
| `asks_json` | JSON list of ask `{price, size}` levels |
| `bids_json` | JSON list of bid `{price, size}` levels |
| `min_order_size` | Exchange minimum shares for an order |
| `tick_size` | Permitted price increment |

For example, asks `[{"price":0.40,"size":10},{"price":0.42,"size":20}]`
mean buying 15 shares costs `10*0.40 + 5*0.42 = $6.10`. It does not cost
`15*0.40`. An explicit empty side is an observed absence; null or malformed
depth is unknown. Crossed books are treated as unusable evidence.

`inputs/events.parquet` maps events to frozen market members;
`inputs/markets.parquet` supplies member creation dates. Only necessary
descriptive metadata is exported. `inputs/fee_evidence.csv` preserves the
cohort's frozen zero-fee flags; it is not a timestamped fee-change ledger.
`inputs/contracts.parquet` supplies the actual questions, resolution text and
sources for review. `tokens.csv` maps required token IDs to market and side.
`ladder_pairs.csv` and `ladder_contract_audit.csv` preserve audited relationships,
thresholds, validity and description hashes. `baskets.csv` provides a readable
index of the exact baskets used by the pipeline. Metadata verification checks
these frozen routes and text; it does not replace the upstream semantic audit.

## Alignment and eligibility

At scan time t, each active token uses the latest row whose source timestamp is
at or before t. Duplicate source timestamps keep the largest provider index,
with stable ordering. No future row supplies a missing earlier book. Different
tokens therefore need not have identical timestamps after alignment; book age
measures that difference.

An event can enter the ranking once all required active members have been seen,
its basket validity covers the scan, and the scan lies in the offline recording
extent. The recording extent is an evaluation mask, not a predictive feature
or a live deactivation policy. There is no 30-second freshness requirement.

The frozen preprocessing has two contiguous coverage partitions at
8 January 2026 00:00 UTC. Earlier coverage ends at the last recorded state
within that partition. Later coverage uses audited per-token recording ends
and bounded terminal context. This is a preprocessing boundary, independent of
the weekly training/evaluation origins. It is retained in raw reconstruction
to reproduce the exact labels and comparisons, rather than silently extend
earlier unknown windows. Lagged features still use the preceding scheduled
scan across this boundary.

`FYP.data.pipeline.rebuild` reads the raw files and reconstructs features,
signals and labels. It never loads a saved feature or label table as input.

## Prepared examples

`data/processed/examples.parquet` has one row per eligible event and scheduled
scan: 29,308 rows over the full data calendar, of which 23,452 have known
targets and 470 are positive. All times are UTC. Each row includes:

| Group | Columns |
| --- | --- |
| Identity | `event_slug`, `stratum`, `time` |
| Availability | `eligible_snapshot`, `coverage_fraction`, `label_known` |
| Predictors | 33 `x_` features and three optional `m_` features |
| Targets | `target` (0/1 or missing), `value_target` (dollars or missing) |
| Diagnostics | `witnessed_opportunity`, `opportunity_at_scan`, `future_positive`, `witnessed_new_onset`, episode count and first/peak evidence |

`target=1` means at least one qualifying displayed basket exists in
`[t,t+1 hour)`. Both positive and negative targets require the complete active
window to be decidable. An observed blocking leg can rule out a basket even
when another leg is unknown; otherwise all members must be observed. A witnessed
opportunity in an incomplete window remains a diagnostic, with missing targets.

`value_target` is the largest qualifying one-basket dollar margin at any
recorded state during the window. It is a maximum, not a sum over snapshots or
ladder pairs, and is independent of latency and unwinding. For a known negative
it is zero. It is not the profit that can necessarily be earned during the hour.

## Features

The following 11 current features each have a previous-scan value and a
current-minus-previous change, giving 33 book features. A lag is missing when
there is no consecutive preceding scan; it is not filled using future values.

| Current feature | Intuitive meaning |
| --- | --- |
| `x_active_packages` | Number of validated baskets currently active |
| `x_active_tokens` | Distinct member books required by those baskets |
| `x_seen_fraction` | Fraction of active members with a recorded as-of state |
| `x_clean_fraction` | Fraction with valid, nonempty, noncrossed depth |
| `x_spread_mean` | Mean ask-minus-bid spread across clean members |
| `x_spread_max` | Largest member spread |
| `x_ask_depth_sum` | Total displayed ask shares across active members |
| `x_book_age_max_s` | Age in seconds of the oldest carried member state |
| `x_best_quoted_net_usd` | Best integrated-depth basket dollar margin at the scan; can be negative |
| `x_size_feasible_package_fraction` | Fraction of baskets with compatible depth, integer size and ticks |
| `x_positive_quoted_net_package_fraction` | Fraction whose quoted integrated margin is positive |

The quoted-gap rule ranks only by `x_best_quoted_net_usd`, descending. Missing
values receive the lowest score. This feature does not require positive
worst-limit margin; opportunity qualification does. Thus the rule is not an
execution eligibility filter, and it can rank events even when no current
profit exists.

Optional metadata is `m_event_type` (negRisk/ladder), `m_topic` (a fixed broad
text-derived category) and `m_age_days` (days since the earliest frozen member's
creation). Negative or missing ages remain missing. Event identifiers, realised
winners, future volume, status and execution returns are excluded from model
inputs. Topic and membership retain the frozen metadata's retrospective limits.

## Training and result datasets

Every weekly fit filters known rows whose entire target window ends strictly
before the origin. The initial 28-day history grows with time. All eligible
forward rows, including unknowns, receive scores and compete for slots.
Preprocessing is fitted on training rows only.

Saved weekly bundles record feature order, model files, tuning selections and
training cutoffs. Predictions add `model`, `score` and `fold`. Selections add
`scope` and `top_n`; raw future labels never determine which rows are selected.
The simulator exports attempts, scheduler audit rows, event caches and economic
summaries separately. CSV tables and figure indexes are under `artifacts/`.
