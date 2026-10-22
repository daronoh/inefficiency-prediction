# Dataset

The source is saved Dome order-book history with frozen Polymarket metadata.
All analytical reads use the development interval
`2025-10-14T00:00:00Z <= time < 2026-01-08T00:00:00Z`.
No final-test rows are used to construct examples or inspect opportunities.

## Discovering negRisk and ladder groups from metadata

The frozen study remains reproducible through its original audited manifest.
For a new universe, `src.data.universe_detection.detect_universe` accepts market
and event DataFrames and returns classifications, validated pairs, basket
definitions and review reasons. It uses no prices, volume, realised winners or
actual closing times. Discovery is independent of the ranking model and latency.

The market input supports canonical rows or Gamma-shaped records, including
JSON-encoded `outcomes`, `clobTokenIds` and nested `events`. Token mapping uses
the YES/NO labels rather than assuming their array order. Multiple parent events
require an explicit choice. Canonical fields are:

| Field | Requirement |
| --- | --- |
| `market_id`, `event_id`, `event_slug` | Stable market and parent-event identities; distinct events must have distinct ranking slugs |
| `question`, `description`, `resolution_source` | Full contract wording, including exceptional resolution provisions |
| `yes_token`, `no_token` | Unique outcome-token IDs |
| `listed_at` | Time trading became possible, with explicit timezone |
| `metadata_available_at` | Earliest known availability of this version of the rules and membership |
| `valid_until` | Optional exclusive end of this metadata version's validity |
| `neg_risk`, `neg_risk_market_id` | Platform flag and shared negative-risk contract identity |
| `contract_year` | Optional explicit year evidence for titles without a year; never inferred from actual closing time |

Use one metadata version per market/event per call. Split revised metadata into
separate validity intervals before calling. The default `point_in_time` mode
requires availability timestamps and prevents baskets starting before the metadata
was available. `frozen` mode explicitly assumes the supplied retrospective rules
and membership; it is not evidence of historical availability. The supplied
analysis window bounds every package and can enforce a train/test boundary.

For negRisk, supply a separate event inventory with `event_id`, `neg_risk`,
`neg_risk_augmented`, `neg_risk_market_id`, `member_market_ids`,
`metadata_available_at`, optional `valid_until`, and `complete_set_verified`.
The last field is an explicit upstream verification that the inventory is
exhaustive under the contract rules. It is **not** inferred from a winning outcome,
market count, or the flag alone. Missing verification, mismatched membership,
unavailable legs or augmented outcomes retain the negRisk classification but
produce no executable complete basket. An embedded event summary alone generally
does not supply this evidence.

For ladders, the detector compares contracts **within the same parent event**.
It recognises full rule templates for monthly exchange-candle reach/dip,
launch FDV, public-sale commitments, OPM shutdown deadlines, and minimal explicit
UTC fixed-measurement/occurrence contracts. These are supported grammars, not a
general natural-language reasoner. Sources, measurement periods, comparators and
fallback clauses remain part of the compatibility signature. Every compatible
pair with distinct thresholds is generated; range bins and unmatched wording are
excluded. A template match without a compatible partner is marked `unpaired`.
Adding a grammar requires new positive and adversarial tests and a version bump.

```powershell
python -m src.data.universe_detection --markets markets.jsonl --events events.jsonl --start 2025-10-14T00:00:00Z --end 2026-01-08T00:00:00Z --output data/processed/universe_detection/run1
```

Parquet and JSON arrays are also supported. The output directory must be new:

| Output | Meaning |
| --- | --- |
| `events.parquet` | Event classification, eligibility, package count and reason |
| `markets.parquet` | Per-market template, rule signature, threshold, validity and review reason |
| `pairs.parquet` | Broader YES / narrower NO ladder relationships |
| `packages.parquet` | Complete token baskets with exclusive end times |
| `design.json` | Version, input/source fingerprints, assumptions and output hashes |

`DetectionResult.read(folder)` checks output integrity. Use
`detection_plans(result, path_for_token)` to map packages to raw book files, or
`build_plan('all', detection=result)` for the repository's local file layout.
Missing histories exclude complete baskets or affected ladder pairs; a required
leg is never removed. Detection alone does not prove book coverage or profits.
The existing notebook training/replay defaults still use the frozen cohort;
new manifests are explicit inputs for a new dataset/platform integration, not
an automatic replacement for the current models.

For a new acquisition, `src.data.catalogue` downloads the public Polymarket event
catalogue with cursor pagination and applies the same detector. It includes open
and closed events with nominal dates overlapping the requested interval, including
older listings. Only identifiers, contract text, token mappings and metadata dates
are saved; prices, volume and realised outcome values are discarded.

```powershell
python -m src.data.catalogue fetch-partitions --output data/raw/catalogues/march_august_2026_partitioned --start 2026-03-01T00:00:00Z --end 2026-09-01T00:00:00Z --final-start 2026-08-01T00:00:00Z --max-pages 10000
python -m src.data.catalogue analyse-partitions --output data/raw/catalogues/march_august_2026_partitioned
```

The interval is half-open: March through August, with August reserved for final
evaluation. Discovery inspects metadata only, including that month's contract
definitions. This proposed acquisition does not change the Dome baseline's dates.
Frozen pages have hashes and resumable cursors. `discovery/events.csv` lists
negRisk candidates and supported ladders; `ladder_pairs.parquet` contains the
validated relationships, and `request_tokens.parquet` identifies books to request.
NegRisk flags do not automatically verify exhaustive historical membership.
Current nominal dates and rules are not point-in-time evidence; missing dates or
historical revisions may cause omissions. FinData book coverage, provenance,
fees and observation timing require separate verification before training.
An interrupted catalogue is not marked complete. Explicit `--allow-partial`
analysis writes `discovery_partial/` and identifies unfinished partitions; its
counts are lower bounds on the queried catalogue.

`src.data.findata.FinData` reads `LUMID_TOKEN` from the environment after local
environment loading. Its SELECT-only client reads materialized JSONL results,
checks row counts and excludes credentials and signed URLs from audit receipts.
Warehouse sources, including PMXT, are assessed separately for book semantics,
capture timing, continuity and duplicate liquidity. A provider name alone neither
qualifies nor disqualifies a recording. Discovery and coverage inventories contain
no opportunity or P&L targets and do not make final-test data available to training.

## Inputs and outputs

| Location under `data/` | Contents |
| --- | --- |
| `raw/orderbooks/ob_<token>.parquet` | Original token histories |
| `raw/ladder_orderbooks/ob_<token>.parquet` | Ladder collection; preferred for ladder tokens when both collections contain a file |
| `raw/polymarket_markets_1y.jsonl` | Frozen market descriptions, tokens and exact fee flags |
| `metadata/markets.parquet`, `tokens.parquet` | Parsed contract descriptions and YES/NO token mapping |
| `metadata/event_universe.parquet` | Frozen original event/market membership and grouping |
| `metadata/fee_schedule.parquet`, `universe_token_ids.parquet` | Original acquisition/fee metadata; active fee checks use exact frozen market flags |
| `processed/features/` | Original scheduled features and eligibility; input to the profitability rebuild |
| `processed/profitability/examples.parquet` | Current snapshot features, net-P&L/activity/occurrence targets and coverage |
| `processed/profitability/events/<event>/` | Rebuilt signals and standalone-window attempts |
| `processed/profitability/design.json`, `complete.json` | Execution contract, input hashes and completion check |
| `processed/labels/` | Original capped occurrence-label reference and fee evidence |
| `processed/profitability_runs/`, `profitability_inputs/` | Additional target or changed-window builds |

Files in `results/` contain model predictions, metrics, fitted pipelines and policy
replays. They are outputs, not predictors. The notebook run manifests identify
the selected dataset and model run. Data paths in input inventories are relative
to `DATA_ROOT`; the default is the repository's `data/` directory.

In the profitability design manifest, `contract` preserves the inherited
scheduled-input settings; `execution` is authoritative for the new arrival delay
and uncapped sizing. `profit_setup()` defines these overrides explicitly.

`processed/labels/fee_evidence.csv` records the exact frozen fee flags checked
for the selected markets; all are false. Combined with the
[dated fee rollout](https://docs.polymarket.com/changelog/predictions), these
support zero exchange trading fees for the development sample. The January
2026 rollout covered 15-minute crypto markets, excluded from this baseline.

## Raw book records

A token represents one outcome of a binary contract. Each Parquet row is a
snapshot at that token's own timestamp; token histories are asynchronous.

| Field | Meaning |
| --- | --- |
| `token_id`, `asset_id` | Outcome-token identifiers |
| `snapshot_time`, `snapshot_timestamp_ms` | Source time in UTC / Unix milliseconds |
| `indexed_at_time`, `indexed_at_ms` | Provider indexing time, used to order duplicate source timestamps |
| `market_hash` | Provider market identifier |
| `tick_size`, `min_order_size` | Permitted price increment and minimum share quantity |
| `orderbook_neg_risk` | Provider's negRisk flag |
| `asks_json`, `bids_json` | JSON arrays of price/size levels |

For example, `asks_json = [{"price":"0.40","size":"25"}]` means 25 shares
are offered for purchase at $0.40. Bids are orders into which owned tokens can
be sold. Prices lie in [0,1]; quantities are shares. Levels are sorted and
cumulative depth is calculated before quoting a trade size.

An explicit `[]` is a recorded empty side. Null/malformed data is unknown.
A clean entry book has both sides and a best bid no greater than its best ask.
Minimum-size and tick metadata must also be usable.

## Temporal alignment

1. Filter to development rows, sort by source timestamp and provider index,
   and keep the latest indexed row at a duplicate source timestamp.
2. For each basket, form the union of member update times. At a time t, choose
   the last row at or before t for each member: a **backward-as-of lookup**.
3. Preserve invalid or empty states. Never skip an invalid row to reuse an older
   valid one; never interpolate or use a later observation to fill a gap.
4. For labels and fills, stop each token at its own final recorded timestamp.
   A pair may use a pre-activation observation as its initial state, but entries
   must respect the audited contract window.

Illustrative alignment at 10:00:00:

| Token | Last source time | Ask | Age |
| --- | --- | --- | --- |
| A | 09:59:55 | $0.40 | 5 seconds |
| B | 09:59:59 | $0.50 | 1 second |

The aligned state combines these preceding rows; it does not claim they were
simultaneously observed. Source-time alignment does not measure provider latency.
`align_books` exposes a long table with `time`, `token_id`, `source_time`, age,
availability, clean-book flag, best prices and depth. The full pipeline aligns
size-dependent cost/limit arrays in memory rather than storing a huge expanded
copy of every order-book level.

Scheduled predictors are sampled separately at t, t-W, and so on. Eligibility
requires the relevant books to have been seen by t; it does not consult future
recording endpoints. Thus an event may enter a ranking even when its following
window is too poorly recorded to score. Only the selected events' intervening
updates would be monitored by the proposed policy. Offline histories for other
events are used solely to build labels and evaluate missed opportunities.

## Features: one row per event and scheduled scan

The feature table has `event_slug`, `stratum`, `time`, `eligible_snapshot` and
33 predictors. The following 11 current values are aggregated across active
baskets and their unique tokens:

| Predictor (`x_` prefix) | Definition |
| --- | --- |
| `active_packages` | Number of active complete baskets or audited ladder pairs |
| `active_tokens` | Number of distinct required tokens |
| `seen_fraction` | Fraction of tokens observed by the scan |
| `clean_fraction` | Fraction with two-sided, noncrossed books |
| `spread_mean`, `spread_max` | Mean and maximum best-ask minus best-bid spread |
| `ask_depth_sum` | Total displayed ask shares across unique tokens |
| `book_age_max_s` | Age of the oldest contributing observation |
| `best_quoted_net_usd` | Best displayed dollar margin over active baskets and all feasible integer quantities, without artificial size/cash caps; can be negative |
| `size_feasible_package_fraction` | Fraction of baskets with a finite size-qualified quote |
| `positive_quoted_net_package_fraction` | Fraction with positive quoted margin |

Each also has `x_snapshot_lag1_*` and `x_snapshot_change1_*` versions, adding
22 features. Lags are missing at the first scan or after a missing scheduled
scan; they never cross events or gaps. Quoted margin is not realised profit
and does not perform every execution check. There are no event identifiers,
final outcomes, future volume, labels or intrawindow rolling features in X.

## Event metadata comparison

Metadata is joined when constructing modelling examples. Classifiers, direct
regressors and two-stage models compare book-only and combined inputs using
identical rows and chronological folds. Earlier metadata-only comparisons are
retained in the historical baseline results.

| Field | Definition and availability |
| --- | --- |
| `m_event_type` | negRisk or ladder, from the declared event universe |
| `m_topic` | A fixed broad topic inferred from the event name; unmatched names map to `other` |
| `m_age_days` | Scan time minus the earliest frozen constituent market's `created_at`, in days; negative or unavailable ages remain missing |

Topics are tweet counts, central-bank decisions, shutdowns, crypto, elections,
sports, geopolitics, technology/business, culture, and other. Event names are
used only to assign these categories; names and identifiers never enter X.
Creation timestamps are treated as stable historical facts. This age is not
time since a verified event listing; frozen membership retains the original
cohort's retrospective limitation. Age can also identify repeated events
indirectly, so chronological and held-out-family results are both reported.

The generic `end_date` values cannot establish the deadline known at a past
scan and are excluded, along with actual closing times, final status, outcomes,
lifetime volume and text embeddings. The metadata comparison has three input
fields; combined models have 36 before encoding. Categories are one-hot encoded
using training rows only, with unseen categories mapped to all-zero indicator
blocks. Numeric preprocessing is also fitted on training rows only. Metadata
does not receive book-style lag features.

## Observation-only labels: the current prediction layer

`processed/opportunities/examples.parquet` reuses the exact scheduled snapshot
features and eligible population, but rebuilds labels from raw books without
running a trading scheduler. `processed/opportunity_value/examples.parquet`
adds a dollar-magnitude target to those rows. Neither table contains simulated
P&L, fills, arrival delays or unwind outcomes as learning targets.

| Column | Meaning |
| --- | --- |
| `label_known` | Every active basket's entry status is decidable throughout the window |
| `target` | At least one depth-qualified quoted basket exists in `[scan, scan + W)`; 1/0 when known, otherwise missing |
| `opportunity_at_scan` | A qualifying basket is already visible at the scan |
| `future_positive` | A new episode has an observed onset strictly after the scan, in a known window |
| `witnessed_opportunity` | A candidate was observed, including in incompletely covered windows; diagnostic only |
| `value_target` | Maximum qualifying quoted dollar margin of one basket at one state within a known window; zero if absent, missing if coverage is incomplete |
| `witnessed_peak_quoted_usd` | Largest observed margin regardless of coverage; diagnostic only |

A qualifying basket has clean member books, valid ticks and minimum order sizes,
sufficient displayed depth for at least five shares, and strictly positive gross
and worst-limit-price margins. There is no artificial maximum size, cash cap,
minimum duration or quote-age cutoff. Qualification uses the cohort's zero-fee
assumption. Changes to source-time alignment, recording continuity or contractual
membership can still affect labels; independence from execution does not make
these assumptions disappear.

The value target is `max over states and baskets of (q - displayed purchase cost)`
over quantities passing all qualification checks. It is **not total hourly
profit**: it counts neither repeated snapshots nor simultaneous overlapping pairs
as additional money. Even maxima in successive hours may refer to the same
unchanged liquidity. Their sum is a weighting measure for ranking evaluation,
not an economic return. `package_peaks.parquet` records the peak time, quantity,
margin and number of checked states for each package-window.

The development table contains **26,835 eligible rows across 48 events**:
**454 positive**, **21,373 negative** and **5,008 unknown**. All 17 hours with
negative net returns under the previous execution simulation have positive
opportunity labels. These labels describe the entry opportunity before an
execution outcome is imposed. Unknown rows compete for monitoring slots, but
neither their witnessed gaps nor their observed quote magnitudes enter fitting.

Classifiers learn occurrence. Direct value regressors learn the nonnegative peak
margin. Two-stage models learn `P(opportunity | X)` and
`E(peak margin | opportunity, X)` separately, then multiply. Their probability
gates use unweighted classes; the conditional amount model fits positive rows
only. Negative amount predictions are clipped to zero. Only the 33 `x_` features
and, where declared, three metadata fields enter X; future witness/label columns
are never predictors. The current dollar-margin feature and its scheduled lag
and change remain available in every feature view.

Outer evaluation begins on 1 November, 22 November and 15 December 2025. Training
contains **4,859 / 11,191 / 17,690** known earlier event-hours with
**69 / 234 / 405** positive opportunity labels. A training window must end
strictly before the next evaluation begins; this purging depends on W, not
execution latency. Two inner chronological validation blocks tune each method
before it is fitted on all eligible earlier training rows. The final test from
8 January 2026 remains closed. Changing W requires matching rebuilt scheduled
features, opportunity labels and model splits.

## Earlier execution-dependent targets

`processed/profitability/examples.parquet` contains one row per `(event_slug,
time)`. `load_profitability` retains snapshot-eligible rows, including unknown
targets. Features whose values depend on sizing are recomputed from uncapped
depth; all scheduled lags and changes are rebuilt afterward.

| Column | Meaning |
| --- | --- |
| `coverage_fraction` | Fraction of the window where every active basket's entry status is decidable |
| `positive` | At least one positive simulated attempt was witnessed |
| `label_known` | Complete-window coverage and no unknown attempted executions |
| `target` | 1 or 0 when known; missing otherwise |
| `pnl_target` | Net simulated P&L from all attempts initiated in the window, including losses; missing unless fully evaluable |
| `activity_target` | Whether the window contains at least one attempted trade; missing unless fully evaluable |
| `future_positive` | A positive attempt from a gap first observed strictly after the scan, with an observed onset |
| `attempts`, `unknown_attempts`, `partial` | Standalone-window execution counts |
| `known_pnl_usd`, `cash_usd` | Standalone accounting diagnostics, not additive portfolio returns |

A recorded blocker in any required leg rules out that basket under the declared
policy. Otherwise all legs must be observed. Every active basket must be decidable
for the whole event-window to qualify. Inactive contract time needs no evidence.
This distinguishes known no-entry states from missing data. Positives and
negatives use the same completeness requirement for training and model scoring.

The prepared profitability table contains 31,345 rows across 48 events;
26,835 rows are snapshot-eligible. Of these, 21,827 are usable labelled examples
(437 net-positive, 21,373 zero and 17 net-negative), and 5,008 remain unknown. The modelling table retains the unknown
rows so they still compete for monitoring slots.

There are 454 known trading hours and 444 hours containing at least one positive
attempt. Occurrence and net profitability are different targets. The earlier P&L
target was rebuilt using 100 ms arrival and uncapped depth; the original capped
500 ms labels remain a historical reference. Targets are generated for every
window, independently of model selections. Attempts' later unwind results are
assigned to their initiating window, and split purging covers those exits.

| Evaluation interval (UTC) | Earlier training examples | Training hours with attempts |
| --- | --- | --- |
| 1 to 21 November 2025 | 4,859 | 69 |
| 22 November to 14 December 2025 | 11,191 | 234 |
| 15 December 2025 to 7 January 2026 | 17,690 | 405 |

Each fit uses only known targets earlier than its evaluation start minus W and
1.1 seconds. Models are fixed within a fold but rank events at every scan.
Median imputation, missingness indicators, signed-log transformation and scaling
are fitted on the training subset. The book-only baseline excludes topic.
Metadata variants include broad topic; evaluation families additionally retain
separate groups for unmatched events. See [the simulator guide](simulator.md) for how
attempts produce targets and P&L.

Direct regressors fit signed dollar targets on all known hours. Two-stage gates
fit the binary activity target without class weighting. Their conditional
regressors fit only known hours containing attempts, including negative and zero
net outcomes. Feature imputation and encoding for each stage use that stage's
training rows. Targets are fitted in dollars; no log-target inversion is used.

## TabM preprocessing extension

Notebook 08 uses the same frozen opportunity labels and the same 33 book or
36 combined input columns as notebook 06. Within each fit, TabM imputes continuous
numeric columns with training medians and applies a training-fitted normal
quantile transform. Binary columns are imputed without scaling. Metadata
categories are one-hot encoded; unseen categories map to all zeros. Missing
values and infinite numerical values are treated as missing. No validation or
evaluation rows determine these transforms.

Dollar regressors standardize their targets with the fitting rows' mean and
standard deviation, then invert that transformation before clipping predictions
to zero. Conditional amount models fit only positive training windows and learn
their own preprocessing. The occurrence gate uses unweighted binary labels.
Neither execution returns nor latency enters these targets. Full protocol and
results are in [the TabM report](reports/tabm.md).
