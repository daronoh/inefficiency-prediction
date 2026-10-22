# FinData discovery and recording coverage

This audit assesses whether FinData can support a larger event-ranking
experiment. It does **not** establish executable arbitrage or profitability.
The existing Dome dataset, models and results remain unchanged.

The proposed interval is **1 March to 1 September 2026, UTC, end exclusive**:
six calendar months, March through August. March–July are development data;
August is reserved for the final test. This audit examines metadata,
identifiers, source provenance and recording timestamps/counts only. August
prices, opportunities, returns and model performance have not been evaluated.

## Source policy

PMXT is eligible for assessment alongside other warehouse sources. An earlier
PMXT exclusion was a project preference, not evidence that its data was
unsuitable. FinData is the access layer; its records can originate from
different collection systems.

No source name automatically qualifies a recording for replay. Acceptance
requires interpretable book levels and quantities, correct token mappings,
timestamp semantics, verified snapshot/update handling, and adequate recording
coverage for every required leg. Preserve source identity and avoid counting
duplicate captures as independent observations. Combining sources requires
explicit reconciliation of timestamps and book states.

Sparse periodic snapshots can describe the market at sampling times without
supporting precise execution outcomes 100 ms later. Conversely, a quiet period
in a verified complete update stream is not automatically missing data.
Capture completeness, rather than an arbitrary quote-age cutoff, matters.

## Catalogue discovery

The public Polymarket Gamma catalogue supplies event and market metadata.
Pages retain contract text, identifiers, token mappings, structural flags and
dates; price, volume and realised outcome values are discarded.

| Discovery result | Count |
| --- | ---: |
| Events scanned | 694,736 |
| Constituent market records scanned | 2,443,894 |
| negRisk-flagged events | 65,173 |
| Explicitly non-augmented negRisk events | 60,606 |
| Augmented negRisk events | 4,567 |
| Events matching supported ladder templates | 42 |
| Supported ladder pairs | 2,277 |

These are structural candidates, not verified historical trading universes.
A negRisk flag does not prove that the collected outcomes form an exhaustive
basket. Current metadata also does not establish which members were available
at each historical decision time. Nominal end dates are not verified trading
lifetimes.

The ladder detector accepts conservative templates for monthly crypto price
thresholds, launch valuations and sale commitments. It does not attempt to
infer every possible relationship between arbitrary contract descriptions.

**The catalogue is incomplete.** The June listing partition stopped after
92,200 events when its pagination request returned HTTP 403. Other partitions
completed. Outputs are explicitly marked partial; the counts are provisional
and must not be presented as an exhaustive market census.

## Warehouse inventory

Authenticated read-only FinData queries succeeded. The inventory groups
snapshot rows by month, source and token, saving counts and first/last
recording times. Failed large queries were replaced with smaller bounded
intervals. The following are observed inventories for the inspected sources.

| Month | Warehouse source | Distinct tokens | Snapshot rows |
| --- | --- | ---: | ---: |
| March | PMXT | 2,224 | 4,874,933 |
| April | PMXT | 5,029 | 2,141,425 |
| May | `polymarket_ws` | 724 | 161,950,900 |
| June | `polymarket` | 877 | 752,555 |
| June | `polymarket_ws` | 798 | 4,231,028 |
| July | `polymarket` | 733 | 1,759,389 |
| July | `polymarket_ws` | 600 | 1,536,297 |
| August | `polymarket` | 357 | 1,970,856 |
| August | `polymarket_ws` | 502 | 262,180 |

There are 6,730 distinct token IDs across this inventory. Counts cannot be
added across sources or months to obtain a distinct-token total.

The original inventory queries selected PMXT in March–April and
`polymarket_ws`, `polymarket` and `predexon` in May–August. Full-month source
summaries confirm PMXT was the only recorded source in March and April. A
follow-up checked every May–August interval for any other source, including
null source labels, and found zero rows. May's full-month query timed out;
two adjoining half-month queries completed. The source restriction therefore
did not omit additional records at the time of that audit. Evidence is saved
under `source_scope_audit/`. This checks the queried snapshot table, not
unadvertised archives or other provider products.

The snapshot table has no storage chunks covering **16 April–21 May**.
Within the inspected inventory, April PMXT recordings end on 15 April;
May websocket recordings begin on 22 May. Including PMXT therefore adds early
history but does not establish six continuous months of observations.

Recording intensity also changes substantially. In sampled one-hour windows,
March PMXT averaged about 13 rows per token, July REST about 12, and May
websocket about 1,642. June websocket averaged only about 7 in its sampled
hour. These are sample counts, not measured execution latency or proof of
complete capture. Source changes could change labels and apparent model
performance independently of market behaviour.

## Matching contracts to recorded tokens

Matching the discovered catalogue against the inventory gives:

| Necessary coverage condition | Observed matches |
| --- | ---: |
| Candidate events with at least one required token recorded | 672 |
| Non-augmented negRisk candidates with every listed YES token in the same source/month | 19 |
| Supported ladder events with both pair tokens and overlapping recording spans in the same source/month | 1 |
| Supported ladder pairs satisfying that condition | 1 |

The matched ladder pair belongs to the May 2026 Bitcoin price event, using
the broad-threshold YES token and narrow-threshold NO token. This does not
establish coverage for other execution routes requiring additional tokens.
Likewise, the negRisk count concerns listed YES baskets, not every NO or
complement route.

### How the 19 negRisk matches were calculated

A follow-up audit reproduced the count from the saved Parquet tables, without
querying prices or evaluating returns:

1. Select the 60,606 events classified as negRisk with augmentation explicitly
   false. Of these, 60,602 have at least one extracted YES token identifier.
2. Extract the listed YES token IDs, giving 397,769 distinct required IDs
   across this candidate universe. Match exact token-ID strings against the
   6,730 distinct IDs in the saved warehouse inventory.
3. Only **159 non-augmented events** have even one matching YES token.
4. Require at least two outcomes, one unique YES token per listed market,
   no other detector eligibility failure, and every required YES token to
   appear within at least one common source/month. This gives **19 unique
   events**, deduplicated across sources and months.

The earlier figure of 672 events with any recorded required token includes
augmented negRisk and ladder candidates. It is not the corresponding count
for the 60,606 non-augmented events.

Relaxing both the source and month requirements gives 20 complete YES sets.
The additional event is Spain–Saudi Arabia on 21 June: one required token
appears only in the April PMXT inventory, while another appears only in June
websocket data. Those records do not establish a concurrent tradable basket.
Relaxing just the source requirement within a month leaves the count at 19.

The 5,000-group query limit did not silently truncate the successful inventory
queries: April PMXT was paginated into 5,000 and 29 groups, and the other
successful query intervals returned fewer than 5,000. The saved May and
August subdivisions join without interval gaps or overlaps within their
queried bounds. The separate source-scope audit found no omitted source
records. Neither check establishes completeness of the public catalogue or
correct historical market membership.

A separate NO-token presence check finds 15 complete multi-outcome NO sets,
all belonging to events already among the 19 YES-set matches. This remains
a presence check, not a verification of an executable complement strategy.

Consequently, 19 must be described as **complete listed-YES token matches in
the inspected inventory**, not as the number of warehouse events with good
order-book data. The event-by-event audit is saved as
`negrisk_token_match_audit.csv` alongside the inventory. The source-scope check
has now been completed. A conclusion about every eligible event still requires
reconciliation with the warehouse's own market metadata and completion of the
interrupted public catalogue.

**Token presence is only a preliminary requirement.** It does not establish
simultaneously usable books, continuous capture, historical exhaustive
membership, active trading, fees or profitable execution.

All 11 April PMXT negRisk matches have at least one required leg represented
by only one snapshot and no common first-to-last recording span. Across all
months and sources, 15 unique negRisk candidates have a common recording span
of at least one hour. Even that weaker span check can hide substantial gaps.

August contains four unique negRisk candidates with all listed YES tokens,
and no supported ladder pair under these checks. One matched event is a
dated June sports fixture whose recordings extend into August, illustrating
why recorded presence must be checked against the actual trading lifetime.
The catalogue remains partial, so these are observed matches, not definitive
upper bounds for the entire warehouse.

## Research implications

### Comparison with the warehouse event table

A separate query against `prediction_markets.polymarket_events`, selecting
`start_date >= '2026-05-01' AND start_date < '2026-09-01'`, reproduced the
following counts exactly: 67,274 distinct events, 3,604 with
`raw->>'neg_risk' = 'true'`, and 63,670 with that flag false. This flag
distinguishes negRisk from non-negRisk; it does not distinguish augmented
from non-augmented negRisk.

| Event start month | Metadata events | negRisk events |
| --- | ---: | ---: |
| May | 67,266 | 3,604 |
| June | 8 | 0 |
| July | 0 | 0 |
| August | 0 | 0 |

The latest start date is 7 June 2026, and all returned records have source
`predexon`. This result does not establish why later listings are absent,
nor does it imply that July/August order books for earlier-listed events are
absent. A filter on start dates also omits events listed earlier but still
trading during the interval.

Of the 3,604 negRisk event IDs, 3,579 match the saved classified public
catalogue: 3,460 are non-augmented and 119 augmented according to the public
metadata. The remaining 25 are not matched in that classified catalogue;
their absence has not been resolved. The warehouse's `market_ids` and the
two inspected augmentation fields are null for all 3,604 retrieved records,
so this event-table export alone cannot verify complete baskets.

Only three of the matched warehouse candidates have any catalogue-required
YES token in the March–August inventory. One of those is non-augmented, and
none of the 3,460 matched non-augmented events passes the complete-YES-set
check. This is an audit of the matched cohort, not a finding that all 3,604
events have been fully mapped or that no arbitrage exists.

These counts concern a different cohort from the original 19: warehouse
events with May–August start dates versus public-catalogue events selected
for nominal overlap with March–August. Event metadata counts do not measure
historical order-book coverage. Saved queries, receipts and the event-ID
comparison are under `warehouse_metadata_audit/`.

### Dataset decision

The available evidence does **not** yet justify replacing the Dome baseline
with a six-month, 100-negRisk/100-ladder experiment. The main obstacle is
recording every required leg during usable trading periods, rather than
finding more event names. With only four preliminary complete-YES candidates
in August, selecting the top five may offer no meaningful monitoring-budget
comparison.

Next steps are to finish catalogue discovery through a working supported
route, verify historical basket membership and trading lifetimes, and inspect
development-period books for the matched candidates. Establish timestamp
semantics, depth completeness, duplicate handling and capture gaps before
generating labels. Assess whether the provider has a fuller historical
archive: no separate Polymarket L2 update table was advertised in the inspected
schema catalogue.

Use a common data-quality requirement across sources, then report coverage
and results by recording regime. A 100 ms replay remains a simulation
assumption unless the observations support reconstructing the relevant book
states at that resolution. If this interval cannot support a competitive
ranking experiment, revise the protocol before evaluating held-out returns;
do not select a replacement test period based on profitability.

## Reproducible artifacts

Local audit evidence is stored under ignored data directories:

- `data/raw/catalogues/march_august_2026_partitioned/`: saved catalogue pages,
  manifests, hashes and `discovery_partial/` outputs.
- `data/raw/catalogues/findata_coverage_march_august_2026/`: bounded-query
  evidence, `monthly_inventory.csv`, `token_inventory.parquet`,
  `event_presence.parquet`, `listed_yes_baskets_present.parquet`,
  `ladder_pairs_present.parquet` and `presence_summary.json`.

The catalogue summary predates the PMXT-inclusive coverage audit; its
non-PMXT limitation describes the earlier acquisition stage. The source policy
and inventory in this report supersede that restriction.

Reusable acquisition code lives in `src/data/catalogue.py` and
`src/data/findata.py`. The latter loads credentials from the environment and
does not forward bearer credentials to external signed-download hosts.
The test suite passed **115 tests** after these additions. No model training,
profitability replay or final-test scoring was performed for this audit.
