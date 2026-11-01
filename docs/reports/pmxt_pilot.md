# PMXT data-adapter pilot

Checked 29 October 2026. This is a separate acquisition and reconstruction
experiment. The completed Dome research, data, models and final-test results are
unchanged: **2,913 protected files passed a before/after SHA-256 comparison**.

This report preserves the initial receive-time smoke test. The later
[source-time validation](pmxt_validation.md) evaluates the agreed source-clock
scenario, expands the cache to six hours and qualifies how full-book differences
should be interpreted. Its results are separate from the figures below.

## Completed smoke test

The frozen design proposes a development-only week, **4 May 2026, 01:00 UTC to
11 May, 01:00 UTC**, plus one warm-up hour. The completed run covers only
**4 May, 00:00–02:00 UTC**: one warm-up hour and one analysis hour. The full week
has **not** been downloaded or evaluated. All endpoints are excluded.

| Measure | Observed |
| --- | ---: |
| Selected events | 10 negRisk; 5 ladder |
| Required tokens / supported buy baskets | 200 / 301 |
| Selected messages across both hours | 435,971 |
| Complete event-hours out of 15 analysis examples | 2 |
| Fraction of event-time with every supported basket observable | 73.47% |
| Tokens usable at end | 178 / 200 |
| Qualifying conditional gross opportunities observed | 0 |
| Remaining hourly labels | 13 unknown |
| Malformed messages | 0 |
| Batches failing best-quote consistency | 13,077 |
| Extracted raw Parquet size | 4.89 MB |
| Recorded successful extraction range bytes | 718.61 MB |
| Identifier-discovery range bytes | 202.99 MB |

The two complete examples are `bitcoin-vs-gold-vs-sp-500-in-2026` and
`chi1-edv-ccu-2026-05-22`. No positive was observed in the usable states of the
other events, but their missing intervals prevent assigning negative labels.
This does **not** establish that the market had no opportunities or that the
strategy earns zero profit. No execution or profitability estimate was made.

The mismatch count includes repeated checks while a book remains inconsistent;
it is not a count of independent outages or missing updates. Some same-source
millisecond messages contain conflicting intermediate best quotes. The checker
accepts a compatible reported best within that source millisecond, rather than
treating arbitrary Parquet row order as true exchange order. Other inspected
cases contain a reconstructed price level inconsistent with later reported best
quotes. Those states remain unknown until a consistent full snapshot arrives.
This identifies unresolved stream/reconstruction continuity questions, not proof
that PMXT alone caused each discrepancy.

Price-change source-to-receive delay is **619 ms at the median, 5.50 seconds at
the 90th percentile and 12.63 seconds at the 99th percentile**. Full snapshots
have a median source-clock age of 26.19 seconds. Snapshot age is distinct from
network delay: a full book may retain an older source timestamp. Both clocks are
preserved. A future 100 ms execution scenario would start at the selected
observation clock; it cannot be described as 100 ms from the original exchange
update without accounting for these delays and the archive's batching.

## Selection and provenance

The [PMXT v2 archive](https://archive.pmxt.dev/docs/v2-data-overview) supplies
public hourly Parquet files of books, absolute level updates, trades and tick
changes. The pilot first read identifiers and timestamps from the warm-up file,
without prices, to establish token presence. Its frozen universe was selected
before extracting prices.

The existing, **incomplete** local metadata catalogue supplies candidate events.
NegRisk candidates require explicit non-augmented metadata, 2–12 members,
nominal overlap through the pilot week, constituent listing dates before warm-up
and all required YES tokens in the identifier inventory. Selection uses a fixed
hash and sector round robin. It is a small engineering sample, not a population
sample. Current public Gamma metadata then verifies structural membership,
tokens and condition IDs with the existing strict universe detector.

The target was 10 events of each type. Only five ladder events in this catalogue
had supported pairs with both tokens present in the warm-up hour: the May
Bitcoin, Ethereum, Solana, XRP and Dogecoin price ladders. A missing unrelated
threshold does not invalidate an independently supported two-leg pair. This
shortfall is not evidence that Polymarket has only five ladder events.

Gamma fields retained locally exclude outcome prices, winners and volume.
Rules and membership are **current metadata used retrospectively under a fixed
membership/rules assumption**, not verified point-in-time history. Current fee
schedules, minimum sizes and ticks are saved with that qualification. They are
not backdated as historical facts.

## Reconstruction and diagnostic assumptions

- Availability follows `timestamp_received`. Source timestamps order revisions;
  they do not make a message available before it was received.
- A level update replaces its size; zero removes the level. Trades do not
  directly decrement book depth.
- A delayed full snapshot is rebased using newer level revisions already
  received, including deletion tombstones. Older revisions cannot overwrite
  newer versions of the same price level.
- Same-receipt-millisecond updates are applied before evaluating an event.
  Reported best quotes check consistency but never supply invented depth.
- A book needs a full baseline with source time at or after capture began.
  Left-censored seeds and inconsistent/malformed states remain unknown.
- Missing archive files stop the audit at the contiguous cached prefix. A quiet
  book is allowed to carry forward within that prefix; there is no quote-age or
  persistence cutoff. File presence and matching best quotes do not prove that
  every deep-level update was recorded; there are no exchange sequence numbers.
- Conditional quote sizing uses minimum five shares, an initially assumed
  0.0001 tick until an observed tick change, zero fees and uncapped capital/shares.
  Displayed depth still limits size. Every leg must have a clean book, and both
  integrated cost and worst-limit cost must leave positive margin.

These quote checks are a **conditional zero-fee diagnostic**, not a verified
definition of executable PMXT arbitrage. The [current Polymarket fee
documentation](https://docs.polymarket.com/trading/fees) has category-dependent
taker fees. Historical fee parameters and exchange minima/ticks need evidence
before net opportunity labels or economic replay are used for research.
Current Gamma schedules and trade-message fee fields alone are not sufficient
to establish a complete historical fee regime.

Hourly value is the maximum qualifying displayed dollar margin of **one basket**
in that hour. Repeated quotes and overlapping ladder pairs are not summed into
collectible profit. A positive can be witnessed during partial coverage;
assigning zero requires the complete hour. Fee-net status remains unknown.

## Reproduction and files

Activate the repository environment first. Discovery and downloading need public
network access; audit and verification are local. No API key is needed.

```powershell
python -m src.data.pmxt_pilot discover
python -m src.data.pmxt_pilot download --hours 2 --transfer-gb 2
python -m src.data.pmxt_pilot audit
python -m src.data.pmxt_pilot verify
```

The default run is `data/pmxt/pilot_v1`. Repeated discovery returns its frozen
design. Downloads reuse hash-checked extracted files and record archive URLs,
ETags, selected assets and transfer bytes. Requests require exact HTTP 206 ranges
and a stable object identity; there is no whole-file fallback. Explicit path
guards reject writes outside the separate PMXT output roots. Missing or damaged
files fail visibly. Per-invocation transfer limits stop further requests; the
reported bytes exclude aborted requests, HTTP overhead and earlier discarded
work, so they are not a complete network billing counter.

| Code | Responsibility |
| --- | --- |
| `src/data/pmxt_archive.py` | Range reads, extraction, hashes and Dome preservation |
| `src/data/pmxt_pilot.py` | Candidate selection, metadata, frozen plan and CLI |
| `src/data/pmxt_books.py` | Causal depth reconstruction and quality checks |
| `src/data/pmxt_audit.py` | Hourly features, coverage and conditional quote audit |
| `notebooks/12_pmxt_pilot.ipynb` | Verify and display saved findings; network work opt-in |

`data/pmxt/pilot_v1/` contains the plan, metadata, selected raw messages,
`hourly_features.parquet`, `hourly_quote_audit.parquet` and an audit-end book
checkpoint. Raw messages are authoritative for replay; the checkpoint is not
an automatically resumed simulator state. The feature table currently contains
adapter diagnostics: package/token counts, usable/clean fractions, quoted gap,
spread, depth, receive age and conditional snapshot value. It is **not yet the
full feature schema expected by the saved Dome models**. Labels and diagnostic
columns must not be passed as training features.

`results/pmxt_pilot/pilot_v1/` contains `summary.json`, per-event-hour diagnostics
and token quality counters. Summary hashes cover the frozen inputs, derived
tables and adapter implementation. Notebook 12 checks those hashes, displays the
actual interval and re-verifies Dome preservation by default. It fits no models.

## Decision before expansion

Acquisition, compact caching and a causal audit work end to end. The smoke test
does **not** yet pass the gate for replacing the Dome experiment: complete-hour
coverage is limited, stream discrepancies need further diagnosis, and historical
fee treatment is unresolved. The small interval also says little about the
prevalence of opportunities. No new final test was accessed or model selected.

At the two measured hours' rates, the 169-file week would require roughly
**61 GB of extraction transfer** while saving about **0.41 GB of selected raw
messages**. These are extrapolations; activity and file sizes vary. An explicitly
budgeted continuation is available:

```powershell
python -m src.data.pmxt_pilot download --hours 169 --transfer-gb 65
python -m src.data.pmxt_pilot audit
python -m src.data.pmxt_pilot verify
```

The full-week transfer was not started automatically after the quality findings.
Resolve the clock/continuity and historical parameter questions before committing
to months of collection, full model-feature alignment, fresh chronological folds
and a newly reserved untouched final period. Preserve the Dome study as the
completed baseline regardless of the pilot's eventual outcome.
