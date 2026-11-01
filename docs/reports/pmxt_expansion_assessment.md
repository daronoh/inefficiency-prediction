# PMXT expansion assessment

Checked **29 October 2026**.

## Recommendation

Use PMXT's **v2 historical archive** as the candidate source for a separate,
larger experiment. Keep the completed Dome baseline and its held-out findings
intact. The final Dome test has already been examined and cannot become a fresh
test for newly tuned models.

An initial target of **100–200 supported events** is reasonable, subject to
discovery and complete constituent-book coverage. Concurrent event counts matter:
the design should aim for dozens of eligible events at typical scans, including
the final period. A large total cohort with only a few simultaneous events would
retain the current comparison's limitations. There is no guarantee that expansion
will make ML outperform quoted-gap or increase net profitability.

## Evidence and limits

The [v2 overview](https://archive.pmxt.dev/docs/v2-data-overview) describes hourly
files containing full books and incremental updates, starting 13 April 2026.
It reports that v2 fixed substantial subscription coverage problems in v1.
These are publisher claims, not independent proof of complete histories.
The older January–June proposal cannot be supplied entirely by v2.

Bounded public range requests verified three archive objects:

| File hour (UTC) | Response | Total object bytes | First four bytes |
| --- | --- | ---: | --- |
| 4 May 2026, 00:00 | 206 | 411,088,967 | PAR1 |
| 1 June 2026, 00:00 | 206 | 389,191,137 | PAR1 |
| 1 July 2026, 00:00 | 206 | 531,633,209 | PAR1 |

The requests read four bytes from each file. They establish accessible Parquet
objects on those dates, not continuous coverage, event membership or replay
correctness. The overview page timed out during the direct check; its published
description was available through web search. No books, returns or proposed
final-period labels were inspected in these checks.

Files partition the whole market stream by hour. Targeting 200 events does not
automatically limit transfer to their data. Acquisition needs filtering,
streaming extraction and caching, with a measured storage/download budget.

## Integration work

The existing model comparisons, top-N policy concept and economic accounting are
reusable. The main new work is:

1. Acquire metadata and use the template detector to identify structurally
   supported negRisk baskets and ladders. Select by metadata, date and coverage,
   without filtering for realised profits or winners.
2. Reconstruct per-token depth from book/update messages, including warm-up,
   event ordering, delayed snapshots, deletions, duplicates and gaps. Preserve
   source and receive timestamps and save reproducible local Parquet exports.
3. Feed validated books into equivalent scheduled features and quote-label
   calculations. Distinguish missing histories from valid observed blockers.
4. Verify historical fee parameters for the selected period and markets. The
   [current fee documentation](https://docs.polymarket.com/trading/fees) includes
   category-dependent taker fees; the Dome cohort's zero-fee scenario cannot
   simply be carried into a new period. Qualification and quantity optimisation
   must use the appropriate fee-adjusted basket margin.
5. Rebuild development folds, tune/refit all compared models, and evaluate on a
   newly reserved final month. Choose dates from an archive-coverage audit before
   viewing test opportunities. Preserve 100 ms/uncapped economics as the primary
   execution scenario and retain latency sensitivity.

Start with approximately **10 negRisk and 10 supported ladder events over a
development-only week**, plus the necessary warm-up context. Validate complete
membership, depth reconstruction, rule semantics, fee treatment and recording
quality before committing to months of acquisition. Pilot acceptance should
depend on correctness and coverage, not selecting a profitable sample.

Only after that audit should the event count and contiguous date range be fixed.
PMXT is a plausible route to a stronger experiment; a small pilot and three
accessible files do not establish that the expanded dataset is ready.
