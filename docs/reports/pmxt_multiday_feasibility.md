# PMXT multi-day data suitability study

Completed 29 October 2026. The frozen 24-hour pilot is complete, but the current
reconstruction is **not ready for a larger model-training study**. It finds two
conditional positive event-hours in one ladder event, 41 certified negatives
and 317 unknown labels. Sparse starting snapshots in the later isolated blocks
are the largest new limitation; contradictions after seeding remain a separate
problem. These results do not establish the prevalence or profitability of
arbitrage across Polymarket.

## Expanded results

| Analysis block | Event-hours | Positive | Negative | Unknown | Complete ask-side event-time |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4 May | 90 | 0 | 39 | 51 | 55.86% |
| 6 May | 90 | 2 | 2 | 86 | 1.91% |
| 8 May | 90 | 0 | 0 | 90 | 0.00% |
| 10 May | 90 | 0 | 0 | 90 | 0.00% |
| **Total** | **360** | **2** | **41** | **317** | **14.44%** |

NegRisk contributes 240 event-hours: 38 negative and 202 unknown. Ladders
contribute 120: two positive, three negative and 115 unknown. The original
45 overlapping event-hour labels are unchanged when replayed with the extended
4 May context.

Only **11.94%** of hourly occurrence labels are certified, against the frozen
90% target. Complete asks cover **14.44%** of event-time, against 95%. The median
number of ask-complete events at a scan is **zero**, against ten. All three
engineering criteria fail. Complete bid-and-ask event-time is 11.82%, and
occurrence is determined for 19.40% of event-time. The latter can exceed full
book coverage because known legs sometimes rule out baskets with unknown legs.

All 41 exact hourly value targets are zero. Both positive hours supply only a
lower bound on their hourly peak because other baskets remain unobservable.
They must not be used as exact positive regression targets. The small, dependent
positive sample and non-random missing labels cannot support model comparison
or a representative opportunity-frequency estimate.

### The observed ladder quote

The two positive hours are 13:00-14:00 and 14:00-15:00 UTC on 6 May, in
`what-price-will-solana-hit-in-may-2026`. Both involve the same threshold pair:

| Leg | Price | Quantity | Displayed purchase cost |
| --- | ---: | ---: | ---: |
| YES: Solana reaches $160 in May | $0.004 | 800 | $3.20 |
| NO: Solana reaches $170 in May | $0.995 | 800 | $796.00 |
| **Combined** | **$0.999 per set** | **800 sets** | **$799.20** |

Under the frozen implication rules, each set pays at least $1. The displayed
minimum payout is therefore $800, giving **$0.80 gross margin** at that size.
The first recorded candidate starts at 13:37:17.010 UTC. Independent replay of
its two legs reproduces the 800-share quantity, prices and margin. Sizing uses
the uncapped depth optimizer, rather than a fixed 800-share cap.

There are 12 recorded episode starts triggered by accepted source updates.
Every associated fixed-quantity/limit diagnostic finds displayed support at
0, 50, **100**, 250 and 500 ms. This is not proof of fills: the checks do not
reserve or consume liquidity, simulate other traders, or represent partial
execution. Starts are update-triggered diagnostics rather than a complete
monitoring-policy replay. The displayed margin is supported for a combined
1,977.496 event-seconds across the two hours; this is not 1,977 separate trades.
Neither 12 times $0.80 nor two times $0.80 is a valid realised P&L estimate.

The zero-fee assumption is conditional. The corresponding raw historical trade
messages contain 14 observations with `fee_rate_bps=0` in this block, whereas the
frozen current market export enables `crypto_fees_v2` with rate 0.07 and exponent
1. Those records alone do not verify a complete historical fee schedule. The
[current fee documentation](https://docs.polymarket.com/trading/fees) describes
market-specific parameters applied at match time; applying today's parameters
to May without historical verification would be another assumption. The $0.80
must therefore remain a gross displayed-margin finding, not verified fee-net
profit.

## Purpose and fixed design

This study tests whether PMXT v2 can supply reliable event-level opportunity
labels across several days. It does not fit a model, replace the Dome experiment,
inspect a new final test or estimate trading profit.

The universe remains the original ten negRisk and five ladder events, with 200
required tokens and 301 supported buy baskets. Events were selected using
contract structure and recorded identifiers, rather than inspected profits.
The incomplete metadata catalogue and the concentration of ladders in monthly
cryptocurrency thresholds prevent treating this as a representative market sample.

The blocks below were saved in `study_design.json` before inspecting their
expanded price outcomes. All times are UTC.

| Date | Analysis interval | Receipt archive interval |
| --- | --- | --- |
| 4 May 2026 | 01:00-07:00 | 00:00-09:00 |
| 6 May 2026 | 09:00-15:00 | 08:00-17:00 |
| 8 May 2026 | 17:00-23:00 | 16:00-01:00 on 9 May |
| 10 May 2026 | 09:00-15:00 | 08:00-17:00 |

The planned sample is 24 distinct analysis hours, or 360 event-hours. Each block
has a one-hour warm-up and two-hour receipt tail. The original 4 May 01:00-04:00
analysis was already inspected; the remaining 21 hours were uninspected at design
freeze. The sample is a development feasibility study, not an untouched final test.
The fixed cohort is not refreshed based on later recording quality or returns.

## What made the original hours unknown

The audit independently reproduces the unchanged version 3 labels and explains
unknown intervals using only tokens of baskets that cannot be ruled out. A faulty
token in an already blocked basket is not counted as opportunity uncertainty.
Bid-only faults do not invalidate reliable ask-side buy opportunities.

| Original sample | negRisk | Ladder | Total |
| --- | ---: | ---: | ---: |
| Event-hours | 30 | 15 | 45 |
| Certified negative | 23 | 1 | 24 |
| Witnessed positive | 0 | 0 | 0 |
| Unknown | 7 | 14 | 21 |
| Unknown event-time, seconds | 4,793.758 | 37,883.018 | 42,676.776 |

Of the 42,676.776 unknown event-seconds, the primary attribution is:

- **39,985.781 seconds (93.69%)**: a retrospective exclusion before an ask-price
  contradiction, starting at the preceding full-snapshot anchor because the
  onset of the disagreement is unknown.
- **2,690.995 seconds (6.31%)**: an active ask-price contradiction after it is
  observed and before reliable recovery.
- No interval has missing initial snapshots, quantity alternatives, crossed
  books or malformed groups as its primary cause.

Causes can coexist. Active contradictions occur somewhere in the relevant
unknown baskets for 15,477.431 seconds; quantity alternatives overlap with
380.884 seconds. These overlapping numbers must not be added to the primary
totals or interpreted as independent outages.

For example, a completed update group can leave a reconstructed best ask of
$0.030 while its `best_ask` field reports $0.031. The reported price does not
provide the missing depth or establish when the old quote disappeared. Such a
case can reflect missing updates or timestamp semantics; this audit does not
prove which. Full snapshots can themselves introduce new state, so ordinary
snapshot-versus-delta depth differences are not counted as evidence of loss.

This means that longer warm-up alone is not a demonstrated solution to the
original coverage problem. The current policy retains the exclusion rather than
manufacturing liquidity, dropping levels to create profits, or labelling unknown
hours as negative.

## Starting books in the expanded sample

The later blocks expose a different problem. Full-book messages are not
guaranteed at the beginning of each archive hour. An absolute update sets the
quantity at one price level; it does not establish the untouched levels of a
token that has no complete starting snapshot.

| Block | Raw full-book messages over nine receipt hours | Tokens seeded before first scan, of 200 | Tokens seeded before analysis ends, of 200 | Tokens without a qualifying snapshot in the entire nine-hour context |
| --- | ---: | ---: | ---: | ---: |
| 4 May | 39,415 | 200 | 200 | 0 |
| 6 May | 401 | 2 | 172 | 28 |
| 8 May | 84 | 3 | 58 | 132 |
| 10 May | 31 | 4 | 18 | 172 |

The counts include duplicate full-book copies; the corresponding distinct
token/source-timestamp groups are 27,565, 393, 83 and 31. In the source capture
and analysis windows, 1.02%, 72.06%, 88.26% and 97.24% of price-level updates,
respectively, occur before that token has a qualifying full snapshot. The seed
audit does not turn these updates into valid full depth or backfill earlier
states with later snapshots.

Consequently, a one-hour warm-up is inadequate for these isolated later blocks.
This does **not** establish that continuous PMXT history lacks the required
starting snapshots. A continuous historical reconstruction could obtain them
earlier and carry state forward through every intervening update. It would still
need to address contradictions after seeding. Reusing a 4 May book on 6 May
without the missing intervening updates would not be valid.

The earlier result about warm-up applies to the original 4 May sample, which
already seeded every token. It must not be generalized to the later blocks.
The additional `seed_coverage.csv` and `seed_tokens.csv` report this distinction.

Across the expanded sample, primary attribution assigns 943,850.415 unknown
event-seconds to missing qualifying starting snapshots, 93,428.365 to
retrospective drift exclusions and 7,329.055 to active contradictions. The
priority ordering makes these primary counts mutually exclusive. Causes still
overlap: retrospective drift is present in relevant unknown baskets for
425,124.672 seconds, including some intervals also lacking a different leg's
seed. Recovering seeds therefore would not automatically resolve every unknown.

## Label and value definitions

- A positive hour contains at least one supported basket with positive margin
  under every retained depth alternative, including the minimum size and legal
  limit-price checks. Other intervals may remain unknown.
- A negative hour requires every supported basket to be ruled out throughout
  the hour. Reliable expensive legs can prove this even when other legs are
  unknown: $0.70 plus $0.40 already exceeds the $1 minimum payout.
- An unknown hour has no confirmed positive and at least one interval in which
  an opportunity cannot be ruled out.
- The value target is the peak qualifying displayed margin of a single basket.
  It is an exact target only when the hourly lower and upper bounds coincide.
  A witnessed positive with an uncertain higher peak remains a lower bound.

The first layer uses source-time information before each scan for features.
Later quality findings can exclude label evidence but never become retrospective
feature values. The exported features are still an adapter diagnostic schema;
they are not the complete inputs used by the existing Dome models.

## Suitability criteria and interpretation

Before expanded outcomes were inspected, the design recorded engineering
targets of 95% complete ask-side event-time, 90% certified occurrence labels and
a median of at least ten ask-complete events at the hourly scans. These are study
targets, not industry standards. Complete bid-and-ask coverage is reported
separately, since bid features and unwind simulation require it.

Positive hours, distinct positive events, candidate episode starts, opportunity
duration and peak displayed margin are reported separately from coverage. Passing
coverage checks does not establish enough positive examples for modelling.
Failing them must not be addressed by selecting only events with convenient
observed outcomes. Unknown intervals can be concentrated during active periods,
creating non-random missing labels.

## Data access and reproducibility

[PMXT v2](https://archive.pmxt.dev/docs/v2-data-overview) publishes source and
receipt timestamps, full books, absolute price-level updates, trades and tick
changes in hourly receipt partitions. The pilot preserves source-millisecond
batch reconstruction and compatible closing-snapshot assumptions from version 3.
The archive does not supply the original exchange sequence or book hash needed
to prove complete ordering and depth capture.

The downloader uses bounded ranges with identity and length checks, retry
allowances and per-file accounting. Small probes completed promptly without an
`If-Match` request header while conditional request bodies stalled. The adapter
therefore verifies each response's ETag and object length without sending that
header. A changed ETag, wrong range, truncated response or unsuccessful HTTP
status never becomes an empty market hour. Range requests have a wall-clock
deadline as well as socket timeouts.

To avoid many fragile remote column reads, new hours are explicitly staged in
bounded chunks. Local filtering retains only selected tokens; staging archives
are removed after the selected subset is committed and file handles are closed.
The design caps new charged range bytes at 16 GB and selected raw cache at 1 GB.
The transfer ledger also includes failed attempts and a conservative allowance
for interrupted transport diagnostics; it is not a precise network-interface
traffic measurement. Previously cached files are reused and verified.

The completed run retains **57,209,608 bytes (57.21 MB)** of selected raw
Parquet, with **13,892,862,044 charged range bytes (13.89 GB)**, including the
700 MB diagnostic/interruption allowance. The 36 complete source objects total
14.37 GB; six existing subsets were reused. Network traffic and retained subset
storage are different quantities. All temporary whole-object staging files
were removed after extraction.

```powershell
python -m src.data.pmxt_diagnostics
python -m src.data.pmxt_multiday freeze
python -m src.data.pmxt_multiday download
python -m src.data.pmxt_multiday replay
python -m src.data.pmxt_bootstrap
python -m src.data.pmxt_feasibility
python -m src.data.pmxt_pilot verify
```

The default downloader uses three workers. Resume with the same command; cached
successful hours and identity-checked staged prefixes are retained. Do not change
the frozen plan to conceal a missing or failed hour. `--workers 1` is available
for a serial transfer. `pmxt_feasibility --reuse-audits` reuses completed audits
after checking that they match the replay summaries.

[Notebook 15](../../notebooks/15_pmxt_multiday_feasibility.ipynb) verifies saved
evidence, shows the original audit and compares the expanded results by date,
event type and individual event. It performs no network access by default.
Inputs and derived tables are under `data/pmxt/pilot_multiday_v1/`. Block reports
are under `results/pmxt_pilot/may04/`, `may06/`, `may08/` and `may10/`; aggregate
reports are under `results/pmxt_pilot/pilot_multiday_v1/`. Original pilot versions
remain under `pilot_v1`.

## Limits on economic claims

The diagnostic assumes zero fees, a five-share minimum and an initial 0.0001
tick until an observed tick change. Historical contract membership, fees, minimum
sizes and ticks remain unverified. Current metadata is used retrospectively.
The supported buy baskets use a $1 minimum payout and do not enumerate every
possible complement, inventory-dependent sale or higher-payout combination.

Source-time replay assumes a hypothetical timely feed; it does not describe
PMXT delivery latency. Finite receipt padding and internal consistency cannot
establish complete capture. Millisecond grouping cannot recover missing
sub-millisecond states.

Displayed margin is not collectible profit. Repeated quotes, episode starts and
hourly peak values must not be summed as realised P&L. A later economic study
would use 100 ms total source-update-to-order-arrival latency, uncapped capital
and shares, and displayed-depth constraints, with separate latency sensitivities,
inventory reservations and explicit partial-execution treatment. No model has
been trained on this expanded pilot.

## Research decision

Keep the completed Dome experiment unchanged. Do not replace it with this pilot
or fit PMXT models on these labels. The expanded evidence establishes that some
conditional gross ladder inefficiency can be witnessed, but it does not supply
adequate occurrence labels, positive value targets or cross-event support.

The next engineering validation should obtain a historical starting snapshot
for each token and replay every intervening update through a short continuous
window. Measure how long seeding actually takes instead of assuming a fixed
one-hour warm-up. Then remeasure the independent drift problem and validate the
source-clock interpretation. Do not reuse old depth across undownloaded gaps or
backfill earlier features with later snapshots.

The conservative v3 policy also declines to treat pre-seed absolute level updates
as a usable partial book. A separately specified extension could investigate
whether observed levels provide valid positive lower bounds while leaving the
unobserved remainder unknown. It would need independent validation and preserved
comparisons; it is not implemented in this frozen study.

Only after those checks, historical trading parameters and the complete feature
adapter are validated should a new cohort and continuous months be frozen. Keep
the last month untouched before inspecting its opportunity outcomes. The
completed Dome final test remains unavailable for further model selection.

Validation: 244 unit/integration tests pass. The Dome preservation check confirms
2,913 protected files unchanged, and the original v2/v3 PMXT implementation and
artifact hashes still match.
