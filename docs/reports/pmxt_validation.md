# PMXT reconstruction, uncertainty and label validation

Checked 29 October 2026. Version 3 uses the same frozen universe and cached
records as version 2. The Dome data, code, models and final results remain
unchanged. No additional download, PMXT model fitting or new final-test access
is part of this run.

## Sample and findings

The cache contains six receipt-hour files for **4 May 2026, 00:00?06:00 UTC**.
The analysis spans **01:00?04:00 UTC**: three hours across 10 negRisk and five
ladder events, 200 required tokens and 301 supported buy baskets. The first hour
seeds reconstruction; the final two provide late-record and validation context.
This is a pilot layout, not a requirement to discard half of every future block.

The 45 event-hours have the following results:

| Measure | Version 2 | Version 3 |
| --- | ---: | ---: |
| Certified occurrence labels | 7 | **24** |
| Certified negative labels | 7 | **24** |
| Witnessed positive labels | 0 | **0** |
| Unknown occurrence labels | 38 | **21** |
| Complete bid-and-ask event-hours | 7 | **7** |
| Complete ask-only event-hours | Not separately reported | **8** |
| Exact hourly value targets | Not separately certified | **24** |

All seven previously known labels remain known. The **17 newly certified labels
are negRisk event-hours**. In total, 23 of 30 negRisk hours and one of 15 ladder
hours are certified. This improves label evidence, not the underlying capture
rate. For example, known legs costing $0.70 and $0.40 per set already rule out a
strictly positive guaranteed margin against the $1 payout floor, regardless of
an unknown third leg's nonnegative cost.

Version 3 fully observes both sides for **45.73%** of event-time and asks for
**55.42%**. Occurrence is determined for **73.66%** of event-time, including
periods where known legs prove that a basket cannot qualify. Some unknown hours
are mostly ruled out; a remaining unobservable interval still prevents assigning
zero. The proof applies to the implemented minimum-payout buy baskets at the
archive's source-millisecond resolution and under the stated metadata and
trading-parameter scenarios.

No qualifying robust positive was found in accepted states. This small sample
does not establish that no market opportunities existed. It supplies no actual
candidate episode starts for an empirical latency comparison or P&L estimate.
The unresolved ladder coverage and lack of positive labels also prevent treating
this sample as adequate model-training evidence.

## Handling the four reconstruction issues

[PMXT v2](https://archive.pmxt.dev/docs/v2-data-overview) contains full books,
absolute price-level updates, trades and tick changes, with source and receipt
clocks. Records are grouped by token and source millisecond before checking the
resulting state. Duplicate copies are coalesced; arbitrary Parquet row order
does not choose the final exchange state.

| Issue | Version 3 policy |
| --- | --- |
| Best-price contradiction | Check the completed group. Never create liquidity from a reported best price. An unresolved contradiction invalidates the affected side until a reliable full snapshot. Because an absent update's onset is unknown, retain a conservative retrospective exclusion from that side's preceding snapshot anchor. |
| Conflicting quantities at one source timestamp | Retain the possible sizes for that price level. Lower/upper quantities bound what every/some ordering could support. A compatible closing snapshot or later unambiguous absolute update can resolve the level. No blanket backward exclusion arises from the level ambiguity itself. |
| Crossed book | Check after the whole native-time group. A still-crossed or possibly crossed state is not tradable. Do not prune prices or fabricate liquidity to repair it; a valid later snapshot restores the joint state. The exclusion begins at the observed crossing unless separate drift evidence requires an earlier interval. |
| Full snapshot conflicts with same-time updates | A single snapshot can close the group where its quantities match the observed alternatives and its best prices are compatible. Otherwise retain the disagreement at the affected levels. Conflicting full snapshots also supply alternatives rather than a selected winner. |

The closing-snapshot convention is an explicit source-clock interpretation, not
independent proof of ordering. The [NautilusTrader integration documentation](https://nautilustrader.io/docs/latest/integrations/polymarket/#price-change-bursts)
describes match bursts whose best quotes can describe the completed match before
all removals are applied, with a full-book event closing the burst. Its behavior
is a useful implementation reference, not validation that every historical PMXT
message has identical semantics. PMXT's retained schema supplies no original
book hash or exchange sequence with which to prove complete ordering.

A full snapshot can itself supply new state, so disagreement with preceding
deltas alone is not a missing-data count. The earlier strict-equality experiment
is retained as a diagnostic and is not the primary acceptance rule.

Across the six-hour context, classification found:

- 48,731 identical level-update replays and 10,049 identical snapshot replays.
- 1,550 unresolved same-time level-conflict cases; 1,427 later absolute updates
  resolved a previously ambiguous level. These are case/update counts, not a
  one-to-one recovery percentage.
- Two conflicts resolved by a compatible same-time snapshot, and 12 unresolved
  snapshot/level ties.
- 123,012 best-price contradiction side-groups. This includes repeated checks
  while a side remains faulty; it is not a count of independent outages.

Counters span warm-up, analysis and receipt context. A fault on bids does not
by itself make otherwise reliable asks unusable for a buy-only basket. Bid
information remains necessary for bid features and a later unwind simulation.
Reliable unrelated ladder pairs remain assessable.

## Labels, value bounds and causal features

Each basket receives a conservative value lower bound and an optimistic upper
bound across its retained depth alternatives. The bounds need not be attainable
jointly: combining alternatives deliberately produces an outer range. No unknown
leg is supplied with artificial free liquidity for a trade.

A basket is ruled out if, for example, known legs already cost at least the
minimum payout, a known leg cannot supply the minimum quantity even under its
optimistic depth, or every retained alternative is unprofitable. An event is
ruled out only when all its supported baskets are ruled out.

| Evidence | Hourly target |
| --- | --- |
| A reliably supported candidate is witnessed | Occurrence = 1, even if another part of the hour is unknown |
| Every basket is ruled out throughout the hour | Occurrence = 0 |
| An unobservable interval could contain a qualifying basket | Occurrence remains unknown |
| The hourly value lower and upper bounds coincide | Exact peak-value regression target |
| A positive observed peak could have been exceeded in an unobservable interval | Retain the lower bound; do not use it as an exact regression target |

**Model features use only causal source-time states strictly before the scan.**
Retrospective drift masks can exclude label evidence but are removed when
computing features; they cannot become a predictor derived from later records.
The causal uncertainties already observed before the scan remain in those
features. A later snapshot never backfills earlier feature prices or quantities.

The 24 exact value targets in this run are all zero because those hours are
ruled out throughout. No positive censored-value examples were observed; their
handling is verified by tests. The exported feature table remains an adapter
diagnostic schema, not the full schema used by the saved Dome models.

## Trading and research limits

Quote bounds use a five-share minimum, zero fees and an initially assumed
0.0001 tick until an observed tick change. Historical fees, exchange minimum
sizes/ticks and point-in-time contract metadata are not independently verified.
Current Gamma metadata is used under the frozen retrospective rules/membership
assumption. These are conditional displayed-margin labels, not verified fee-net
arbitrage or collectible dollars.

Synthetic tests check fixed quantities and limits at arrival, including an
opportunity disappearing after 50 ms. The primary future economic assumption
remains **100 ms total source-update-to-order-arrival delay**, uncapped capital
and shares, bounded by displayed depth. The diagnostic also checks 0/50/250/500
ms. Actual arrival tables are empty in this sample because no accepted candidate
was found. No reservations, top-N policy replay, partial unwinding or net P&L are
estimated here.

Native timestamps have millisecond resolution. Grouping cannot recover
unrecorded sub-millisecond states. Neither internally matching best prices nor
finite receipt padding proves that all deep-level updates were captured.
Quality exclusions may disproportionately affect active periods; broader
validation must report that bias rather than selecting only easy quiet events.

## Reproduction and preserved artifacts

```powershell
python -m src.data.pmxt_revalidation
python -m src.data.pmxt_pilot verify
```

These commands use the local cache. New helpers are
`src/data/pmxt_uncertainty.py` and `src/data/pmxt_revalidation.py`.
[Notebook 14](../../notebooks/14_pmxt_uncertainty.ipynb) verifies the design,
universe, raw records, implementation, derived tables, reports and Dome hashes,
then displays the comparison. It downloads nothing and fits no models by default.

Derived tables are under `data/pmxt/pilot_v1/source_validation_v3/`; reports,
issue examples, arrival checks and `comparison_v2.csv` are under the matching
`results/pmxt_pilot/pilot_v1/source_validation_v3/`. Earlier PMXT versions and
Dome artifacts are retained. Notebook 13 still reproduces the version 2 report.

The six raw subsets occupy **13.58 MB** and recorded extraction ranges total
**2.08 GB**, plus the earlier identifier inventory. The earlier roughly 61 GB
weekly estimate referred to network traffic, not retained storage. At the
six-hour rates, a 169-file week would retain roughly 0.38 GB and transfer 59 GB;
activity and universe size change those extrapolations.

Before a larger model study, resolve remaining ask-side contradictions on
several development days, verify historical trading parameters and complete the
feature-schema adapter. Select events by contract structure and recording
coverage, not inspected profits. Then freeze the supported cohort and contiguous
months and reserve the final month before viewing its opportunity outcomes. The
completed Dome final test remains unavailable for new model selection.
