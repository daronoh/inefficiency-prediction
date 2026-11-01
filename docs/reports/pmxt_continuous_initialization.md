# PMXT continuous initialization diagnostic

Completed 30 October 2026. Continuous history supplies starting books for all five
selected tokens, but **does not make the 12 diagnostic event-hours certifiable**.
Missing starting snapshots were one problem. Disagreement between reconstructed
depth and reported best prices remains after initialization under the unchanged
v3 replay assumptions.

## Fixed comparison

The diagnostic selects contracts using missing-seed status, basket size and a
fixed hash. Prices, opportunity labels and returns are not selection inputs:

- The complete three-outcome negRisk basket for Bitcoin versus gold versus the
  S&P 500 performance in 2026.
- One XRP ladder pair: YES to a dip to $1.20 in May and NO to a dip to $0.80.

The XRP result concerns this pair alone, not every relationship in its parent
event. The isolated comparison is rebuilt for exactly the same pair. Comparing
it directly with the earlier full ladder-event label would change the scope.

Both versions evaluate **6 May 2026, 09:00-15:00 UTC**, giving 12 event-hours.
Both use receipt context through 17:00, the same source-clock policy, metadata,
minimum size, tick convention and fee scenario. The only intended difference is
the amount of earlier recorded history used to initialize and maintain depth.

| Measure | One-hour warm-up | Continuous earlier history |
| --- | ---: | ---: |
| Receipt capture starts | 6 May 08:00 | 4 May 23:00 |
| Hours before analysis | 1 | 34 |
| Contiguous receipt-hour files | 9 | 42 |
| Tokens with a starting snapshot before the first scan | 0 / 5 | **5 / 5** |
| Tokens with causally available asks at the first scan | 0 / 5 | **2 / 5** |
| Certified positive event-hours | 0 | 0 |
| Certified negative event-hours | 0 | 0 |
| Unknown event-hours | 12 | 12 |
| Complete, retrospectively certified ask-side event-time | 0% | 0% |
| Unknown event-seconds primarily attributed to missing seeds | 34,617.331 | **0** |

The remaining ask-side faults at startup are not inferred from a missing
snapshot: the continuous replay has already observed price contradictions.
There are three ask-faulted tokens in total, leaving two with causally available
asks. Both causal availability and later retrospective label exclusions are
reported separately.

Zero certified positive labels here means that no opportunity is established
under this diagnostic's evidence policy. The unknown hours cannot be called
negative, and no market-wide opportunity-frequency or profitability estimate
follows from this deliberately selected subset.

## How initialization changed

The search walks backward through complete receipt hours until every required
token has an earlier full snapshot within the contiguous captured history. It
also follows the snapshot's source timestamp backward if necessary, rather than
accepting a receipt-time snapshot whose native anchor predates the captured
update history. The search is bounded below by 4 May 00:00.

The oldest of the five latest pre-scan snapshot anchors is **5 May
00:52:43.261 UTC**, for the XRP $1.20 YES token. The remaining four latest anchors
fall between 5 May 23:47 and 6 May 00:04. Three-hour search batches produce a
contiguous capture prefix starting at 4 May 23:00. Every intervening hourly file
through 6 May 17:00 is present and hash-checked; old depth is never carried across
an unobserved gap.

State is maintained continuously across hourly boundaries. The analysis still
uses snapshots before each scan; earlier updates construct that state rather
than providing additional future information to a model. No model fitting or
new final-test access occurs.

## What remains uncertain after seeding

In the continuous replay, the full 43,200 event-seconds remain unknown. Primary
attribution assigns **40,529.420 seconds** to retrospective best-price drift
exclusions and **2,670.580 seconds** to active contradictions. The former
exclusions look backward to the affected side's preceding snapshot because the
onset of disagreement is not established by the retained stream.

The original isolated run could not perform many consistency checks before its
first snapshot. During the same six analysis hours, the observer records 108
best-price contradiction side-groups in the isolated replay and 8,076 in the
continuous replay. These counts include repeated checks of already-faulted
states. More checks become possible after initialization; these numbers are
**not independent packet-loss counts** or proof that collecting more history
damaged the feed.

## Independent trace of one disagreement

A separate Decimal/dictionary replay reproduces an ask-side disagreement for
the Bitcoin YES token. It does not invoke the v3 book implementation or change
any labels:

1. At **12:31:33.883 UTC on 6 May**, a full snapshot contains **16.68 shares
   offered at $0.39**, which is its cheapest ask.
2. Through **12:44:47.912**, the captured stream supplies no intervening full
   snapshot and no SELL level update changing that $0.39 quantity. There are no
   conflicting same-time SELL quantities in this interval. Straight application
   of the absolute level updates therefore still leaves that offer present.
3. At **12:44:47.912**, a BUY level update sets $0.39 to **483.32 shares** while
   reporting a best bid of $0.39 and best ask of **$0.42**.
4. At **12:44:47.931**, 19 ms later, a trade message reports a BUY of exactly
   **16.68 shares at $0.39**. The trade size equals the stale snapshot quantity.

The next captured full snapshot for this token has native time 13:44:03.901.
A small targeted extraction of both outcome tokens for the 12:00 receipt hour
finds the complementary NO-side update, but no corresponding $0.39 YES ask
removal within ten source seconds of the contradiction. The NO update at the
same timestamp sets its $0.61 SELL quantity to 483.32, mirroring the new YES bid;
it does not explain removal of the old YES ask.

This is evidence of a trade-related transition that the current price-level-only
replay cannot reconcile. It does **not** prove whether the archive omitted an
expected removal, the stream expects different trade handling, or historical
timestamp conventions require another interpretation. The v3 replay treats
`last_trade_price` as informational and never decrements depth from it.

The [current Polymarket stream documentation](https://docs.polymarket.com/market-data/realtime-data)
defines separate book, price-change and trade messages. The
[NautilusTrader integration](https://nautilustrader.io/docs/latest/integrations/polymarket/#price-change-bursts)
describes completed-match best quotes accompanying intermediate resting-order
and maker-removal messages. Those are implementation references, not proof that
every archived May message follows the current convention. PMXT's retained
schema lacks the original exchange sequence/book hash needed to settle ordering
and completeness independently.

Blindly subtracting every trade is not a validated repair: a subsequent or
preceding absolute quantity can already reflect the fill, and duplicate trade
messages or cross-token representations need explicit handling. A future
trade-aware extension should be validated against independent later full books
before its state or labels are used for research claims.

## Artifacts and reproduction

```powershell
python -m src.data.pmxt_continuous freeze
python -m src.data.pmxt_continuous download
python -m src.data.pmxt_continuous replay
python -m src.reporting.pmxt_continuous
python -m src.data.pmxt_pilot verify
```

The frozen design and selected data are under `data/pmxt/continuous_seed_v1/`.
The downloader reuses verified prior subsets, reads only relevant remote row
groups, records per-hour hashes and identities, and stops at an 8 GB logical
range budget. An unsuccessful HTTP request never becomes an empty hour.

The main search charged **909,065,300 bytes (909.07 MB)**. Its selected-token
cache occupies **1,565,662 bytes**, including reused files outside the final
prefix; the continuous replay's 42 raw subsets occupy **1,052,741 bytes**.
Replay copies are additional local files. The targeted two-token forensic hour
charged **5.50 MB** separately, within a 100 MB cap.

Run-level reports remain under `results/pmxt_pilot/seed_v1_isolated/` and
`seed_v1_continuous/`. The paired overview, labels, token initialization,
contradiction examples and independent raw trace are under
`results/pmxt_pilot/continuous_seed_v1/`. All prior PMXT versions and Dome
artifacts remain preserved.

[Notebook 16](../../notebooks/16_pmxt_continuous_initialization.ipynb) checks the
saved manifests, raw data, implementation and label invariants, then displays
the comparison. Its default execution downloads nothing and trains no models.

## Research implication

Continuous initialization is the correct approach for a continuous replay, with
separate initialization for new tokens or recovery after gaps. This experiment
demonstrates that it resolves missing starting books for the selected contracts.
It also demonstrates that initialization alone does not certify the subsequent
state under the existing policy.

The next validation target is trade-related state transitions and message
semantics, using the recorded example and independent later snapshots. Keep
those changes versioned and compare them with these preserved results. Expanding
the event universe or training PMXT models before this validation would leave
the current label-quality problem unresolved.

Displayed-margin diagnostics retain the zero-fee, minimum-five and initial
0.0001 tick assumptions; historical parameters remain unverified. The primary
future economic scenario remains 100 ms total source-update-to-order-arrival
delay, uncapped capital/shares and displayed-depth constraints. This diagnostic
estimates neither fills nor P&L and makes no changes to the completed Dome study.

## Follow-up reconciliation audit and stopping decision

A local-only audit in `src/data/pmxt_reconciliation.py` applies absolute depth
updates with an independent Decimal replay. It neither subtracts trades nor
prunes levels using best prices. Results are under
`results/pmxt_pilot/reconciliation_v1/`; no additional downloads were required.

Four disagreement episodes begin during the six-hour analysis interval. None
is repaired to its original reported quote by ordinary depth updates within
one second, across checks at 0, 10, 50, 100, 250 and 1,000 ms. Two end at a full
snapshot, one remains unresolved at the endpoint, and one becomes consistent
after 76 ms at a different quote. That last case does not validate the original
reported state. Four further episodes begin before analysis and overlap it.

At nine ask-side full-book checkpoints without earlier quantity ambiguity,
eight match the reconstructed best price and its quantity, but only two match
the entire reconstructed ask depth. Seven disagree at levels not updated at
the checkpoint timestamp, so these differences are not explained simply by
ordering that timestamp's updates before or after its snapshot. This remains
a test of the assumed source-clock conventions, not proof of a specific archive
failure or a provider-wide estimate.

A weaker rule that accepts a book whenever its latest best prices agree would
retain 24.77% of negRisk-basket event-time and 30.83% of ladder-pair event-time on
the ask side. These are **causal top-price consistency fractions**, not verified
depth or usable-label coverage; matching top prices did not establish deeper
quantities at the checkpoint tests. The preserved v3 policy still has zero
certified ask-complete event-time in this diagnostic.

Partially observed hours can supply witnessed-positive labels and lower bounds
on value when an independently valid basket exists. Absence during an observed
fraction does not establish absence during the full hour. The onset and recovery
of uncertain state must themselves be justified; merely hiding the moments that
display a mismatch does not validate the intervening depth.

Further PMXT reconstruction work is deferred. Dome remains the primary dataset,
with a separate [longer forward-validation study](dome_validation_horizon.md).
No PMXT labels are relaxed, no missing intervals become negatives, and no PMXT
models or economic returns are produced by this follow-up.
