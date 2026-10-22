# Economic replay of opportunity-model rankings

## Main finding

Opportunity prediction and economic performance are related but distinct.
At the primary **100ms** execution assumption, quoted-gap leads the selected
policies at **N=5**, with **\$6,045.25** known simulated net P&L. At **N=3**,
book-only logistic regression leads with **\$5,999.41**, compared with quoted-gap's
**\$4,379.96**. The same fitted predictions are used at both budgets.

The smaller-budget logistic result exceeds monitor-all's **\$5,951.60**, because
monitoring more events can introduce losing partial baskets. Monitor-all is a
reference, not an oracle or an upper bound. These are development-period,
conditional simulated margins, not demonstrated live or settled profits.

## Design fixed before replay

The first layer predicts quoted opportunity occurrence or peak dollar margin.
This second layer does not retrain any model, rebuild its targets or change its
rankings in response to execution outcomes. First-layer capture results remain
in [the opportunity report](opportunity_prediction.md).

Six policy rankings are replayed: quoted-gap, book-only logistic, book-only forest,
forest with metadata, tuned two-stage forest and tuned value ensemble. The five
learned policies were selected from the preceding development analysis, before
this replay, rather than from final-test results. Twenty fixed-seed random
policies and monitor-all provide references.

The grid contains **540 independent policy/scenario combinations**:
27 policies ? two monitoring budgets ? two event scopes ? five arrival delays.
Each has its own liquidity reservations; they do not trade against each other.
Event selection is frozen across delays. Random scores are also paired across
budgets and scopes. Removing December Fed reranks the remaining events and
reallocates the slots; it does not merely subtract that event's P&L.

Evaluation covers **1 November 2025 through 7 January 2026**, using the original
three chronological development folds. Reservations persist across their
boundaries. Trade attribution in fold tables follows the initiating monitoring
window. The final test from **8 January 2026 onward remains closed**.

## Execution contract

- Primary arrival: **100ms**; sensitivities: **0, 50, 250 and 500ms**.
- Immediate submission at a qualifying gap, with no persistence threshold.
- Full integer displayed-depth sizing, minimum five shares or the exchange
  minimum if higher; no artificial capital or share cap.
- Equal-share complete negRisk YES baskets or audited broader-YES/narrower-NO
  ladder pairs. Contractual payout floor is $1 per set.
- Size and per-leg price limits are fixed before arrival. Individual fill-or-kill
  legs are assumed to fill only when arrival depth supports those frozen orders.
- Positive gross and worst-limit-price entry margins; zero exchange fees for
  this historical cohort and zero additional per-attempt costs.
- Partial baskets are unwound **one second after arrival**. That one-second
  event hold is unchanged across delays, including after complete or empty fills.
- One attempt in flight per event, no retries within a continuously positive
  episode, and shared/unchanged ask-version reservations across pairs and hours.
- No imposed random failure probability, quote-age cutoff or favourable filtering
  based on future label coverage. Unknown executions would be counted separately.

All **105,612 attempt records across the counterfactual configurations** have
known outcomes in this run. This is not the number of unique market trades: the
same historical opportunity can appear in several independent policies/scenarios.
Incompletely observed event-hours still exist and consume shortlist slots; known
execution outcomes do not make their full-window prediction labels known.

Zero arrival is an optimistic same-book reference within this strategy. It retains
the one-second hold and reservations, and is not a guarantee of live execution or
a theoretical upper bound on every path-dependent policy. Changing arrival can
also change later trades through reservation history.

## Primary economic comparison

Net P&L includes known completed-basket margins and actual simulated partial-basket
unwind gains/losses. It imposes no hypothetical failure penalty. The random row is
the mean of 20 complete replays, not a single tradable policy.

| Policy | N=5 net P&L | N=5 partial P&L | N=3 net P&L | N=3 partial P&L |
| --- | ---: | ---: | ---: | ---: |
| Quoted-gap rule | $6,045.25 | $-283.47 | $4,379.96 | $-118.16 |
| Book logistic | $5,903.33 | $-414.75 | $5,999.41 | $-282.97 |
| Book forest | $5,939.12 | $-414.75 | $5,845.60 | $-414.48 |
| Forest + metadata | $5,942.52 | $-414.75 | $5,846.04 | $-413.98 |
| Tuned two-stage forest | $5,946.19 | $-414.75 | $4,635.97 | $-270.63 |
| Tuned value ensemble | $5,963.27 | $-378.05 | $4,894.95 | $-242.37 |
| Monitor all | $5,951.60 | $-414.75 | $5,951.60 | $-414.75 |
| Random (20-seed mean) | $1,747.61 | $-140.16 | $961.75 | $-92.91 |

The first layer's N=5 quoted-value leader, forest with metadata, earns **\$5,942.52**,
below quoted-gap despite higher measured opportunity capture. At N=3, logistic's
advantage over quoted-gap is **\$1,619.45**. The direct link from improved ranking
metrics to improved economic outcomes therefore depends on the budget and policy.

At N=3 logistic completes **427 of 443 attempts**, with **16 partial baskets**,
**-\$282.97** partial P&L and **\$142.08** maximum drawdown. Quoted-gap completes
**364 of 376 attempts**, with **-\$118.16** partial P&L and **\$44.49** drawdown.
Logistic's larger net return comes with larger execution losses and drawdown.
At N=5 quoted-gap completes **474 of 491 attempts**, with **-\$283.47** partial
P&L and **\$142.08** drawdown. Random selection averages **\$1,747.61 at N=5** and
**\$961.75 at N=3**; seed variation is reported separately.

Two distinctions explain why the metrics cannot be equated. First, the quoted-value
target is the best one-basket margin observed within the hour; the trading policy
enters when a qualifying gap appears and does not wait with hindsight for that
maximum. Second, fills, partial losses, one-attempt scheduling and reserved
liquidity determine what can subsequently be traded. Summed hourly quote maxima
are not a profit denominator, so P&L divided by that sum is not an execution rate.

## Arrival-latency sensitivity

| Policy / budget | 0ms | 50ms | 100ms | 250ms | 500ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Quoted-gap rule / 5 | $6,507.42 | $6,385.30 | $6,045.25 | $5,307.43 | $5,075.98 |
| Book forest / 5 | $6,536.63 | $6,414.64 | $5,939.12 | $5,201.30 | $4,966.74 |
| Book logistic / 3 | $6,460.99 | $6,339.46 | $5,999.41 | $5,260.23 | $5,028.48 |
| Quoted-gap rule / 3 | $4,522.21 | $4,462.48 | $4,379.96 | $3,703.20 | $3,620.77 |
| Monitor all / all | $6,549.12 | $6,427.13 | $5,951.60 | $5,212.42 | $4,977.86 |

For example, at N=5 book forest earns more than quoted-gap under 50ms arrival,
but less under 100ms. The first-layer model has not changed; execution has.
December Fed alone illustrates the reversal: book logistic/forest earn about
**\$5,603.72** versus quoted-gap's **\$5,578.74** at 50ms, but **\$5,222.45** versus
**\$5,332.95** at 100ms. Those event-only figures should not be confused with the
full-period totals above.

The latency table is a sensitivity analysis, not evidence that the feed or a
trader can actually achieve those delays. Information-delivery latency is not
measured. Source timestamps and as-of books are used as the observation clock.
All scenarios retain the same unwind assumptions, so this study isolates arrival
latency within the existing simulator rather than tuning execution to maximise P&L.

## Event concentration

| Policy | N=5, Fed excluded | N=3, Fed excluded |
| --- | ---: | ---: |
| Quoted-gap rule | $726.57 | $729.50 |
| Book logistic | $697.69 | $673.07 |
| Book forest | $726.35 | $694.89 |
| Forest + metadata | $725.47 | $657.43 |
| Tuned two-stage forest | $722.81 | $725.54 |
| Tuned value ensemble | $756.61 | $761.09 |
| Monitor all | $729.15 | $729.15 |
| Random (20-seed mean) | $315.17 | $174.61 |

December Fed remains a major source of the aggregate returns. The exclusion
comparison preserves each fitted model and reranks the remaining event pool.
It does not establish generalisation to unseen topics or events. Fold results,
event contributions and paired family-resampling intervals accompany the tables.
The intervals use only nine event families, known returns and no correction for
multiple policies or scenarios; they are exploratory, not a substitute for the
untouched test.

Specifically, December Fed contributes **\$5,346.83 (89.12%)** of book logistic's
N=3 net P&L, and **\$5,332.95 (88.22%)** of quoted-gap's N=5 net P&L. With that
event excluded and slots reallocated, logistic earns **\$673.07 at N=3**, below
quoted-gap's **\$729.50**. The tuned value ensemble earns **\$761.09**, but its
advantage is only **\$31.59**. These results do not support a broad claim that the
logistic policy is economically superior across the market.

The paired nine-family interval for logistic's full-universe N=3 advantage over
quoted-gap is approximately **-\$164 to +\$5,066**, encompassing zero despite the
point estimate of +\$1,619. At N=5 the combined forest's difference is **-\$102.73**,
with an interval of approximately **-\$340 to +\$30**. Concentration and the small
number of independent families materially limit the conclusions.

## Funding, conversion and risk accounting

Net margin is not available cash. A completed basket's guaranteed payout is
booked at arrival in the P&L curve, but its purchase cash can remain tied up until
settlement. Partial P&L is booked at unwind. These curves therefore describe
economic margin accounting, not a settled cash balance.

The report separates the largest single order reservation, peak simultaneous
in-flight reservations, cumulative purchase cash, and a **no-settlement funding
scenario**. In the last scenario, completed baskets return no settlement cash
during the experiment; known partial unwinds return proceeds; unknown arrivals
retain reservations and unknown exits return no cash. Simultaneous submissions
are funded before releases. This is an explicit funding scenario, not an estimate
of live required capital or an ROI denominator. Fold-only funding summaries omit
holdings inherited from previous folds; the full-period scenario is primary.

| Policy and budget, 100ms | Peak in-flight reservations | No-settlement funding scenario |
| --- | ---: | ---: |
| Quoted-gap, N=5 | $72,920.87 | $1,448,419.75 |
| Book logistic, N=3 | $72,920.87 | $1,411,625.59 |
| Quoted-gap, N=3 | $52,909.79 | $1,109,836.04 |
| Monitor-all | $72,920.87 | $1,452,791.40 |

The difference between temporary reservations and held-basket funding is large.
These figures make the uncapped assumption explicit; the roughly $6,000 return
does not imply a strategy needing only a small amount of cash. Actual settlement
timing could release funds sooner, but it is not reconstructed here.

Opportunity-to-trade conversion counts selected, fully observed positive hours
with at least one attempted or completed basket. It does not measure the share
of all market opportunities captured. A selected positive hour may have no trade
because an unchanged quote is reserved or another attempt is in flight.
For logistic at N=3, **74.9%** of selected known-positive hours produce at least
one completed basket; the corresponding quoted-gap rate is **74.0%**. Individual
attempt completion rates are higher because the hourly denominator includes
positive states skipped by the scheduler.

## Reproduction and limits

Run [notebook 07](../../notebooks/07_economic_replay.ipynb). Default execution
verifies and displays saved outputs. Set a fresh `RUN` directory and enable
`RUN_REPLAY` to recompute without overwriting a completed run. Core implementation
is `src/simulator/opportunities.py`; audited tables, figures and comparisons are
built by `src/reporting/opportunity_replay.py`.

The original order-book, entry, sizing and scheduler helpers are reused unchanged.
Model predictions, raw books, source files and outputs are hash-checked. No
parameters are refitted against simulated losses and no final-test rows are used.
The no-reuse rule conservatively reserves whole ask versions, including residual
depth. Changed quoted books may be reused, but counterfactual market impact and
true replenishment are not modelled. Other limitations include source-time
alignment, unmeasured feed latency, queue priority, competition, frozen contract
membership and the retrospectively selected, small event universe.

Validation: **142 tests pass**, including latency-induced partial losses,
unknown-label selection, reservations across hour boundaries and cash accounting.
Notebook 07 executes all six code cells. The 100ms quoted-gap and monitor-all
summaries match the earlier persistent benchmark at both N=5 and N=3, across
folds and event scopes, including P&L, attempt counts and drawdown. Cumulative
curves reconcile to the summary totals, monitor-all is identical in both budget
panels, and all replay/report artifacts match their saved hashes.

The appropriate conclusion is that ranking can help allocate a scarce monitoring
budget, while execution assumptions can change the economic ordering of policies.
The research should report both layers instead of defining a pricing opportunity
by the profitability of a particular simulated execution. No final winning
strategy or commercial profitability is established by these development results.
