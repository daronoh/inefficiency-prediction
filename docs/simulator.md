# Order-book replay simulator

The simulator asks what the declared strategy could earn from the recorded
orders when following each model's actual event selections. It is a historical,
displayed-depth simulation, not an exchange matching engine or evidence of
confirmed fills. Notebook [07_economic_replay.ipynb](../notebooks/07_economic_replay.ipynb)
runs the fixed opportunity-model comparison. Notebook 03 preserves the earlier
P&L-target benchmark.

Economic replay is the second research layer. The current first-layer models in
notebook 06 learn opportunity occurrence or peak displayed dollar margin without
using arrival delays, fill outcomes or unwind losses as targets. Their scores
are not P&L forecasts. The second layer holds these predictions fixed while
changing arrival latency: 0, 50, 100, 250 and 500ms. The primary arrival is 100ms;
the one-second unwind/hold is fixed throughout. Budgets of three and five events
are compared, with and without December Fed in the ranking pool. Twenty seeded
random policies and monitor-all provide references. No models are refitted and
the final test remains closed. Zero arrival is an optimistic same-book execution
reference, not an upper bound on every policy's path-dependent returns.

The existing economic benchmark uses **100 ms arrival with no artificial cash or share cap**
for rebuilt targets, refitted models and policy replay. Each model supplies its
own hourly choices. The arrival delay is an assumption, not a historical
measurement. No additional failure probability is imposed; partial baskets
caused by the recorded arrival books still count.

`src/simulator/opportunities.py` orchestrates the new experiment using the same
entry/execution/scheduling helpers. Each policy, latency and budget has an
independent ledger; reservations persist across hours and folds within its run.
Raw input hashes are verified and shared tokens across distinct events are
rejected, because the current reservation ledger is per event. Outputs under
`results/opportunity_replay/` include frozen selections, attempts, scheduler skip
reasons, summaries and hashes. Notebook 07 also compares first-layer capture
against economic outcomes; quoted-value capture is never treated as P&L.

## Funding measures

The largest single-order reservation and peak simultaneous in-flight reservations
describe temporary order funding. They omit capital still tied up in completed
baskets. Cumulative purchase cash measures turnover, not required capital.

The additional `no_settlement_funding_usd` scenario assumes completed baskets
receive no settlement cash during the experiment. It reserves order limits at
submission, pays known entry costs at arrival and returns known partial-unwind
proceeds. Unknown arrivals retain their full reservation; unknown exits return
no cash. Simultaneous submissions are funded before releases. The peak cumulative
cash requirement is reported separately from P&L and is not an estimate of live
portfolio capital or a return-on-capital denominator. Full-period funding is the
primary measure; fold summaries do not include inherited holdings.

Completed-basket P&L is the contractual payout floor minus purchase cost, booked
at arrival for the P&L curve. Partial P&L is booked at unwind. This is economic
margin accounting, not a settled cash balance. Unknown P&L is excluded and counted.

## From ranking to attempted trade

1. At a scheduled scan, rank every snapshot-eligible event using the selected
   model's book features, event metadata, or both. Select the top N for the following W
   minutes. Unknown future labels are not used to filter the shortlist.
2. For selected events, inspect each relevant update using backward-as-of member
   books. NegRisk baskets buy all YES; ladder baskets buy broader YES plus
   narrower NO. Both have a minimum payout of $1 per equal-share set.
3. Discovery checks full displayed depth. A candidate
   requires clean two-sided books, sufficient ask depth, valid size/ticks,
   positive gross margin and positive margin at the proposed worst fill prices.
4. At entry, choose the integer size with the largest displayed gross profit,
   breaking profit ties in favour of smaller size. Minimum quantity is five,
   or the largest member's minimum order size if higher. There is no artificial
   maximum: the shallowest required leg and profitable limit prices bound size.
   Reserved cash is quantity times the sum of submitted limit prices.
5. Submit immediately. Fix quantity and each leg's maximum purchase price before
   inspecting books at the assumed arrival time, 100 ms later.

For q sets, purchase cost C and per-leg limits L_i, entry requires
`q - C > 0` and `q * (1 - sum(L_i)) > 0`.
The optimizer evaluates integer endpoints of marginal-price depth intervals;
profit is linear between those endpoints. This avoids allocating an array for
every share when millions of shares are displayed.

Uncapped discovery is rebuilt from raw books, including training windows and
quantities excluded by the historical grid and limits. The snapshot-eligible
population is retained, so results do not measure opportunities in events or
windows excluded by the declared universe and eligibility rules.
The configurable additional per-attempt cost is zero in the reported run.
Exchange trading fees are zero for these markets in the development period.
Verification combines the selected markets' frozen `feesEnabled=false` flags
with the [dated exchange policy](https://docs.polymarket.com/changelog/predictions):
fees introduced on 5 January 2026 applied to 15-minute crypto markets, outside
this baseline. The monthly Bitcoin and Ethereum ladders are a different market
type. Funding and operating costs remain unmeasured and excluded from P&L.

Signals occur when a qualifying gap begins and at a new scan boundary if it is
already present. There is no minimum-duration filter and no continuous retry of
an unchanged episode. A first observation after missing data is not proof of a
new gap; the new-gap diagnostic requires an observed preceding non-opportunity.
The search is bounded to these baskets, sizes and entry rules.

## Fill and failure accounting

At arrival, each leg fills entirely if sufficient depth remains within its fixed
limit and the size/tick checks pass; otherwise that leg does not fill. This is
an idealised full-or-zero order assumption. The basket itself is not atomic.

| Outcome | Recorded P&L |
| --- | --- |
| All legs fill | Minimum basket payout `q` minus total purchase cost |
| No legs fill | Zero |
| Some legs fill | Sell the acquired legs at recorded bids one second after arrival; proceeds minus their purchase costs |
| Arrival or required unwind cannot be evaluated | Unknown; never converted to zero profit/loss |

Entries must arrive before the monitoring window and contract-rule deadline.
Risk management of an already-submitted basket can continue afterward if the
necessary development observations exist. No final-test book may price an exit.
The current conservative usability rule also treats empty or crossed arrival
books as unknown rather than assuming a fill outcome.

For ladder baskets the simulator credits only the guaranteed $1 minimum, even
when some resolutions would pay $2. P&L therefore values retained completed
baskets at their guaranteed payoff; it does not simulate waiting for settlement
or make settlement proceeds immediately available as cash.

## Preventing duplicate use of liquidity

There is at most one attempt in flight per event. Simultaneous candidates are
ordered by their decision-time gross margin, never by later fill results.
The scheduler reserves shared token ask states across all overlapping baskets.
A later timestamp with identical ask prices and sizes does not create fresh
liquidity. Changed quotes can become usable after the in-flight restriction ends.
This is a conservative quote-version reservation scheme, not an order-ID ledger
or a precise model of replenishment.

Each model, rule and random policy receives its own chronological replay and
reservation ledger. Reservations persist across selected windows and fold
boundaries. Selecting monitor-all's winning trades afterward would give a
wrong result because a different shortlist changes reservations and available
attempts. Profits from overlapping pairs or repeated snapshots cannot simply
be summed.

Labels have a different purpose: each standalone window begins with an empty
ledger to measure net P&L from monitoring that event. The primary target includes
all returns from attempts initiated in that window, including losses. Activity
and positive-attempt occurrence are separate secondary targets.
Consequently, **summing label-window P&L does not give a strategy's P&L**.
The policy replay includes all attempted outcomes, including losses and witnessed
trades in partly observed windows; supervised metrics use complete labels.

## Outputs

Within a model run, `execution/` (or its selected repeat) contains:

- `attempts.parquet`: model, event, signal/decision/arrival/unwind times, fixed
  quantity and limits, filled legs, entry cost, status and P&L.
- `summary.csv`: attempts, completed profits, partial/unknown outcomes, known
  net P&L, drawdown, maximum reserved cash and cumulative purchase cash per policy,
  fold and evaluation scope.
- `paired_comparisons.csv`: exploratory paired topic-family resampling against
  the quoted-gap rule, with unknown outcomes identified.
- `run.json`: input digests, execution settings and interpretation limits.

Event-level summaries can be calculated from `attempts.parquet`; a second
export of the same accounting is not stored.

The notebook plots cumulative known P&L at completed-entry or partial-unwind
time, with separate panels for model families and a second view showing the
dollar advantage over the quoted-gap rule. Unknown outcomes are reported
alongside known subtotals. There is no random or additional assumed failure
chance. Failures arise from insufficient arrival depth within the submitted
limits; recorded partial-basket losses remain in known P&L.

Cash spent across all attempts is a turnover measure, not starting capital or a
portfolio return. Drawdown uses the known payoff-valued P&L path, not a mark-to-market
valuation of positions held to settlement. The simulator credits only the
guaranteed minimum basket payout.

## Concentration and historical reference scenarios

The primary scope contains all eligible events. A post-hoc concentration check
removes the December Fed-decision event from the monitoring pool, reranks the
remaining events and replays the resulting choices. It keeps the same fitted
models, so it is distinct from family-holdout training. The latter excludes each
evaluated broad topic from earlier training rows.

Historical reference runs remain under `results/baseline/`: the capped 500 ms
classifier study, capped arrival/unwind timing comparisons and uncapped 100 ms
execution with those original rankings held fixed. Current profitability models
use rebuilt labels and quote features; comparisons with historical runs therefore
change more than the learning objective. Current within-study comparisons share
all data and execution assumptions.

The scheduler conservatively holds an event until the configured unwind time
even after complete fills, and reserves quote versions observed at that time.
Changing the unwind parameter therefore changes both exit timing and subsequent
entry availability. The zero-delay/no-hold scenario is an optimistic benchmark
within the same capped strategy, not an estimate of all market-wide arbitrage.
Positive-depth candidates can fill completely at observation in that scenario;
this is an assumption, not evidence of an atomic multi-market execution route.

An actual HFT system's latency requires
measurements of feed, network and matching latency. Today's performance cannot
automatically be projected into this historical period. FOK applies to each
order; [batch responses](https://docs.polymarket.com/trading/place-orders) can
succeed or fail independently. Faster execution can reduce incomplete baskets
without making them impossible. Holding an incomplete basket instead of
unwinding would require a separate inventory-risk and valuation policy.

## Limits of the interpretation

Carry-forward assumes all relevant book changes were captured. There is no age
cutoff, although each token's first/last recordings bound offline label and fill
evidence. Source timestamps and an assumed 100 ms or 500 ms delay do not measure actual
feed delivery, computation, acknowledgements or different leg latencies.

The model does not include queue competition, adverse selection after submission,
partial quantities within one leg, a common portfolio cash budget, settlement
cash release, conversion operations, funding or operational costs. The uncapped
scenario assumes sufficient funding for every order. The reference $1,000
cap applies separately to each attempt. Contract membership and resolution
semantics remain frozen assumptions. Reported margins are therefore conditional
simulated P&L, not proof of commercially executable arbitrage.

## TabM policy comparison

Notebook 08 evaluates the TabM extension through the same persistent scheduler,
quantity selection and fill accounting as notebook 07. Eight declared TabM or
TabM/forest policies are replayed at 0, 50, 100, 250 and 500 ms, at N=5 and N=3,
with both the full population and December Fed excluded before ranking.
The primary scenario remains **100 ms, uncapped capital and shares**, with no
imposed random failure probability. Existing classical-policy replay artifacts
are verified and reused for comparison. Execution outcomes do not tune TabM.
See [the TabM findings](reports/tabm.md).
