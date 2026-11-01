# Order-book replay and P&L

The prediction layer chooses which events to monitor. This simulator evaluates
what those choices might earn under a declared execution scenario. Models do
not learn its losses or use its P&L to choose hyperparameters.

## Primary scenario

| Assumption | Setting |
| --- | --- |
| Monitoring budget | N=3 primary; N=5 secondary |
| Scan and monitoring period | One hour, nonoverlapping |
| Clock | Source timestamps and backward as-of books |
| Total signal-to-arrival delay | 100 ms |
| Persistence / confirmation wait | Zero; attempt immediately when a new signal is observed |
| Capital and share caps | None; recorded available depth bounds size |
| Minimum quantity | At least five integer shares and every leg's exchange minimum |
| Entry | Equal quantity per leg, fixed decision-time limits, independent per-leg FOK support |
| Qualification | Positive depth-integrated margin and positive worst-limit margin |
| Extra attempt costs / exchange fees | Zero in the frozen-cohort scenario |
| Stochastic failure probability | None imposed |
| Quote-age ceiling | None |
| Partial-basket unwind | Sell filled legs at available bid depth one second after arrival |

100 ms is an assumed total delay, not an added provider receive-time delay or
a measured real trading latency. Historical snapshots do not establish queue
priority, competing consumption or actual exchange fills.

## Sizing and submission

For each basket, the optimizer evaluates integer quantities at relevant depth
breakpoints. With piecewise linear costs, the best supported quantity lies at
an interval endpoint. Every token must support the quantity at compatible
ticks and minimum size. Ties choose the smaller quantity.

The quoted dollar margin is quantity minus the sum of integrated ask costs.
The reserved limit cash is quantity times the sum of marginal order limits.
Requiring positive margin even at those worst limits prevents an apparently
cheap deep basket from submitting limits whose joint cost exceeds its payoff.

At a simultaneous signal time, one event-wide scheduler chooses the qualifying
basket with the largest decision-time dollar margin, with a deterministic
basket-ID tie break. It does not consult future arrival outcomes to choose the
order. The event has only one attempt in flight at a time.

At arrival, each leg is assumed fully filled if the recorded ask depth supports
the full requested quantity within its frozen limit. Otherwise that leg does
not fill. FOK applies per leg; it does not make a multi-market basket atomic.
No retry is inferred from the eventual duration of the quoted signal.

## Outcomes and P&L

| Outcome | Accounting |
| --- | --- |
| All legs fill | Conservative guaranteed basket payoff minus integrated purchase cost |
| No legs fill | Zero execution P&L, with no imposed attempt charge |
| Some legs fill and can be unwound | Bid proceeds at unwind minus purchase cost of the filled legs |
| Arrival state or required unwind is unknown | P&L remains missing; the attempt is counted separately |

For example, buying ten shares of two legs at $0.40 and $0.50 costs $9 and
completes a basket worth at least $10: $1 margin. If only the first leg fills
and its unwind bid is $0.30, the simulator loses `10*(0.30-0.40) = $1`.
This loss follows the observed depth path; there is no random failure deduction.

Complete-set YES baskets pay exactly $1 per share under the audited exhaustive
membership. A ladder broader-YES/narrower-NO basket may pay $2 in the intermediate
outcome, but the study books only its guaranteed $1 lower bound. It does not
forecast the resolution outcome to count the extra payment.

Completed margins enter P&L at completion; partial results enter at unwind.
These are economic margin recognition times, not settlement cash receipts.
Maximum drawdown starts with a zero P&L balance. Unknown P&L is excluded from
known-return sums and reported separately, never treated as a measured zero.

## Persistent liquidity and cash

The replay starts empty at the first evaluated hour and preserves in-flight
state and shared-token ask-version reservations through every hour and weekly
refit. Renaming or refitting a model does not reset its liquidity ledger.
Unchanged displayed asks cannot be consumed repeatedly by different pairs or
successive scans. Token histories are bounded by their own audited recording
ends; there is bounded terminal context for outstanding unwinds.

The frozen events share no tokens across different event schedulers. A new
universe with cross-event token overlap requires a global shared ledger.
Within a ladder event, pairs can share tokens and the event scheduler reserves
them jointly.

Unlimited funding is assumed, so P&L should not be presented as ROI. The
`no_settlement_funding_usd` statistic is a separate cash scenario: order cash
is reserved, purchases consume cash, known partial unwinds return proceeds,
and complete baskets release no settlement cash during the study. Unknown
orders retain conservative reservations. `peak_inflight_reservations_usd`
measures simultaneous reservation cash, not total settlement funding.

## Comparisons and artifacts

Every deterministic policy and monitor-all runs at 0, 50, 100, 250 and 500 ms
for N=3 and N=5. Twenty random seeds run at 100 ms. Excluding the December Fed
event removes it before reranking and replays N=3 at 100 ms.

Selections stay fixed across latency scenarios. Each policy/scenario has an
independent persistent ledger. Monitor-all is a monitoring-resource reference;
path-dependent order competition means it is not a formal P&L upper bound.

`artifacts/replay/attempts.parquet` records signal, submission, arrival, unwind,
quantity, limits, filled legs, costs, outcome and P&L. Scheduler audits explain
blocked/competing signals. `summary.csv` and `artifacts/tables/` contain complete
and partial contributions, known/unknown attempts, drawdown, quantities, cash
scenarios and policy/week comparisons. Figure captions identify the population,
budget and latency used.
