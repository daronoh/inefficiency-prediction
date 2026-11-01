# 1 Introduction

## 1.1 Motivation

Prediction markets turn uncertain future outcomes into tradable contracts. Their
prices provide information about participants' beliefs, but related contracts
also impose logical constraints on one another. A complete collection of
mutually exclusive outcomes should not collectively cost less than its
guaranteed payoff. Likewise, a narrower condition cannot occur without a
compatible broader condition. Violations can produce structural arbitrage
baskets without requiring a forecast of the eventual winner.

The practical difficulty considered here is deciding where to look. Continuously
processing every market's depth, checking relationships and maintaining order
state consumes resources. This dissertation represents that constraint as a
fixed event-monitoring budget. At scheduled times, a screening policy observes
all eligible events and chooses a small subset for intensive monitoring during
the following period. It must allocate resources before knowing the future
opportunity path.

The budget is an experimental abstraction. The study does not measure exchange
subscription limits or demonstrate that monitoring the entire contemporary
platform is impossible for a particular trading firm. Instead, it asks whether
limited monitoring can preserve most of the observable opportunity available
in a fixed historical cohort. A useful screen could support systems where
relationship checking, execution infrastructure or risk supervision is costly.

## 1.2 Research question and objectives

The central question is: can features computed from an event's scheduled
order-book state predict which events will offer depth-supported structural
arbitrage during the next hour, sufficiently well to allocate only three
monitoring slots? Five slots provide a secondary budget comparison.

Four objectives make the question testable. First, construct event-level
examples whose features are available at the scan and whose future labels
distinguish negative evidence from missing evidence. Second, compare simple
observable rules with linear, tree-based and recent neural tabular models,
including bounded tuning and rank ensembles. Third, identify whether additional
metadata and modelling complexity improve capture. Fourth, evaluate the same
monitoring decisions under separate execution assumptions, including latency
and incomplete baskets.

The distinction between opportunity and execution is central. A model should
learn where useful opportunities tend to occur, rather than learn losses
created by one particular latency or unwind setting. Conversely, strong
opportunity capture does not establish economic profitability. Both layers are
needed, and neither should substitute for the other.

## 1.3 Contributions and scope

The contribution is an implemented research workflow for budgeted event
selection, rather than a new general learning architecture. It combines
coverage-aware occurrence and value labels, comparable chronological models,
and persistent economic replay. The submission includes the relevant raw
books, frozen contract membership, prepared examples, trained weekly models,
28 focused tests, an executable notebook, CSV tables and a catalog of 50
figures. Saved checkpoints can be verified or fully reconstructed from raw data.

The empirical scope is a selected Dome cohort: 46 negRisk complete-set events
and five ladder events. Two buy-side basket structures are implemented. The
study does not cover arbitrary natural-language relationships, all sell-side
or complement constructions, or the entire Polymarket universe. Model choice
is a retrospective comparison of chronological predictions; it is not described
as a pristine, untouched final-test selection. These boundaries determine the
strength of the conclusions.

{{FIGURE:1}}

# 2 Background and related work

## 2.1 Forecasting and structural consistency

Wolfers and Zitzewitz (2004) discuss prediction markets as mechanisms that
aggregate dispersed information about uncertain outcomes. Their emphasis is
the quality and interpretation of market-generated forecasts. This motivates
studying these markets but does not establish that every relationship between
contracts is continuously arbitrage-free. Forecast accuracy and structural
consistency are distinct: an event can be difficult to predict while its
contracts still admit a guaranteed-payoff basket.

An event in this study is one ranked object, such as a particular Fed decision.
It can contain many binary markets. A token is one YES or NO outcome share and
has its own order book. A basket is an audited set of tokens with a known
payoff lower bound. A candidate is an event eligible for a scan; the term does
not mean an opportunity already exists. A signal is an observed interval in
which a basket passes the opportunity test. An attempted order, a completed
basket and a profitable executed trade are separate observations.

## 2.2 Empirical arbitrage research

Saguillo, Ghafouri, Kiffer and Suarez-Tangil (2025) investigate arbitrage on
Polymarket, distinguish within-market rebalancing from relationships across
markets, and address the cost of searching related conditions using heuristic
reduction. Their evidence motivates asking whether mispricing exists rather
than assuming a perfectly consistent platform. The task here differs: it
predicts which events deserve future monitoring under an explicit slot budget.
The cited market-wide profit estimate is neither a label nor a profitability
benchmark for this dataset. Historical depth coverage, basket definitions and
execution assumptions must be established independently for the present study.

This distinction also limits the role of semantic discovery. A market title
suggesting a related subject is insufficient to guarantee compatible settlement
rules. The implemented cohort therefore uses frozen audited relationships.
Membership is an upstream assumption, whereas the learner ranks events after
membership has been fixed. General automatic relationship discovery remains
outside the evaluated strategy.

## 2.3 Tabular learning methods

Breiman (2001) introduces random forests as ensembles of randomized tree
predictors. For this application, their ability to express nonlinear feature
interactions provides a useful comparison with a regularized linear score.
The original method does not resolve temporal leakage, class imbalance or
resource-constrained evaluation; those require application-specific choices.

Chen and Guestrin (2016) present XGBoost as a scalable tree-boosting system.
Here it provides a sequential tree ensemble alongside the forest's averaging
approach. Its inclusion tests a different tabular inductive bias, rather than
presuming that a stronger general benchmark result implies better event
ranking. All compared families receive the same chronological populations and
bounded selection objective.

Grinsztajn, Oyallon and Varoquaux (2022) compare neural and tree methods across
tabular benchmarks and examine why trees can remain competitive on medium-sized
tables. Their findings justify retaining strong classical controls. However,
general tabular benchmarks are not evidence about rare, temporally dependent
prediction-market opportunities. A comparison in the present setting is still
necessary.

Gorishniy, Kotelnikov and Babenko (2025) introduce TabM, an ICLR 2025 method
using parameter-efficient ensembling within a tabular neural model. It supplies
a recent neural comparator rather than a generic default multilayer network.
This study uses the official implementation and preserves member-wise training
losses and prediction averaging. Its smaller bounded search and three-seed
two-stage adaptation are application choices, not a replication of the paper's
full benchmark programme. Success on broader benchmarks does not guarantee
superiority on this small, concentrated cohort.

## 2.4 Evaluation and interpretation

Bailey, Borwein, López de Prado and Zhu (2017) discuss overfitting when backtests
are used to select strategies and propose a framework for assessing it. The
present study does not implement their probability-of-backtest-overfitting
estimator. Its practical response is to separate earlier-only tuning from
forward prediction, preserve simple controls, report concentration and qualify
retrospective policy selection. Chronological prediction alone does not remove
all uncertainty created by a selected cohort and multiple policy comparisons.

The resulting research gap is a specific resource-allocation problem. Evidence
that arbitrage has occurred does not show that an hourly screen can anticipate
it. A high-performing generic learner does not show that its selected events
will produce superior executed returns. The experiments below connect those
questions while retaining their separate targets and assumptions.

# 3 Problem formulation

## 3.1 Scheduled ranking

Let \(\mathcal{C}(t)\) be the eligible event set at scheduled scan \(t\). For each event \(e\), the
screen constructs \(\mathbf{x}(e,t)\) using only current and earlier recorded states. A
policy assigns score \(s(e,t)\) and selects the highest-scoring \(\min(N,|\mathcal{C}(t)|)\)
events. Ties use the event identifier deterministically. Future labels do not
filter \(\mathcal{C}(t)\) or the selected rows.

The selected events are monitored over the half-open interval \([t,t+H)\), where
\(H\) is one hour. An opportunity at the scan counts in that interval; a new
opportunity exactly at \(t+H\) belongs to the next window. The implementation makes
cadence and horizon configurable but requires them to match, preventing
overlapping allocations from silently exceeding the monitoring budget. The
reported models and results use only the one-hour setting.

When \(|\mathcal{C}(t)|\leq N\), every candidate is selected. These hours cannot
distinguish exclusion quality. The study therefore reports candidate competition,
competitive-hour recall and the larger-budget comparison alongside pooled
capture. Equal slot counts also need not imply equal processing cost: events
can require very different numbers of token books.

## 3.2 Guaranteed-payoff baskets

For an audited mutually exclusive, exhaustive negRisk event, buy one YES share
of every member. Exactly one member resolves YES, so one complete basket pays
$1. For \(q\) complete shares, the conservative payoff is \(q\) dollars. All members
are required; an unavailable leg cannot simply be dropped.

For a ladder, let \(A\) be a broader condition and \(B\) a narrower condition such that
\(B\Rightarrow A\) under compatible resolution rules. Buying \(\mathrm{YES}(A)\) and \(\mathrm{NO}(B)\) pays
one dollar if neither occurs, two dollars if A occurs without B, and one dollar
if both occur. The impossible state B without A is excluded by the audited
implication. The simulator uses the guaranteed one-dollar lower bound and does
not add a speculative payment for the intermediate outcome.

For a basket with \(m\) members, let \(C_j(q,t)\) be the integrated ask cost of buying \(q\) shares of member \(j\) at time
\(t\). The quoted lower-bound margin is

\[ G(q,t)=q-\sum_{j=1}^{m}C_j(q,t). \]

Reading only best
asks is insufficient because depth can be exhausted before \(q\) shares are filled.
A qualifying quantity must meet exchange minimums, integer sizing and tick
constraints. The study requires \(q\geq 5\) and places no artificial cash
or share cap.

The marginal limit \(L_j(q,t)\) is the worst ask price required for leg \(j\). Entry
qualification additionally requires

\[ q-q\sum_{j=1}^{m}L_j(q,t)>0. \]

This guards against
submitting independent limit orders whose worst supported combined cost
exceeds the guaranteed payoff, even if their integrated displayed cost looks
favourable. There is no fixed $0.50 profit deduction or minimum-dollar threshold.

## 3.3 Occurrence, value and missing evidence

The occurrence target \(y(e,t)\) is one when any active basket qualifies during
the following hour and zero when none does. Both classes require the active
window to be fully decidable from the recorded states. If a required interval
is unknown, \(y\) remains missing even when a positive gap was witnessed elsewhere
in that hour. This symmetric requirement prevents partially observed positives
from being compared with artificially complete negatives.

The value target \(v(e,t)\) is the largest qualifying one-basket dollar margin
observed during the window. It is zero for a known negative and missing for an
unknown outcome. Repeated snapshots do not add value. Multiple ladder pairs
are not summed because their depth can overlap. Consequently, summing \(v\) over
event-hours is an evaluation weight, not a statement of cumulative tradable
profit.

A secondary diagnostic distinguishes opportunities already visible at the scan
from newly observed onsets afterwards. The first layer is not restricted to
classifying whether a positive gap is visible now. It is evaluated on the
future monitoring period, including the possibility of new opportunities after
the selection decision.

## 3.4 Metrics and references

Positive-hour recall is the number of selected known positive event-hours
divided by all known positive event-hours. Quoted-value capture is the selected
known \(v\) total divided by the full known \(v\) total. The primary model-comparison
criterion is pooled value capture at N=3, with occurrence capture and average
precision used to resolve ties. Precision among known selected rows, average
precision, unknown-slot share and token-monitoring burden provide additional
perspectives.

Random selection has an exact expected inclusion probability \(\min(N,m)/m\) in a
scan with \(m\) candidates. Its expected positive and value capture can therefore
be calculated without Monte Carlo noise. The economic random baseline instead
uses 20 explicit fixed-seed policies, because trading path dependence prevents
simple linear expectation of P&L. Monitor-all supplies a full-resource
reference. Neither a zero-positive week nor a zero-value week defines a recall
denominator; those metrics remain undefined rather than becoming zero.

# 4 Data and example construction

## 4.1 Source, cohort and coverage

The source is saved Dome token-level L2 depth with frozen Polymarket metadata.
The supplied raw cohort contains 994 token histories, approximately 1.01 GB.
The calendar extends from 14 October 2025 to 3 February 2026 at 08:00 UTC. Its
112 days and eight hours are not continuous complete coverage for every event.
Only the two audited basket types enter the study.

There are 51 supported events: 46 negRisk and five ladders. The complete
prepared table has 29,308 eligible event-hours, 23,452 known targets and 470
positive targets. The forward interval contains 44 distinct events. Some
supported events contribute earlier training history but no usable forward
scans. The relatively small ladder sample is a limitation of the available
complete histories and audited relationships, not evidence that ladder
arbitrage is rare throughout the market.

{{TABLE:population}}

{{FIGURE:5}}

## 4.2 Raw depth and as-of alignment

Each raw row includes source snapshot time, provider index time, ask and bid
level lists, minimum order size and tick size. Prices and sizes are checked for
finite valid values. Empty depth is distinguishable from missing or malformed
depth. Crossed books do not provide trusted entry evidence.

At a scheduled scan, each active token uses its latest source-time row at or
before the scan. Equal source timestamps retain the last provider-indexed
version with stable ordering. Tokens may have different last-update times;
their book ages quantify that difference. No future book seeds an earlier
state. During replay, each token is additionally bounded by its own audited
recording end.

Using source time approximates observation through an idealised feed subject
to the declared total execution delay. Provider receive-time delays are not
reconstructed. A 100 ms scenario therefore does not verify that the historical
collector delivered each source update within that delay. Removing a freshness
cutoff likewise avoids arbitrary exclusion but does not prove that all carried
states remained available at the exchange.

## 4.3 Eligibility and full-window labels

An event enters a scan when its active basket is valid, all required members
have been seen and the scan lies within the offline event observation extent.
This is separate from label coverage. It permits an event with a usable
current snapshot but an unknown future interval to compete fairly for a slot.
Training subsequently excludes that unknown target; selection does not.

Coverage is evaluated over active basket intervals. Normally every required
leg must be observed. An observed blocker, such as empty asks on one required
leg, can establish that a basket cannot be bought even if another leg is
missing. Otherwise missing member evidence makes the interval unknown. Both
occurrence and value require full-window decidability under this rule.

Raw reconstruction preserves an audited preprocessing boundary at 8 January
2026 00:00 UTC. Earlier coverage ends at the last available state within its
partition; later coverage uses audited per-token endpoints and bounded exit
context. This boundary is independent of training and forward-evaluation
origins. Preserving it avoids silently turning previously unknown edge windows
into known labels. Scheduled feature lags remain continuous where the
preceding scan is available.

{{FIGURE:7}}

## 4.4 Features and metadata

There are 11 current book summaries: active baskets, active tokens, seen-member
fraction, clean-member fraction, mean spread, maximum spread, total ask depth,
maximum book age, best quoted dollar margin, size-feasible basket fraction and
positive quoted-margin basket fraction. Each has one consecutive-scan lag and
one difference from that lag, giving 33 book features. Lag construction precedes
snapshot eligibility filtering, and a gap in the scheduled grid yields a
missing lag rather than an invented observation.

The quoted-gap feature maximises integrated quoted margin over supported
integer sizes and active baskets. It may be negative. It omits the positive
worst-limit-margin requirement used by opportunity qualification and execution;
it is a descriptive ranking signal, not an instruction to submit every basket
that it scores. Missing quoted margins receive the rule's lowest score.

Three optional metadata features describe event type, fixed broad topic and
days since the earliest frozen member's creation. Creation age is a proxy for
event maturity, not a verified historical listing date. Negative or missing
ages stay missing. Broad topic uses fixed title-based categories; event IDs,
winners, realised status, future volume and execution returns are excluded.
Frozen topic text and membership retain a retrospective selection limitation.

Metadata ablation compares fixed book-only and fixed combined models with
identical family settings. It therefore isolates adding the three metadata
features more cleanly than comparing separately tuned models. Feature
correlations are descriptive; they are not causal explanations or proof of
independent predictive contribution. The current seen-member fraction is
mechanically constant among eligible scans, which illustrates why feature
availability alone should not be confused with useful predictive variation.

# 5 Prediction methodology

## 5.1 Rules and model families

Three rules establish low-complexity references. Quoted gap ranks the best
integrated basket dollar margin at the scan. Depth ranks total active ask
shares. Freshness ranks the negative maximum member book age, preferring
events whose oldest active state is more recent. None of the rules needs the
eventual label, and a negative quoted gap can still receive a relative rank.

Logistic regression models a regularized linear boundary for opportunity
occurrence. Its simplicity provides a useful test of whether current feature
combinations already explain most ranking quality. Random forest models
nonlinear interactions through an ensemble of trees. XGBoost supplies a
regularized boosted-tree alternative. Each family has fixed book-only, fixed
combined-feature and tuned combined-feature policies.

Two-stage models estimate expected opportunity value from an occurrence
probability and a conditional positive-value estimate:

\[ \widehat{v}(\mathbf{x})=\widehat{\mathbb{P}}(y=1\mid\mathbf{x})\,\widehat{\mathbb{E}}[v\mid y=1,\mathbf{x}]. \]

The probability gate uses natural
prevalence, rather than class weighting intended to rebalance classification.
The amount model trains on positive quoted values only, with nonnegative
predictions. A fixed forest hurdle is a reference; TabM uses the same two-stage
decomposition. There is no hard probability threshold before ranking.

Three ensembles average within-scan percentile ranks: the three tuned
classifiers; tuned logistic, tuned forest and TabM; or the two value models.
Percentiles permit comparison of scores with different scales. Weights are
equal and fixed. Combining models is an empirical comparison, not an assumption
that ensembles must improve upon their strongest component.

## 5.2 Preprocessing and bounded tuning

Classical numeric preprocessing uses training-fitted median imputation with
missing indicators, a signed log transform and standardisation. Metadata
categories use training-fitted one-hot encoding with unknown categories
ignored. The same pipeline interface keeps feature order and preprocessing
inside each fitted artifact.

Each classical family evaluates eight configurations. Logistic settings vary
regularisation strength, L1/L2 penalty and class weighting. Forest settings
vary depth, leaf size and feature subsampling with 200 trees. XGBoost settings
vary tree count, depth, learning rate, child weight, subsampling and
regularisation. The exact grids, search results and selected settings are
exported, rather than replaced by a claim of exhaustive tuning.

TabM uses 32 implicit members per stage and four configurations selected from
the declared architecture grid. Numeric preprocessing is fitted on training
data; continuous variables use quantile transformation and categories use
one-hot encoding. Member losses are computed separately, and inference averages
member probabilities or amount predictions. Batch size is 1,024, the maximum
is 60 epochs and early-stopping patience is 10. Inner tuning uses one fixed seed.
Weekly refits average three seed-specific gate-times-amount products. Refit
epochs are the ceiling of the median selected inner best epoch, carried forward
until the next tuning origin.

## 5.3 Expanding chronological evaluation

The initial 28 calendar days supply training history. Forward evaluation runs
from 11 November 2025 to 3 February 2026 at 08:00 UTC: 2,024 scheduled hours,
or 84 days and eight hours. There are 13 weekly refits, with an eight-hour final
block. A training example enters only if its known target window finishes
strictly before the fitting origin. Later observations can enter subsequent
fits after that requirement is satisfied.

Tuning occurs at the first origin and every fourth weekly origin when at least
seven forward days remain: 11 November, 9 December and 6 January. Two inner
chronological blocks lie entirely in the earlier history. Their full target
horizon is purged before each inner fit. Configurations maximise pooled
captured quoted value at N=3, then captured positive hours, mean inner average
precision and declaration order. No subsequent weekly outcomes determine that
origin's hyperparameters, early stopping or ensemble weights.

There are 168 inner candidate evaluations. The saved weekly audit contains 199
distinct fitted artifacts or stages; identical selected and fixed classifier
settings can share an artifact. They are not counted as independent supporting
evidence. The known training population grows from 7,652 rows initially to
23,452 at the final fit. The final origin does not imply recent useful data:
the last known training scan is on 27 January because later coverage is missing.

{{FIGURE:2}}

## 5.4 Model choice and uncertainty

The final recommended policy is selected by the pooled opportunity criterion
from the saved chronological comparisons. This retrospective policy choice
does not alter the already generated weekly scores. It also does not establish
untouched-test generalisation. Economic P&L is reported afterwards and does not
replace the opportunity criterion merely because a different model has higher
simulated returns.

Paired seven-day block resampling compares policies on the same scheduled
periods. Ten thousand draws preserve within-block dependence more faithfully
than resampling individual event-hours. There are only 13 blocks, including a
short last block. Intervals condition on the fitted models and observed cohort;
they do not include refitting, universe selection, missing outcomes or a
multiple-comparison correction. Event-exclusion analysis therefore remains
essential alongside intervals.

# 6 Economic replay

## 6.1 Separate execution layer

The economic layer fixes the first-layer selections and varies execution
assumptions. The primary scenario uses 100 ms total signal-to-order-arrival
delay, immediate submission, uncapped capital and shares, and available-depth
sizing. There is no imposed random failure probability, minimum signal duration
or quote-age ceiling. Entry constraints still enforce valid depth, exchange
minimums, ticks and positive worst-limit margin.

The frozen cohort uses zero exchange fees and zero extra per-attempt charges.
Saved market flags support this scenario; they are not a timestamped historical
fee-change ledger. The results should not be extrapolated to fee-bearing
contracts without rebuilding qualification, sizing and replay costs.

100 ms represents an assumed observation and execution system, not a measured
live fill delay. Other deterministic scenarios use 0, 50, 250 and 500 ms. The
zero-delay scenario is an optimistic recorded-state benchmark, not a verified
upper bound on the exchange's available profit. Every scenario retains the
same event selections and uses its own persistent ledger.

## 6.2 Submission, arrival and partial baskets

When a selected event produces a new qualifying signal, the scheduler sizes
the best supported basket using the decision-time books and freezes each leg's
price limit. If several pairs start together, decision-time quoted margin
determines their order, with a deterministic pair-ID tie break. Future arrival
outcomes do not decide which basket to submit.

At arrival, each leg is assumed to fill in full if the recorded ask depth can
support its entire requested quantity within its frozen limit. Otherwise that
leg does not fill. This is independent per-leg FOK support, not an atomic
multi-market transaction. Missing arrival evidence remains unknown rather than
being interpreted as a failed fill.

A completed basket records conservative payoff minus actual integrated entry
cost. If no legs fill, P&L is zero in this no-attempt-charge scenario. If some
legs fill, the simulator tries to sell them at recorded bid depth one second
after arrival. Their proceeds minus entry cost produce the partial result.
Missing or insufficient exit evidence leaves that result unknown. For example,
ten shares bought at $0.40 and unwound at $0.30 lose $1; there is no synthetic
percentage failure deduction.

The order is attempted when the signal becomes visible. Its eventual duration
is not used as an immediate-entry filter. Short-lived opportunities can thus
be positive first-layer labels while producing no fill or a partial loss after
latency. That difference is an intended measurement of execution sensitivity.

## 6.3 Persistent liquidity and accounting

Each event scheduler permits one attempt in flight at a time. Shared-token
ask versions remain reserved so that different ladder pairs or later scans
cannot spend the same unchanged displayed liquidity repeatedly. Model refits
and weekly boundaries do not reset these reservations. The supplied events
have no token overlap across separate event schedulers; another universe with
cross-event overlap would require a global shared ledger.

Complete-basket margins are recognised at completion and partial outcomes at
unwind. Recognition is distinct from receiving settlement cash. Known P&L sums
exclude missing outcomes and separately count them. Drawdown starts from a
zero balance and uses the resulting accounting-time increments.

Funding is unlimited in the primary comparison. A separate no-settlement
scenario accumulates purchase cash without releasing completed-basket proceeds
during the study, while allowing known partial-unwind proceeds and conservative
unknown reservations. It estimates a funding requirement under that scenario,
not deployed capital or an ROI denominator. Monitor-all uses more monitoring
resources; path-dependent order competition prevents treating it as a formal
profit upper bound.

# 7 Results

## 7.1 Population and monitoring competition

The forward population contains 19,766 event-hours from 44 events. There are
15,787 known outcomes, including 359 positive hours from 12 distinct events,
and 3,979 unknown outcomes. Median concurrent pool size is nine, the maximum
is 19, and 1,887 scheduled hours have more than three candidates. Thus the
primary budget usually requires exclusion, although competition and label
coverage vary materially over time.

At N=3, each deterministic policy selects 5,973 event-hours. The positive-hour
random expectation is 22.23%; expected quoted-value capture is 18.93%. These
are conditional expectations for the observed candidate sets, not estimates of
market-wide opportunity prevalence. Known targets sum to $8,838.31 of hourly
peak quoted value. That number is a weighting denominator rather than a
cumulative executable profit pool.

## 7.2 Primary opportunity comparison

{{TABLE:primary}}

The fixed book-feature random forest leads pooled value capture at N=3:
99.53%, with 340 of 359 known positive hours captured, or 94.71% recall.
Quoted gap captures 60.76% of value and 264 positives, or 73.54% recall. The
forest also captures 205 of 223 known newly arising opportunity hours that
had no opportunity at the scan, showing that the comparison includes more than
detecting currently positive quotes.

The quoted-gap rule's disadvantage is consistent with the decision timing. It
uses a single current dollar-margin statistic, whereas the label concerns
the next hour and includes later changes. A presently negative event may
subsequently offer valuable depth, and a presently attractive gap need not
remain useful throughout monitoring. Current depth, spreads, ages and their
changes can therefore improve ranking without estimating the final event
outcome. This is an interpretation of the comparison, not a causal attribution
to a specific feature.

The primary forest's known-selected precision is only 6.79%. A rare-event
screen can nevertheless have high recall when its purpose is allocating
monitoring, rather than issuing a trade on every selected hour. About 16.16%
of its selected event-hours have unknown targets. Treating these as negatives
would produce a different, unjustified evaluation.

## 7.3 Metadata, tuning, ensembles and budget

{{TABLE:metadata}}

Adding metadata to fixed logistic regression increases value capture by
0.39 percentage points. The fixed forest decreases by 0.12 points, while fixed
XGBoost improves by 15.76 points. This mixed outcome does not support requiring
metadata for every family. The large XGBoost difference is especially useful
as a reminder that feature effects depend on the learner, rather than being a
universal ranking of feature usefulness.

Tuned models do not consistently improve upon fixed controls. TabM captures
99.04% of quoted value and 88.02% of positive hours at N=3, but does not lead.
The classifier ensemble reaches 99.16% value capture; this is strong relative
to quoted gap but still below the fixed book forest. Extra architecture,
search and ensemble complexity therefore has limited incremental value in
this cohort. A recent model is a meaningful comparator even when it loses.

At N=5, the forest captures 98.05% of positive hours and 99.99% of value.
Quoted gap reaches 93.59% and 98.45%, respectively. The larger monitoring
budget narrows the gap, as expected when more events are selected. This
supports reporting N=3 as the principal resource-constrained comparison and
N=5 as a secondary sensitivity, rather than presenting the easier budget alone.

## 7.4 Economic comparison at 100 ms

{{TABLE:economics}}

The opportunity-selected forest produces $5,873.32 known simulated P&L from
453 attempts, including 436 completed baskets and 17 partial baskets. Completed
margin contributes $6,290.08; partial results contribute −$416.76. Quoted gap
produces $4,370.05. Monitor-all produces $5,944.97, and the 20-seed random
mean is $1,123.15. None of the primary forest's attempted execution outcomes
is unknown in this replay; this does not eliminate the separate unknown
opportunity windows in the candidate population.

The forest's maximum known-P&L drawdown is $206.15. Its largest order is
73,067 shares. Its no-settlement funding scenario requires $1,426,124.21,
illustrating why a few thousand dollars of margin must not be interpreted as
a high return on a small account. Uncapped available-depth sizing permits
large positions even when per-share margins are small.

The highest model P&L belongs to the forest two-stage policy at $5,921.18,
not the opportunity-selected forest. Most learned policies are economically
close, and freshness and depth also obtain returns relatively near the learned
group. The finding is therefore stronger against quoted gap than it is for
one complex model over every plausible simple screen. Economic differences
did not retroactively choose the first-layer objective.

{{FIGURE:43}}

## 7.5 Latency and incomplete execution

{{TABLE:latency}}

The selected forest's P&L declines from $6,471.72 at zero delay to $6,345.97
at 50 ms, $5,873.32 at 100 ms, $5,132.97 at 250 ms and $4,855.08 at 500 ms.
The monitoring decisions are unchanged. The difference arises from arrival-time
depth support and subsequent partial baskets, not from retraining a target on
the execution setting.

The zero-delay replay has no partial loss for this policy. At 500 ms, partial
results contribute −$1,190.38. This illustrates why assuming simultaneous
instantaneous fills would materially change the economic conclusion. It also
shows the value of keeping execution separate from learning: the same
opportunity predictor can be studied under several operational systems.

These are scenario comparisons, not an empirical estimate of what a particular
high-frequency trader can achieve. Source-time snapshot resolution, missing
updates, competing traders and real exchange queue behaviour remain outside
the supported fill model. Faster simulated execution should not be reported
as measured infrastructure performance.

## 7.6 Concentration and uncertainty

The December Fed event contributes 83.15% of the quoted-value denominator.
This concentration makes the impressive pooled value captures highly dependent
on one event. Excluding it before reranking changes the economic comparison:
the selected forest produces $730.52 versus quoted gap's $736.36. The study
therefore does not establish a consistent profit advantage across independent
events. The exclusion belongs in the main interpretation, not only in a
supplementary file.

{{TABLE:exclusion}}

The paired weekly resampling interval for the selected forest's economic
difference versus quoted gap is approximately $7.93 to $3,866.59 around the
observed $1,503.27 difference. With 13 dependent blocks, multiple policies,
missing evidence and substantial event concentration, this is conditional
uncertainty rather than proof of a universal economic advantage. Resampling
weeks cannot manufacture a larger set of independent event structures.

The final short weekly block has no known opportunity labels. Such periods
remain part of the monitoring and missingness description, but cannot supply
defined recall or value-capture metrics. Calendar coverage and useful labelled
data should therefore be reported separately.

# 8 Discussion and conclusion

## 8.1 Answers to the research questions

Within the selected Dome cohort, hourly event features support effective
budgeted monitoring. The fixed book-feature forest captures substantially more
known positive hours and quoted value than the quoted-gap rule at N=3. It
does so while monitoring only a subset of event-hours and without using
future labels to filter the ranking pool. The larger N=5 budget reduces the
incremental benefit of better ranking.

The comparison does not show that greater model complexity reliably improves
this task. Fixed classical models remain strong, bounded tuning offers mixed
gains, and TabM does not lead. Metadata has family-specific effects. These are
useful research findings because they constrain which additional engineering
is supported by the available evidence.

Economic replay confirms that opportunity capture and executed margin are
related but different. Slower arrivals can erase fills or leave losing partial
baskets. The opportunity winner is not the model with the highest primary
simulated P&L. Most importantly, the economic advantage over quoted gap is
concentrated in one event and disappears in the exclusion comparison.

## 8.2 Limitations

External validity is limited by frozen retrospective event selection, only
five ladder events and 12 positive forward events. Repeated hours and related
contracts are not independent samples. A larger row count does not by itself
establish broader event diversity, and pooled dollar weighting is dominated
by December Fed.

Coverage-aware labels avoid pretending missing data are negative, but they
cannot reveal the opportunities that occurred in unknown intervals. Known-only
metrics may differ from performance on a continuously observed exchange. The
offline recording extent and frozen metadata are also not a fully historical
live catalogue.

The execution model assumes source-time observability, independent per-leg
FOK support from recorded depth, zero cohort fees, zero additional costs and
unlimited funding. It does not model queue position, competing consumption,
network congestion, exchange acknowledgements or settlement delays as a
capital constraint. Partial unwinds use a fixed one-second delay. Their losses
are therefore execution-scenario outcomes, not immutable characteristics of
the event prediction problem.

The chronological fit and tuning safeguards prevent future target windows
from entering earlier models. They do not remove retrospective policy-selection
uncertainty or adjust every comparison for multiplicity. The final model is a
cohort-level research recommendation, not a demonstrated universally best
Polymarket strategy.

## 8.3 Future work

A stronger subsequent study would prospectively freeze a broader audited
universe, preserve point-in-time membership and collect continuous source and
receive timestamps. It should evaluate the selected policy on independent
future events and time periods, with particular attention to ladder coverage
and event-level concentration.

Operational validation should measure actual observation-to-order latency,
exchange responses and fill support. Fee-bearing contracts, settlement cash
release and bounded capital should be separate declared scenarios. New
sell-side or complement baskets require explicit payoff, inventory and order
support rather than a sign change to the existing buy logic.

The modular workflow permits other nonoverlapping screening windows and
monitoring budgets, but their labels and chronological comparisons must be
rebuilt. Additional tuning should be justified by independent predictive
evidence, rather than used as a substitute for a larger and better observed
cohort.

## 8.4 Conclusion

This dissertation implements a two-layer study of structural-arbitrage event
selection. It demonstrates strong opportunity capture under a three-event
monitoring budget within the observed Dome cohort, while showing that execution
latency, partial baskets, funding and event concentration materially qualify
the economic result. The practical contribution is a reproducible way to
measure those separate effects, with classical and recent tabular models
compared on the same chronological task.

# References

Bailey, D. H., Borwein, J. M., López de Prado, M. and Zhu, Q. J. (2017).
The probability of backtest overfitting. Journal of Computational Finance,
20(4), 39–69. https://doi.org/10.21314/JCF.2016.322

Breiman, L. (2001). Random forests. Machine Learning, 45, 5–32.
https://doi.org/10.1023/A:1010933404324

Chen, T. and Guestrin, C. (2016). XGBoost: A scalable tree boosting system.
Proceedings of KDD, 785–794. https://arxiv.org/abs/1603.02754

Gorishniy, Y., Kotelnikov, A. and Babenko, A. (2025). TabM: Advancing tabular
deep learning with parameter-efficient ensembling. ICLR 2025.
https://arxiv.org/abs/2410.24210

Grinsztajn, L., Oyallon, E. and Varoquaux, G. (2022). Why do tree-based models
still outperform deep learning on tabular data? NeurIPS 2022.
https://arxiv.org/abs/2207.08815

Saguillo, O., Ghafouri, V., Kiffer, L. and Suarez-Tangil, G. (2025).
Unravelling the probabilistic forest: Arbitrage in prediction markets.
arXiv:2508.03474. https://arxiv.org/abs/2508.03474

Wolfers, J. and Zitzewitz, E. (2004). Prediction markets. Journal of Economic
Perspectives, 18(2), 107–126. https://doi.org/10.1257/0895330041371321

# Appendix A Reproduction and artifact guide

The standalone FYP package separates data preparation, structural rules,
models, experiments, economic replay and reporting. The notebook research.ipynb
orchestrates these stages. Its cached mode verifies saved data and fitted
scores and regenerates analyses; its full mode reconstructs examples from raw
books, retunes and refits models, and reruns persistent execution into a new
run directory. Neither mode requires a parent repository or provider token.

The data and simulator guides document schemas and assumptions. The artifact
table and figure indexes map every catalogued output to a CSV source or
supporting dataset. The main report presents selected comparisons; the full
50-figure catalog supports additional inspection without filling the volume
with redundant plots.

The 28 tests exercise payoff constraints, backward as-of reads, duplicate
timestamps, recording endpoints, empty versus unknown books, integer depth
sizing, uncapped quantities, tick/minimum constraints, short opportunities,
incomplete labels, half-open windows, peak-value aggregation, positive-only
amount fitting, unknown slot consumption, chronological purging, selection
invariance, rank ensembles, primary execution settings, persistent reservations
and partial/unknown execution outcomes. They test implemented safeguards,
not the completeness of the original collector or the truth of real fills.

# Appendix B Configuration and provenance

inputs/cohort.json records complete basket membership and validity intervals.
inputs/raw_inventory.csv records every required raw history and its digest.
inputs/study.json records the feature views and bounded configuration grids.
Weekly bundle files record selected parameters and feature order; fit audits
record the actual training populations and cutoffs. Tuning search and selected
hyperparameter CSVs provide the detailed configurations supporting Chapter 5.

Source, input and output digests detect changes during reproduction. Verification
recomputes weekly scores from fitted artifacts, checks temporal separation,
reconstructs capture metrics and reconciles cumulative known P&L with economic
summaries. These controls establish reproducibility of the supplied study,
not point-in-time availability of frozen metadata or completeness of the feed.
