# 1 Introduction

## 1.1 Motivation

Prediction markets turn uncertain outcomes into tradable contracts. A YES share
pays $1 if its condition is met; a NO share pays when it is not. Related
contracts can also impose constraints
on their combined payoff. If exactly one of several outcomes must occur, buying
one YES share for every outcome guarantees a $1 payoff. A combined purchase cost
below $1 creates a potential margin before execution costs. Such opportunities
follow from the relationship between contracts, without forecasting the winner.

Finding a displayed gap is only part of the problem. A trading system must
repeatedly inspect order books, which record prices and quantities available to
buy or sell, check the required combinations and maintain order state. This
dissertation studies where that monitoring effort should go. Once an hour, a
screen examines every eligible event and chooses a small number to monitor
during the following hour.

The primary experiment allows three event slots. This budget is an abstraction:
the cohort has at most 19 simultaneous candidates and cannot establish a
platform-wide computing bottleneck. The question is how much recorded
opportunity a restricted allocation preserves. Screening could be useful when
relationship checks, execution infrastructure or risk supervision are costly.

The hourly scan still requires current books for all candidates. Its cost and
the cost of ranking must therefore be offset by savings during subsequent
monitoring. The experiments measure capture, token-book counts and scoring time
from stored features. Feed subscriptions, feature construction and total live
system cost remain unmeasured.

## 1.2 Research questions

The central question is whether current order-book features can identify events
that will offer structural arbitrage during the next hour. Three monitoring
slots form the primary comparison; five slots provide a secondary comparison.
The investigation has three parts.

RQ1 asks what machine learning adds beyond simple priorities: current quoted
margins, recent books and historical opportunity averages. The comparisons
measure both how many positive event-hours are retained and how much of their
quoted value is retained. They also examine which features and model choices
support the result.

RQ2 asks when that benefit changes. The analyses vary event weighting, training
history and monitoring resources, and test transfer between subject categories.
A separate procedure chooses a policy using only results from earlier weeks.

RQ3 asks what happens when the selected events are traded in simulation.
Execution replay tests delays, incomplete baskets and funding requirements
while keeping the monitoring decisions fixed.

Opportunity and execution are evaluated separately throughout. The first asks
where a qualifying basket appears in the recorded books. The second asks what
the simulator obtains after orders arrive. A high capture rate alone does not
establish profitability.

{{FIGURE:1}}

## 1.3 Contributions and scope

The study contributes an event-selection formulation, an implementation and an
empirical assessment. Its main safeguards preserve unknown outcomes, let those
events compete for monitoring slots, and separate prediction targets from fill
assumptions. Its comparisons examine how the apparent benefit changes when
large-dollar events, simple historical priorities or available resources receive
different emphasis.

The learners are established methods. The contribution lies in adapting and
evaluating them for this task. The frozen cohort contains 46 audited
complete-outcome events, called negRisk events in the retained metadata, and
five ladders of nested conditions. Chapter 3 gives the payoff rules. Source,
processed examples and fitted models support reproduction; independently
rebuilding raw coverage requires the separately retained token-book archive.
Policy choice is retrospective, with no untouched final test.

# 2 Background and related work

## 2.1 Forecasting and structural consistency

Wolfers and Zitzewitz (2004) discuss prediction markets as mechanisms that
aggregate dispersed information about uncertain outcomes. Their emphasis is the
quality and interpretation of market-generated forecasts. This motivates
studying these markets but does not establish that every relationship between
contracts is continuously arbitrage-free. Forecast accuracy and structural
consistency are distinct: an event can be difficult to predict while its
contracts still admit a guaranteed-payoff basket.

An event in this study is one ranked object, such as a particular Fed decision.
It can contain many binary markets. A token is one YES or NO outcome share and
has its own order book. A basket is an audited set of tokens with a known payoff
lower bound. A candidate is an event eligible for a scan; the term does not mean
an opportunity already exists. A signal is an observed interval in which a
basket passes the opportunity test. An attempted order, a completed basket and a
profitable executed trade are separate observations.

## 2.2 Empirical arbitrage research

Saguillo, Ghafouri, Kiffer and Suarez-Tangil (2025) investigate arbitrage on
Polymarket, distinguish within-market rebalancing from relationships across
markets, and address the cost of searching related conditions using heuristic
reduction. Their evidence motivates asking whether mispricing exists rather than
assuming a perfectly consistent platform. The task here differs: it predicts
which events deserve future monitoring under an explicit slot budget. The cited
market-wide profit estimate is neither a label nor a profitability benchmark for
this dataset. Historical depth coverage, basket definitions and execution
assumptions must be established independently for the present study.

This distinction also limits the role of semantic discovery. A market title
suggesting a related subject is insufficient to guarantee compatible settlement
rules. The implemented cohort therefore uses frozen audited relationships.
Membership is an upstream assumption, whereas the learner ranks events after
membership has been fixed. General automatic relationship discovery remains
outside the evaluated strategy.

## 2.3 Monitoring costs and learning from selected events

Trapeznikov and Saligrama (2013) study sequential classification in which a
system decides whether another, more costly measurement is worthwhile. Their
training data include all sensing modalities, while prediction-time decisions
trade acquisition cost against classification error. This supplies a useful
principle: the benefit of screening should be assessed against the information
cost it avoids. The present task allocates future event monitoring rather than
additional measurements for one classification. Event slots and token counts
make that allocation explicit, but do not inherit the paper's measured-cost
interpretation. Cached-feature timing can quantify only part of the overhead.

Chen, Wang and Yuan (2013) study combinatorial bandits, where a learner chooses
a subset and observes outcomes associated with played arms. Their formulation
connects resource allocation to the information obtained for future choices.
That feedback distinction matters here: historical-label baselines and the
earlier-only chooser use outcomes from unselected events as well. They are
full-feedback offline comparisons. Their results do not demonstrate a deployed
learner that acquires its training labels through three selected events, and
this dissertation makes no bandit-regret or exploration guarantee.

## 2.4 Tabular learning and incremental predictive information

Breiman (2001) introduces random forests as ensembles of randomized tree
predictors. For this application, their ability to learn nonlinear feature
relationships provides a comparison with a regularised linear score. The
original method does not resolve temporal leakage, class imbalance or
resource-constrained evaluation; those require application-specific choices.

Chen and Guestrin (2016) present XGBoost as a scalable tree-boosting system.
Here it tests whether sequential tree learning improves upon the forest's
averaging approach on the event-selection task.
Shared chronological populations make their rankings comparable; the bounded
searches examine a limited set of configurations, not each family's best
attainable performance.

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

These studies justify comparing different ways to learn from tabular features.
A separate question is what current books add beyond historical priorities. A model can retain
valuable hours by repeatedly prioritising the same historically productive
event. Historical frequency and value controls therefore test a different
explanation from comparisons between classifier architectures. Feature ablation
and family exclusion further ask whether current information and transferable
relationships contribute beyond that persistence.

## 2.5 Evaluation, selection bias and the research gap

Cawley and Talbot (2010) show that model-selection criteria can themselves be
overfit, with resulting selection bias in reported performance. This separates
two safeguards required here. Training and inner tuning must respect temporal
availability at every origin; choosing a policy after inspecting all forward
blocks still requires independent subsequent evaluation. Reusing those blocks
for additional sensitivity analysis improves diagnosis without creating an
untouched test.

Bailey et al. (2017) highlight the risk of selecting strategies because they
perform well in historical backtests. This concern applies here because several
screening policies are compared and the recommended policy is chosen from their
pooled chronological results. Earlier-only training and tuning prevent future
observations from entering each fit, but do not independently validate the
subsequent choice of the winning policy. This study does not estimate the
probability of backtest overfitting; its recommendation therefore requires
evaluation on independent future data.

Table {{TABLE_NUMBER:literature_synthesis}} summarises how these research areas
inform the design. The gap addressed is the empirical connection between
anticipating depth-supported opportunities, allocating limited monitoring and
evaluating the resulting execution scenario. Existing work supplies the
building blocks; this study tests their combination on a fixed audited cohort.
Its contribution is correspondingly bounded: evidence of cohort-specific
screening, with explicit limits on cost, feedback, coverage and generalisation.

{{TABLE:literature_synthesis}}

# 3 Problem formulation

This chapter follows one monitoring decision from selection to evaluation, then
defines the ranking rule, qualifying baskets and outcome labels precisely.

## 3.1 A worked hourly decision

Consider a fictional scan at 10:00 with five eligible events, A--E. Every event
has usable current books. A policy scores them using information available by
10:00 and ranks them in that order. With three slots, it monitors A, B and C
until 11:00. The following-hour outcomes are revealed only afterwards.

{{TABLE:reading_example}}

Suppose A later offers a qualifying basket with a peak margin of $10, while D
offers $2. B and E have complete records showing no opportunity. C's future
record is incomplete, so its label is unknown. The policy captures one of two
known positive hours: 50% recall. It captures $10 of the $12 known quoted value:
83.33% value capture. Its higher value percentage reflects retaining the larger
opportunity while missing the smaller one.

C still uses a monitoring slot. Removing it after learning that its outcome is
unknown would give the policy information unavailable at 10:00. Unknown outcomes
are excluded from evaluation denominators, not from the selection decision.
The $10 is an opportunity weight, not simulated trading profit; execution is
assessed separately in Chapter 6. All numbers in this example are illustrative.

## 3.2 Ranking events at each scan

An eligible event is a candidate for monitoring. Let \(\mathcal{C}(t)\) be the
candidate set at scan \(t\). For each event \(e\), features
\(\mathbf{x}(e,t)\) use current and earlier recorded states. A policy assigns
score \(s(e,t)\) and selects the highest-scoring \(\min(N,|\mathcal{C}(t)|)\)
events. A fixed event-identifier order resolves equal scores. Future outcomes,
including whether they will be known, do not affect eligibility or selection.

The selected set is \(S_N(t)\). It contains the available number of events up
to N and has the largest total score among sets of that size. Slots are filled
even when scores are small or negative: selection authorises monitoring, while
the separate opportunity and execution rules determine whether a trade is
attempted. Occurrence classifiers rank by an opportunity-probability score;
two-stage models rank by estimated hourly peak value.

The monitoring interval is \([t,t+H)\), with \(H\) equal to one hour. An
opportunity at the scan belongs to this window; one first appearing exactly at
\(t+H\) belongs to the next. The implementation requires the scan interval
and label horizon to match, so overlapping allocations cannot silently exceed
the budget.

When \(|\mathcal{C}(t)|\leq N\), all candidates are selected. These hours
cannot distinguish policies by what they exclude. Competitive-hour recall
therefore measures capture specifically when the pool is larger than the
budget. Equal event counts also need not imply equal token-book processing.

One hour and N=3 are evaluated design choices. N=3 was chosen after development
comparisons to create a selective budget, with N=5 secondary. Neither the
optimal rescan interval nor a prospectively fixed budget is established. Other
budget curves reuse the saved scores and are exploratory comparisons.

## 3.3 Baskets with a guaranteed payoff

A basket contains all shares needed for its payoff guarantee. For an audited
negRisk event, the outcomes are mutually exclusive and exhaustive: exactly one
occurs. Buying one YES share of each outcome pays $1 in total. Buying
\(q\) complete shares pays \(q\) dollars. All required members must be bought.

A ladder instead uses nested conditions. Let \(A\) be broader and \(B\)
narrower, with \(B\Rightarrow A\) under compatible settlement rules. Buying
\(\mathrm{YES}(A)\) and \(\mathrm{NO}(B)\) pays $1 if neither occurs, $2 if
A occurs without B, and $1 if both occur. B cannot occur without A. The basket
therefore has a $1 lower-bound payoff; the simulator uses that bound without
assuming the additional intermediate payment.

An order book must contain enough shares at the required prices. For a basket
with \(m\) members, \(C_j(q,t)\) is the total ask cost of obtaining
\(q\) shares of member \(j\), walking through the displayed price levels.
The quoted lower-bound margin is

\[ G(q,t)=q-\sum_{j=1}^{m}C_j(q,t). \]

Using only the cheapest ask could overstate this margin when little quantity
is available there. Each token provides a recorded minimum order size and price
tick. The implementation also uses whole-share quantities and a five-share
floor. The minimum basket size is the larger of five and the rounded-up largest
member minimum. Whole shares and the five-share floor are study choices;
the tick constrains price. There is no upper quantity or cash cap.

Separate orders for the legs require an additional check. Let
\(L_j(q,t)\) be the worst ask price needed to obtain the requested quantity
of member \(j\). A basket qualifies only if

\[ q-q\sum_{j=1}^{m}L_j(q,t)>0. \]

This requires a positive margin even if every leg fills at its submitted limit.
A positive margin calculated from the average displayed purchase cost alone
would not ensure that property. Any strictly positive margin passing the sizing
and limit checks qualifies; there is no additional dollar threshold.

## 3.4 Positive, negative and unknown outcomes

For each event-hour, \(y(e,t)\) is one if a basket qualifies during the next
hour and zero if none qualifies. Both labels require enough evidence to decide
the whole active window. If any required interval is unknown, the hour remains
unknown even if an opportunity was seen elsewhere in it. This gives positive
and negative hours the same observation requirement.

The value target \(v(e,t)\) is the largest qualifying one-basket margin in
that hour. It is zero for a known negative and missing for an unknown hour.
Repeated snapshots do not add value. Ladder-pair margins are not summed because
pairs can share depth. Summing hourly peaks measures retained opportunity, not
cumulative tradable profit.

Some opportunities already exist at the scan; others first appear later. A
separate diagnostic reports capture of later onsets. The main task includes
both, since monitoring must retain opportunities throughout the following hour.

## 3.5 Measuring capture

Positive-hour recall is the selected number of known positive event-hours
divided by all known positive event-hours. Quoted-value capture is selected
known \(v\) divided by all known \(v\). These are the 50% and 83.33% measures
in Section 3.1. Pooled value adds dollars across events before taking the ratio,
so a few large opportunities can dominate it.

Pooled value capture at N=3 is the primary comparison. Ties use captured positive
hours, then average precision, as specified for the relevant selection step.
Average precision summarises how well scores rank positive examples across
thresholds. Precision among selected known hours measures how often monitoring
finds an opportunity; unknown-slot share records allocations whose outcomes
cannot be evaluated. Token counts describe the associated book burden.

Random selection includes each candidate with probability \(\min(N,m)/m\)
when \(m\) candidates compete. Expected positive and value capture can thus
be calculated exactly. Economic replay instead uses 20 fixed-seed random
policies, since earlier trades change later liquidity use. Monitor-all is the
full-resource reference. Recall is undefined when there are no known positives;
value capture is undefined when their value is zero. Neither is reported as zero.

# 4 Data and example construction

The evaluation periods below refer to overlapping parts of one saved dataset.
They are separated here so the changing denominators in later results can be
read against a single reference.

## 4.1 Source, event cohort and evaluation periods

The source is saved Dome token-level L2 data: order books with prices and
quantities at multiple levels, accompanied by frozen Polymarket metadata. The
archive covers 994 token histories, approximately 1.01 GB, from 14 October 2025
to 3 February 2026 at 08:00 UTC. These 112 days and eight hours do not provide
continuous complete records for every event.

Dome's API retired during the research. Its documentation gives 28 April 2026
as the end-of-life date for all APIs (Dome, 2026). Through that source, the study
could no longer add event histories or fill missing periods. The event universe
therefore remained frozen, constraining its size, diversity and coverage.

The fixed list has 51 supported events: 46 negRisk and five ladders. The complete
prepared table contains 29,308 eligible event-hours, including training history,
23,452 known outcomes and 470 positives. During the main evaluation, 44 events
appear as candidates and 12 have known positive hours. An event-hour means one
eligible event at one hourly scan; repeated rows are not independent events.

{{TABLE:reading_populations}}

The main evaluation supplies the headline 359-positive comparison. The earlier
evaluation ends at 8 January because the retained feature table ends on
7 January; additional refits use its 343 positives. The 77-positive period
after warm-up evaluates policy choice using earlier weeks. These are overlapping
views, not three independent datasets. The earlier feature table contains 48
events including training history; some do not enter the main evaluation.

{{TABLE:population}}

Basket membership is fixed in a manifest of 348 definitions: 46 complete sets
and 302 ladder pairs. The 302 pairs belong to only five events and often share
tokens. Complete sets require audited exhaustive, mutually exclusive outcomes.
Ladders require compatible resolution sources, periods and threshold or deadline
rules. Freezing membership keeps relationship discovery separate from ranking;
it does not reconstruct the platform catalogue available at every past time.

{{TABLE:cohort_structure}}

The ladders concern November Bitcoin and Ethereum thresholds, Gensyn sale
commitments, Monad launch valuation and shutdown deadlines. Within one audited
monthly reach-price family, reaching a higher threshold implies reaching a
lower one. The basket buys lower-threshold YES and higher-threshold NO. Different
measurement periods or reach/dip rules cannot be combined just because titles
mention the same asset. The small ladder sample reflects available histories
and audited relationships, not the market-wide frequency of ladder arbitrage.

The inventory records required tokens, first/last timestamps, file sizes and
file fingerprints. Metadata identify member markets and settlement descriptions.
There is no complete log of rejected or unavailable platform events, so a
market-wide sampling fraction cannot be calculated.

{{FIGURE:5}}

## 4.2 Constructing a book state at the scan

Each raw row contains source and provider-index times, ask and bid levels,
minimum order size and tick size. Prices and sizes must be finite and valid.
An empty side is an observation of no displayed liquidity; a missing or
malformed side is unknown. A crossed book, whose best bid exceeds its ask,
does not provide trusted entry evidence.

For each token, the scan uses the latest source-time row at or before the scan.
Equal source timestamps retain the last provider-indexed version under stable
ordering. This backward lookup is called as-of alignment. Tokens may update at
different times, so book age measures how old each retained state is. A later
record never supplies an earlier state, and replay stops at each token's audited
recording endpoint.

Source time approximates observation through an idealised feed with the declared
execution delay. Provider receive-time delays are unavailable. Thus a 100 ms
replay does not demonstrate that the collector delivered updates within 100 ms.
Keeping an old state without a freshness cutoff also does not prove that its
liquidity remained available at the exchange.

## 4.3 Eligibility and outcome coverage

Eligibility asks whether an event can enter the current decision. It requires
valid active membership, previously seen required tokens and a scan within the
saved event observation extent. Outcome coverage asks whether the following
hour can be labelled. A usable current state can therefore enter ranking even
when its future outcome will be unknown. Only training excludes that target.

Coverage is assessed for each active basket interval. Normally every leg must
be observed. An observed blocker can sometimes decide the result despite another
missing leg: empty asks on one required leg already rule out buying the basket.
Otherwise missing member evidence leaves the interval unknown. Positive and
negative labels both require the entire active hour to be decidable.

For example, suppose all legs are observable from 10:00 to 10:40, but a required
leg becomes unknown until 11:00. The hour remains unknown even if a qualifying
basket appeared at 10:20. Coverage therefore concerns whether the basket can be
evaluated, not just the fraction of tokens with records.

{{FIGURE:7}}

Figure {{FIGURE_NUMBER:7}} counts eligible event-hours by UTC day. Blue includes
known positives and known negatives; orange counts unknown labels. Their sum is
the candidate population. The falling total reflects fewer eligible event-hours
in the fixed cohort. From 28 January, none has a known label although some
carried book states still support features. That is loss of evaluable evidence,
not evidence that opportunities stopped occurring.

## 4.4 Book features and metadata

Eleven summaries describe the event at the scan. They count active baskets and
tokens, describe valid books and spreads, total displayed ask depth, measure
maximum book age, and summarise the margins and size feasibility of its baskets.
Table {{TABLE_NUMBER:features}} defines each quantity and unit.

{{TABLE:features}}

Seen-member fraction is the fraction of active tokens observed previously.
Clean-member fraction additionally requires valid, two-sided, non-crossed books.
Size-feasible fraction counts active baskets for which a valid depth-based
margin can be computed; positive-margin fraction counts those with margin above
zero. Eligibility makes current seen-member fraction always one, so that feature
cannot distinguish eligible candidates.

Each summary also supplies its previous hourly value and change, giving 33 book
features. For current \(x_t\), the additional columns are \(x_{t-1}\) and
\(x_t-x_{t-1}\), with the previous scan exactly one hour earlier for that
event. These are absolute changes. Lags are constructed before eligibility
filtering; a missing scheduled scan gives a missing lag. Shares, dollars per
share, seconds and total basket dollars remain distinct units. Summed depth is
not normalised for event size or computing cost.

The quoted-gap feature is the largest displayed dollar margin across the
event's active baskets and supported integer quantities. It subtracts purchase
cost from the guaranteed payoff in Section 3.3. For example, 100 complete
shares costing $97 give a $3 margin. The feature can be negative when even the
best supported combination costs more than its payoff. It omits the additional
worst-limit-price check used to qualify an opportunity, so a positive quoted gap
alone does not authorise a trade. Missing quoted-gap values rank last in the rule.

Optional metadata add event type, broad subject and days since the earliest
frozen member was created. Creation age approximates event maturity, not a
verified historical listing date; missing or negative ages remain missing.
Subject categories use fixed title rules. Event IDs, winners, realised status,
future volume and execution returns are excluded. Frozen descriptions and
membership retain a retrospective selection limitation.

Metadata comparisons hold the model settings fixed and add these three inputs,
separating added information from tuning. Feature correlations describe
associations; the refitted ablations in Chapter 7 examine predictive contribution.

# 5 Prediction methodology

All policies face the same hourly allocation problem. The comparisons progress
from simple current-book rules to learned scores, historical priorities and
tests of which information transfers. Exact implementation settings are retained
in Appendices C and D.

## 5.1 Rules and learned policies

A policy is a scoring rule together with the event-selection procedure. Three
rules use one current-book summary each: quoted gap prefers a larger margin,
depth prefers more displayed ask shares, and freshness prefers a smaller maximum
book age. A negative gap can outrank a more negative one because the decision
allocates monitoring; a separate test authorises entry.

Logistic regression learns a regularised linear score. Random forest combines
trees that capture nonlinear relationships, while XGBoost builds trees
sequentially to improve earlier predictions. Each has three variants: fixed
book-only, fixed book plus metadata, and tuned book plus metadata. In the tables,
book means the 33 summaries and hourly history features; +metadata adds the
three event descriptors. Fixed means the settings remain fixed, not that the
model is fitted only once. Both fixed and tuned models refit weekly.

{{TABLE:fixed_settings}}

Classifiers learn their standard classification losses rather than directly
optimising the top-three allocation. Class balancing gives rare positives more
weight. Two-stage models instead combine an unweighted occurrence probability
with the expected margin conditional on an opportunity:

\[ \widehat{v}(\mathbf{x})=\widehat{\mathbb{P}}(y=1\mid\mathbf{x})\,\widehat{\mathbb{E}}[v\mid y=1,\mathbf{x}]. \]

The second stage uses positive-value training rows and clips predicted amounts
below zero. An event can thus rank highly because opportunities are likely,
valuable when present, or both. The compared two-stage models are a fixed forest
and tuned TabM, a neural tabular model. Their scores describe opportunity value,
not expected execution profit, and no probability cutoff precedes ranking.

Three equal-weight ensembles combine: the three tuned classifiers; tuned
logistic, tuned forest and two-stage TabM; or the two-stage forest and TabM.
Each component is converted to percentile ranks within the current candidate
pool before averaging. This prevents one model's numerical scale from
dominating the combination.

## 5.2 Preprocessing and model settings

Every learned policy fits preprocessing on its training rows. Classical models
replace missing numeric values with training medians, record missingness where
applicable, compress magnitudes with a signed logarithm and standardise. Metadata
categories become binary indicators. Future examples determine none of these
transformations; unseen categories create no new indicators at prediction time.

Each tuned classical model compares eight declared settings. TabM compares four
architectures, using training-fitted quantile preprocessing and 32 ensemble
members within each stage. Training evaluates each member's loss separately;
prediction averages member outputs. Batch size is 1,024, the limit is 60 epochs,
and early stopping allows ten epochs without validation improvement. Appendix D
retains the transforms, member losses, architecture sizes and scaling rules.

The TabM weekly refit averages three seed-specific products of probability and
amount predictions. Its refit duration is the rounded-up median best epoch
count from earlier validation fits, retained until the next tuning date. Weekly
evaluation outcomes cannot determine when that refit stops. These bounded
searches compare specified alternatives, not every model family's attainable
best performance.

## 5.3 Learning from earlier weeks

The first 28 calendar days supply initial training history. Main evaluation
runs from 11 November 2025 to 3 February 2026 at 08:00 UTC: 2,024 scans across
13 weekly blocks, the last only eight hours. At each block's start, called its
fitting origin in the saved records, models refit on all available earlier
training rows and score the next block.

Availability is stricter than having an earlier scan timestamp: a known label's
entire one-hour window must end strictly before the fitting date. A window
ending exactly at that date is excluded. Earlier evaluation rows can enter
later training once this rule is satisfied. All policies still rank unknown
candidates; only known rows supply training targets and capture denominators.

Tuning occurs on 11 November, 9 December and 6 January, with at least seven
evaluation days remaining. Two validation blocks lie wholly within earlier
history. Their models use the same strict label-end cutoff. Settings maximise
pooled quoted-value capture at N=3; ties use captured positives, mean validation
average precision and finally declared configuration order. Validation also
sets TabM stopping durations. The next evaluation block influences none of
these choices.

{{TABLE:validation_windows}}

Every setting receives the same validation candidates, including unknowns that
consume slots. Table {{TABLE_NUMBER:validation_windows}} gives the actual
timestamps; Appendix D explains their indexed-scan construction. Known training
rows grow from 7,652 to 23,452, but the last known training scan is on 27 January
because later labels are unavailable.

{{TABLE:workflow}}

{{FIGURE:2}}

## 5.4 Policy recommendation and uncertainty

The primary recommendation uses the highest pooled value capture at N=3 across
the main evaluation, followed by the tie criteria in Section 3.5. Economic
profit and loss (P&L) is assessed afterwards and does not change that criterion.
Each prediction uses earlier training, but choosing a winner after comparing
all blocks remains retrospective. The later analyses diagnose that same
inspected dataset and do not create an untouched final test.

Uncertainty intervals use paired resampling: both policies are compared on the
same resampled observations. The main procedure draws the 13 weekly blocks
with replacement 10,000 times and reports a 95% interval for the difference.
Whole blocks preserve dependence within a week, including the eight-hour final
block's actual size. Dependence across weeks can remain.

These intervals hold the fitted models and cohort fixed. They exclude
uncertainty from refitting, universe selection and missing outcomes, and do not
adjust for all policy comparisons. Event- and family-based resampling below
examine different dependence units. Section 8.2 brings these shared limits
together; each result also identifies its particular restriction.

## 5.5 Which features contribute?

Feature ablation means refitting after retaining or removing a specified input
group. It tests whether that information improves the recorded ranking while
holding other experimental choices fixed. Six views use all 33 book features;
the 11 current summaries; current margin alone; all except the quoted-margin
group; all except book age; or current margin and age together.

The quoted-margin group contains six current, lagged and changed margin and
positive-margin-fraction columns, leaving 27 when removed. Other quote-related
information, including size feasibility, remains. Removing age leaves 30. These
are targeted comparisons, not removal of every arbitrage-related input.

New fitting uses the earlier evaluation in Table
{{TABLE_NUMBER:reading_populations}}. Its retained feature table contains 26,835
rows including training history; 17,293 evaluation rows exactly match saved
keys, labels, values and known flags. The full later feature table is unavailable
for these additional fits. Full-period analyses can still reuse the original
336,022 saved rows: 17 policies scored on the same 19,766 candidates.

Every view uses fixed forest settings and three seeds, with matched training
rows, test candidates and strict cutoffs. Nine weekly origins, the last truncated
to two days, give 162 fits. Preprocessing is refitted within each view. Reported
seed minima and maxima describe variation across those fits, not confidence
intervals for future events. Recomputed N=3 and N=5 metrics must agree with the
original results before further analysis.

## 5.6 Historical priorities, subject transfer and equal weighting

Historical controls ask whether productive events can simply be prioritised
again. Four scores use past event opportunity frequency, past event mean quoted
value, last-known event value, or past subject-family mean value. A subject
family groups related topics, such as central-bank decisions or elections.
It is distinct from a model family such as forests or neural networks.

Only known label windows ending strictly before the current scan enter these
scores. Known zeros contribute; unknown windows do not. Event means are smoothed
towards family means, and family means towards the global mean, using a fixed
24-observation prior at each level. This reduces reliance on a few observations.
An event without history falls back to its family estimate; empty global history
gives zero. Last-known value includes observed zeros and uses the family mean
only before the first known event outcome. Appendix D gives the exact calculation.

The history timeline has 29,308 deduplicated rows, combining retained earlier
examples with later saved outcomes. All controls score the main evaluation.
They use event identity explicitly and update hourly; book-feature models omit
event identity and refit weekly. Full-feedback means that outcomes from
unselected events are also available. The offline records provide that feedback;
its acquisition by a deployed three-slot policy is untested and uncosted.

The transfer test removes every training row from one subject family, then
fits the fixed forest and logistic model at each earlier weekly origin. Forest
uses three seeds and logistic one. Every model ranks the whole ordinary pool
before capture is measured within the withheld family; no slots are reserved
for it. Matched unrestricted forests come from the ablation fits. Of 81
family-week combinations, 63 contain target-family candidates, giving 189 forest
and 63 logistic fits. Missing target-family blocks need no fit.

Each withheld family is a separate scenario. Combining their selections would
not produce one three-slot policy. The test examines transfer between categories
in this cohort, not performance on independently collected future markets.

Equal-event evaluation asks how well an average opportunity-bearing event is
served. First calculate each positive event's capture fraction, then average
the 12 fractions equally. Equal-family evaluation first combines outcomes
within each of eight positive families, then averages those fractions equally.
Selections remain global N=3. Events with no known positives still compete for
slots but have no defined capture fraction to average.

Paired event and family intervals use 10,000 bootstrap draws with seed 20261102.
Event resampling draws the 12 positive events. Family resampling draws eight
families, retains their positive events and recalculates the mean-event
difference. Both interval types thus concern the same event-balanced comparison
under different dependence assumptions. They neither refit nor adjust for
multiple comparisons, and eight families give limited independent evidence.

## 5.7 Resource budgets and choosing a policy in advance

Five policies enter these comparisons: book-feature forest, book-feature
logistic, quoted gap, freshness and historical event mean value. Their existing
scores are reused without new labels or execution replay.

Token-observation caps account for event size. An event costs its recorded
active-token count; a token shared by two events counts in each. At each scan,
events are considered in score order. A whole event is selected if it fits the
remaining cap; an expensive event is skipped and consideration continues.
Unknown outcomes use the same resources. The caps are 10, 25, 50 and 100, with
50 the primary comparison and no simultaneous three-event constraint.

This holds the maximum budget fixed, not actual spending. Indivisible events
leave different amounts unused, greedy packing is not optimal allocation, and
selected sets need not be nested as the cap grows. Tokens approximate monitoring
work; subscriptions, scan construction and update rates are not measured.

The earlier-only chooser retains N=3 but chooses which policy to use next week.
The first four scored weeks, 11 November to 9 December, supply warm-up because
earlier out-of-sample scores are unavailable. At each of the nine later fitting
dates, the chooser compares candidates' cumulative captured value on earlier
known windows ending strictly before that date. It then uses the best policy
for the next block. Ties within the declared tolerance follow historical mean,
forest, logistic, quoted gap and freshness. Candidate selections are formed
before outcomes are consulted, and their original weekly fitting is unchanged.

Static policies are compared on the same after-warm-up period in Table
{{TABLE_NUMBER:reading_populations}}. Evaluating alternative past selections
requires full feedback, including unselected events. Paired weekly intervals
hold the observed chooser path fixed and do not adjust for its retrospective
design.

Recorded decision cases use the earlier evaluation. Known positive hours are
grouped by whether forest and historical mean select both, only one or neither.
The largest-value hour from each disagreement or joint-miss group is shown;
ties use time and event identifier. Unknowns remain in the candidate pool.
These deliberately selected examples illustrate decisions without representing
typical hours or identifying causes.

{{TABLE:assumptions_checks}}

## 5.8 Design sensitivity and scoring overhead

Historical mean value is recomputed with prior strengths of 6, 24 and 72
observations at both smoothing levels. The original 24-observation scores must
reproduce exactly. The forest is also fitted with 14-day and 28-day histories,
instead of all earlier data. Training scans start at the fitting date minus
the declared days and keep the strict label-end cutoff. Nine dates and three
seeds give 54 new fits, with existing expanding-history fits as controls.
Candidates, preprocessing and model settings are shared.

The sensitivity grid was fixed before execution after earlier results had been
inspected. All settings are reported without selecting a replacement primary
policy. The same exploratory status applies to the feature, historical, transfer
and resource analyses above. Changed coverage rules, basket sizes, horizons or
execution costs would require raw reconstruction and are not tested here.

Timing uses 12 equally spaced scans from 6--8 January and that block's saved
models. Each policy receives five warm-ups and 30 measured calls per scan.
The forest is measured with one and four jobs, preserving its saved scores;
logistic and quoted gap are references. Costs include cached-column selection,
fitted preprocessing, scoring and N=3 allocation. Loading, fitting, raw reads,
feature construction, feeds and historical feedback are excluded.

# 6 Economic replay

Capture measures whether monitoring retains an opportunity. Replay asks what a
simulated order obtains after submission. Keeping these stages separate allows
the same selected events to be tested under several execution delays.

## 6.1 Execution assumptions

Each screening policy's hourly selections stay fixed across scenarios. The
primary delay is 100 ms from observing a signal to order arrival, with immediate
submission. Alternatives are 0, 50, 250 and 500 ms. These are assumed delays,
not measurements of the historical collector or a live trading system.

At a signal, the simulator chooses the supported basket quantity with the
largest integrated dollar margin, subject to Section 3.3's depth, size and limit
checks. Quantity and capital are uncapped. No minimum signal duration, maximum
quote age or random failure probability is added. Every policy and delay has
its own liquidity-reservation record, maintained throughout replay.

The frozen cohort is evaluated with zero exchange fees and no per-attempt
charge. Saved market flags support that scenario, but are not a historical
fee-change ledger. Fee-bearing contracts would require recalculated qualification,
sizing and execution costs. Zero delay is an optimistic recorded-state comparison,
not an estimate of the exchange's maximum obtainable profit.

## 6.2 From submission to complete or incomplete baskets

When a selected event produces a new signal, the scheduler fixes the basket,
quantity and each leg's price limit using the books then available. Simultaneous
baskets are prioritised by quoted margin, then basket identifier. Arrival-time
books affect fills only after the submission decision is fixed.

Each token order is simulated as fill-or-kill (FOK): it fills completely if
recorded depth supports the full quantity within its limit and satisfies the
minimum-size and tick rules; otherwise it is cancelled. Separate legs can have
different outcomes. A basket is complete when all fill, partial when some fill,
and unfilled when none fill. Missing arrival evidence leaves execution unknown.

A complete basket records the conservative payoff minus its integrated purchase
cost. An unfilled basket has zero P&L under the no-charge assumption. For a
partial basket, the simulator tries to sell the acquired shares at bid depth
one second after arrival. Proceeds minus purchase cost give partial-basket P&L;
missing or insufficient exit evidence leaves it unknown. Buying ten shares at
$0.40 and selling at $0.30, for example, loses $1.

Entry uses the signal as observed, never its eventual duration. An opportunity
that vanishes quickly can therefore be a positive prediction target yet yield
no fill or a partial loss. This is the execution risk the separate layer tests.

## 6.3 Liquidity reuse, profit and funding

Each event waits until the previous attempt's scheduled unwind time before
allowing another. Reservations prevent later scans or overlapping ladder pairs
from repeatedly treating the same unchanged ask book as fresh liquidity. They
survive model refits and weekly boundaries. The supplied events do not share
tokens across schedulers; a cohort with that overlap would need shared
reservations across events too.

Complete margins enter P&L at completion and partial results at unwind. Known
P&L sums only decidable outcomes; unknown outcomes are counted separately.
Drawdown is the fall from the previous cumulative P&L peak, starting at zero.
Recognising a guaranteed payoff does not mean settlement cash is available.

The main comparison assumes unlimited funding. A separate no-settlement
scenario measures peak cash required if complete baskets return no settlement
proceeds during the study. Purchases consume cash, known partial sales return it,
and unknown orders keep conservative reservations. This scenario does not
measure actual capital or return on investment. Monitor-all observes every
eligible event, but its earlier trades can reserve depth needed later; it is
therefore not guaranteed to earn the highest P&L.

# 7 Results

Three findings guide the detailed comparisons. For RQ1, the forest captures
more opportunities than current quoted gap, but historical averages already
retain most pooled dollars. For RQ2, the benefit changes with event weighting,
training history and the resource budget. For RQ3, execution delay and one
dominant event materially change the economic interpretation.

Results use the main evaluation unless labelled earlier evaluation or after
warm-up. Table {{TABLE_NUMBER:reading_populations}} maps these periods. Recall
counts retained positive hours; value capture weights them by their peak margins.

## 7.1 How much selection is required?

Most scans require choosing among competing events. The main evaluation has
19,766 event-hours from 44 events: 15,787 known labels, including 359 positives
from 12 events, and 3,979 unknowns. The hourly pool has a median of nine events
and a maximum of 19. It exceeds three at 1,887 of 2,024 scans.

Every deterministic N=3 policy selects 5,973 event-hours, filling fewer slots
when fewer candidates exist. Random selection would capture an expected 22.23%
of positive hours and 18.93% of value under these changing pool sizes. Those
figures provide a reference for allocation, not a market-wide opportunity rate.

{{TABLE:monitoring_burden}}

Table {{TABLE_NUMBER:monitoring_burden}} shows that the same number of event
slots can require different numbers of token books. Unknown windows still use
resources. Section 7.11 replaces the event-slot budget with a token cap.

## 7.2 Learned ranking versus current quoted gap

The forest retains substantially more known future opportunity than ranking
events by their current quoted margins. At N=3 it captures 340 of 359 positive
hours (94.71%) and 99.53% of quoted value. Quoted gap captures 264 hours
(73.54%) and 60.76% of value. Table {{TABLE_NUMBER:primary}} compares all
original policies using the same $8,838.31 value denominator.

{{TABLE:primary}}

The result includes anticipating new opportunities: forest captures 205 of 223
positive hours with no opportunity at the scan. A current margin describes the
present, while the target covers the following hour. Other book summaries can
help distinguish events whose conditions will change; Section 7.6 tests which
inputs support that interpretation.

High recall does not make every selected hour productive. Only 6.79% of selected
known hours contain an opportunity. Another 16.16% of selected hours have
unknown outcomes. Monitoring is therefore selective attention to rare events,
with entry still governed by the separate qualification test.

## 7.3 What does learning add beyond historical averages?

Historical averages explain much of the pooled dollar result. Ranking by past
event mean value captures 98.41%, only 1.12 percentage points below forest's
99.53%. Its positive-hour recall is lower: 79.67% versus 94.71%. Historical
frequency captures 81.62% of positives and 92.19% of value; last-known value is
weaker on both.

{{TABLE:history_controls}}

The historical-mean control repeatedly favours events that previously offered
valuable opportunities. Forest adds 54 captured positive hours, and its
advantage is larger when events receive equal weight (Section 7.7). Thus the
benefit of learning is clearer for broader opportunity coverage than for the
pooled value total alone.

These controls assume full feedback, including outcomes from unselected events.
The comparison does not establish the cost of collecting those labels in use,
and there is no economic replay for the historical policies.

## 7.4 Can earlier results choose the next week's policy?

The chooser selects forest at all nine fitting dates after warm-up. On the
common later period, it captures 75 of 77 positives (97.40%) and 99.63% of value,
exactly matching the static forest. Logistic captures fewer positives but
slightly more value; historical mean captures 46 positives and 91.59% of value.

{{TABLE:policy_choice}}

The procedure can therefore recover forest using earlier forward outcomes.
However, warm-up already contains 282 of the full 359 positives and $8,251.84
of the $8,838.31 value. Evaluation has only $586.46 left. It cannot independently
confirm the large dollar result seen during warm-up. The cumulative objective
also preserves an early lead: there is no policy switch demonstrating adaptation.
Full feedback is assumed, and separate candidate execution paths cannot be
stitched together to infer chooser P&L.

## 7.5 Do metadata and more complex models improve capture?

Additional modelling does not consistently improve the strongest fixed control.
Metadata changes value capture by +0.39 percentage points for logistic,
-0.12 for forest and +15.76 for XGBoost. Table {{TABLE_NUMBER:metadata}} holds
model settings fixed, so these differences isolate the added descriptors.

{{TABLE:metadata}}

Tuning is similarly mixed. TabM captures 99.04% of value and 88.02% of positives;
the classifier ensemble captures 99.16% of value. Neither leads the fixed
book-feature forest on the primary criterion. These are results for the
declared searches and cohort, not a general ranking of neural and tree methods.

## 7.6 Which book information supports the result?

The full model's capture is not reproduced by learning from current margin
alone. All feature comparisons below use the same earlier evaluation, with
343 positives. Each forest view is refitted with three seeds and fixed settings;
rules are unchanged.

{{TABLE:ablations}}

{{FIGURE:104}}

The full forest averages 94.46% recall and 99.53% value capture. Current summaries
alone reach 93.39% and 99.10%, so the previous-hour values and changes add modest
gains of 1.07 and 0.42 percentage points. A margin-only forest reaches 74.93%
and 61.40%, close to the quoted-gap rule's 72.30% and 60.38% on this period.
Adding age to margin gives 75.90% and 72.70%. Richer book information matters
more than simply applying a nonlinear learner to the gap.

Removing the six margin-group columns lowers recall to 87.07%, while value
capture remains 99.30%. Removing the three age columns gives 95.24% recall and
99.48% value. Age therefore does not explain the full advantage in this test.
Remaining correlated inputs can substitute for a removed group, so these are
tests of predictive dependence rather than causal effects. High value capture
can also hide missed low-margin hours.

The original-seed full refit reproduces saved scores to within one trillionth
on all 17,293 rows. This checks reproduction, not the original collector's
accuracy. Three-seed ranges quantify variability on the same dataset; no
reduced view becomes a new primary policy.

## 7.7 Does the benefit extend across events?

Pooled dollars mainly describe negRisk events. Their 18,095 rows contain 15,177
known labels, 345 positives and $8,835.79 of value. The ladders contain 1,671
rows, 610 known labels, 14 positives and $2.51, about 0.028% of the denominator.
Unknown shares are 16.13% for negRisk and 63.49% for ladders.

{{TABLE:structure_results}}

Forest captures 331 negRisk positives and nine ladder positives; quoted gap
captures 252 and 12. The ladder result reverses the pooled ordering, although
the small incomplete sample cannot reliably rank learners for ladders generally.

{{FIGURE:103}}

Individual events also differ. Both forest and freshness capture all 96 December
Fed positives, while quoted gap captures 59. In Honduras, the counts are 31 of
38 for forest, 21 for quoted gap and 23 for freshness. For the Monad ladder,
forest captures one of six, quoted gap four and freshness three. Monad's total
value is only $0.166, so its misses barely change pooled dollars.

{{TABLE:balanced_results}}

{{FIGURE:202}}

Giving each positive event equal weight makes this distinction visible. Forest
averages 83.34% recall and 87.06% value capture across 12 events, versus quoted
gap's 77.26% and 81.01%. Its value advantage shrinks from 38.77 pooled points to
6.05 event-balanced points. Historical mean falls from 98.41% pooled value to
57.14% event-balanced value, showing its greater reliance on repeatedly valuable
events. Forest has the highest event- and family-balanced point estimates among
the 21 original and historical policies.

{{TABLE:balanced_intervals}}

The uncertainty is wider than the pooled ordering suggests. Forest's mean-event
value difference from quoted gap is 6.05 points, with a family-cluster interval
of -5.75 to 16.03. It spans zero. Intervals also span zero against freshness
(-3.96 to 33.38) and TabM (-4.10 to 8.68), but stay positive against historical
mean (7.05 to 57.16) and logistic (3.07 to 35.36). These intervals use eight
positive families without adjustment for multiple comparisons.

## 7.8 Dependence on December Fed and evaluation week

Removing December Fed tests how much the result relies on one unusually valuable
event. It supplies 83.15% of quoted value. After removing it from each candidate
pool and reranking, forest captures 251 of the remaining 263 positives (95.44%),
against quoted gap's 214 (81.37%). Value capture is 97.50% versus 88.36%.

{{TABLE:exclusion_capture}}

Capture therefore remains stronger after exclusion. Section 7.14 shows that the
economic advantage does not. This procedure frees the excluded event's possible
slot and reranks without retraining; it is not a model trained without that
event. Saved outputs repeat the diagnostic for all 12 positive events.

{{TABLE:strong_controls}}

Against freshness, forest's pooled value gain is 4.32 percentage points, with
a paired weekly interval of 1.51 to 21.53. Its smaller differences from logistic
and TabM span zero. These data do not clearly separate the strongest learned
policies under the resampling assumptions in Section 5.4.

{{FIGURE:102}}

Weekly percentages change with candidate competition, positive counts and value
concentration. The pooled total weights weeks by their opportunity denominators,
not equally. The last eight-hour block has no known labels. It still uses
monitoring slots, but its capture is undefined and plotted as a gap. The weekly
CSV retains these denominators for inspection.

## 7.9 Learning without examples from the same subject family

Transfer is uneven. Removing a subject family's training rows tests whether the
model can rank that category using relationships learned elsewhere. It still
competes against all other candidates. Each family in Table
{{TABLE_NUMBER:family_transfer}} is a separate scenario; RF means random forest,
LR logistic regression, and n/a an undefined capture fraction.

{{TABLE:family_transfer}}

{{FIGURE:201}}

Without central-bank training rows, forest mean recall for that family falls
from 99.57% to 95.96%, and value capture from approximately 100% to 98.41%.
The three-seed ranges are 94.81--96.54% and 97.08--99.65%. The dominant family's
strong capture therefore does not require its own training history here,
although it declines from the unrestricted control.

Elections transfer less well. Forest recall drops from 78.86% to 67.48%, and
value capture from 91.67% to 75.56%. Withheld logistic reaches 80.49% and 79.01%,
exceeding forest on both. Removing crypto, culture, sports or technology/business
rows instead improves mean forest recall, while shutdown is unchanged. Such
changes can reflect both altered fits and competition for slots.

Small denominators limit the interpretation: shutdown has one positive hour,
sports nine and crypto 13. Geopolitics and tweet counts have none in the earlier
evaluation, so remain visible with undefined capture. The test concerns these
recorded subject groups, not independent future markets.

## 7.10 Sensitivity to historical smoothing and training length

The historical-baseline result is stable across the declared smoothing choices.
Using priors of 6, 24 and 72 observations gives full-period value capture of
98.34--98.56% and recall of 79.39--80.50%. Event-balanced value is 54.44--57.14%,
still below forest's 87.06%. Neither the strong pooled baseline nor its weaker
equal-event coverage depends on the single original prior.

{{TABLE:design_sensitivity}}

A month's training history performs similarly to retaining all earlier data.
On the earlier evaluation, 28-day forest mean recall is 94.36% and value capture
99.62%, versus expanding history's 94.46% and 99.53%. Two weeks is weaker:
90.38% recall and 93.71% mean value capture. Its value range of 90.81--95.33%
is also wider than expanding history's 99.52--99.54%.

The result identifies sensitivity to aggressive forgetting, not an optimal
history length. Changing the window also changes sample size and which events
supply training rows. All declared settings remain reported, and the primary
policy is unchanged. Historical rows in the table use 359 positives from 12
events; forest rows use the earlier evaluation's 343 from 11. They support
within-period comparisons, not matched comparisons across those two periods.

## 7.11 What changes when more monitoring is available?

Better ranking matters most with a tight budget. At N=5, forest captures 98.05%
of positives and 99.99% of value; quoted gap reaches 93.59% and 98.45%. Two extra
slots shrink their value gap from 38.77 to 1.54 percentage points.

{{FIGURE:101}}

At N=1, forest captures 63.79% of positives and 84.71% of value, against quoted
gap's 46.52% and 43.78%. With two slots it reaches 87.19% and 97.98%. By ten,
forest, quoted gap and freshness all retain every known opportunity. These
additional saved-score allocations have no economic replay.

{{TABLE:token_cap}}

{{FIGURE:301}}

A token cap changes the ordering again. At the primary cap of 50, logistic
captures 357 of 359 positives, forest 354 and historical mean 347. Forest's
recall difference from logistic is -0.84 points, with a weekly interval of
-2.20 to 0.79; against historical mean it is 1.95 points, with an interval of
-1.99 to 10.32. Neither establishes a clear forest advantage.

Cap 25 favours forest recall; at cap 100, historical mean retains all positives
and value. At cap 10, quoted gap retains slightly more value than forest, which
has higher recall. A policy's apparent superiority therefore depends on both
the resource constraint and the metric.

At cap 50, forest averages 45.60 token observations and 4.33 events, versus
logistic's 44.30 and 4.86. The cap is shared, but spending differs and there is
no N=3 restriction. Greedy packing and token counts approximate allocation cost.

## 7.12 Scoring cost and the savings needed to justify it

Scoring overhead can be measured from stored features even though total
monitoring cost is unavailable. The timing sample has four to five candidates
per scan from one block, below the main evaluation's maximum 19. Table
{{TABLE_NUMBER:scoring_overhead}} includes scoring and N=3 selection.

{{TABLE:scoring_overhead}}

The four-job forest takes a median 30.84 ms, versus 11.79 ms with one job,
4.67 ms for logistic and 2.42 ms for quoted gap. Both forest settings preserve
the predictions. Dispatching parallel work adds overhead on this small sample.
These warm, fixed-order timings and their 95th percentiles do not measure live
system tail latency; environment details accompany the saved measurements.

The practical question is whether avoided monitoring exceeds screening overhead.
Let K be all candidate event-token observations, S the selected observations,
A the additional cost of scanning and feature construction, I scoring and
allocation cost, and c the average cost of continuing to monitor one token
for the next hour. Under this additive approximation, savings require

\[ (K-S)c>A+I. \]

The four-job forest avoids a mean 55 observations in the sampled scans.
Measured overhead alone requires 564.35 microseconds of saved work per avoided
token-hour, or 218.53 with one job. Actual required savings are higher when A
adds cost. Token overlap, unequal update rates, feeds and full historical
feedback can also change the comparison. The calculation makes the requirement
explicit; it does not establish end-to-end savings.

## 7.13 Examples of correct selections and misses

The recorded cases show both advantages and failures. Of 343 earlier positive
hours, forest and historical mean both select 264; forest alone selects 60,
historical mean alone six, and neither 13. Forest-only hours contain $135.14
and history-only hours $36.52; most value is retained by both. Table
{{TABLE_NUMBER:paired_cases}} shows the largest-value example from each
disagreement or joint-miss group under Section 5.7's rule.

{{TABLE:paired_cases}}

In Case A, scan margin improves from -$0.18 to -$0.08. Forest ranks the event
second and historical mean seventh before a known $24.55 peak. In Case B, the
same event's margin worsens from -$0.14 to -$0.24 at a later scan. Forest ranks
it fifth while historical mean ranks it third, capturing $33.37 that forest
misses. These traces illustrate decisions without attributing them causally to
the margin feature.

In Case C, forest ranks the event fourth, immediately below the three-slot
cutoff; historical mean ranks it eighth. Both miss $1.58. This is an allocation
miss against competing scores, not a failed binary probability threshold.
Unknown candidates remain in every pool, and complete features and ranks are
saved. The selected cases are illustrations rather than representative hours.

## 7.14 Do retained opportunities produce higher simulated profit?

Forest earns more than quoted gap in the primary execution scenario, but the
advantage depends on one event. Table {{TABLE_NUMBER:economics}} reports known
P&L at 100 ms. An attempt is a submitted basket; completion requires all legs.
Random-policy rows average 20 seeds.

{{TABLE:economics}}

Forest produces $5,873.32 from 453 attempts: 436 complete baskets contribute
$6,290.08 and 17 partial baskets -$416.76. Quoted gap produces $4,370.05,
monitor-all $5,944.97 and random mean $1,123.15. All attempted forest outcomes
are known, although candidate opportunity labels can still be unknown.

Its largest cumulative P&L drawdown is $206.15. The largest basket requests
73,067 shares per leg, and peak required cash reaches $1,426,124.21 in the
no-settlement scenario. The margins therefore rely on substantial position
and funding assumptions. They are not returns measured on deployed capital.

The two-stage forest earns the highest model P&L, $5,921.18, while fixed book
forest leads opportunity capture. Freshness and depth are economically close
to the learned group. The metric used to recommend a monitoring policy therefore
matters: the study retains opportunity capture as its declared criterion.

{{FIGURE:43}}

{{TABLE:exclusion}}

Without December Fed, reranked forest P&L is $730.52 against quoted gap's
$736.36. The economic advantage disappears even though capture remains stronger.
The full-cohort gain of $1,503.27 has a weekly interval of $7.93--$3,866.59,
but this conditional positive interval does not remove dependence on one event,
policy selection or uncertain fills.

## 7.15 Effect of execution delay

Longer assumed delays reduce what the same monitored events earn. Forest P&L
falls from $6,471.72 at zero delay to $5,873.32 at 100 ms and $4,855.08 at
500 ms. Partial-basket results are included in these totals.

{{TABLE:latency}}

Zero delay has no partial loss. At 500 ms, partial outcomes contribute
-$1,190.38. A later arrival can lose enough depth to leave an incomplete basket
that must be sold at a loss. Keeping the predictor fixed exposes this separate
execution dependency. Source-time resolution, competing traders and exchange
queues remain outside the model, so these are scenario results rather than
evidence of obtainable historical trading profits.

# 8 Discussion and conclusion

## 8.1 Answers to the research questions

For RQ1, learning helps allocate monitoring in this cohort. Forest improves
substantially over current quoted gap, and current margin alone does not explain
the full result. Against historical mean, the pooled value improvement is only
1.12 percentage points. The stronger benefit is retaining more positive hours
and serving more events well under equal weighting. Metadata, tuning, ensembles
and TabM give mixed improvements. Added complexity therefore needs evidence
against strong simple controls.

For RQ2, the preferred policy depends on the question and constraint. More slots
reduce the importance of ranking; common token caps can favour logistic or a
historical control. Equal-event intervals give a less decisive forest advantage
than pooled dollars suggest. Central-bank transfer remains strong, while
election transfer weakens. Historical smoothing choices change little; shortening
forest training to two weeks matters more. The earlier-only chooser selects
forest, but its later evaluation has little remaining dollar value.

For RQ3, retaining opportunities is insufficient to establish economic advantage.
Delays create partial losses, and the replay's large positions require substantial
funding under its assumptions. Excluding December Fed preserves forest's capture
gain over quoted gap but removes its simulated P&L gain. Scoring time quantifies
one system cost; the operational saving still depends on unmeasured scanning
and monitoring work.

{{TABLE:evidence_scope}}

## 8.2 Limits of the evidence

The first limitation is the amount of independent evidence. Only 12 events in
eight families have known positive main-evaluation hours, five supported events
are ladders, and one event dominates value. Repeated hourly rows do not create
independent markets. Reweighting, exclusions and resampling expose this
concentration but cannot replace new observations. Policy recommendation and
the additional analyses were designed using an already inspected dataset.

Data acquisition materially constrained the study. After Dome's API retired,
the original source could no longer expand the frozen universe, improve ladder
coverage or supply missing periods. A few recorded events therefore carry much
of the conclusion. This does not establish that comparable opportunities are
absent elsewhere; broader validation requires a replacement depth archive or
prospective collection.

Coverage is a second limitation. Known-only metrics cannot reveal missed
opportunities in unknown windows. Frozen membership, metadata and offline
recording extent differ from a live catalogue. The processed-data checks
reproduce inherited labels without independently auditing raw reconstruction.
Training and historical comparisons use recorded earlier outcomes; acquiring
feedback for unselected events in deployment remains unresolved.

The cost and execution models are also incomplete. Token observations ignore
different update rates and can count shared tokens twice. Greedy allocation is
not optimal, and cached-feature timing covers only four to five candidates on
one machine. Replay assumes source-time observation, separate per-leg FOK
support, zero fees and extra charges, unlimited funding and a one-second unwind.
Queues, competing consumption, network congestion, exchange acknowledgements and
settlement cash release are unmodelled. Conditional intervals exclude these
uncertainties and are unadjusted for all model comparisons. Selected decision
cases illustrate behaviour without identifying causes.

## 8.3 Next steps

The first priority is a validated replacement for Dome: full historical depth
updates or prospective collection that distinguishes missing data from unchanged
books. Prices alone cannot establish supported quantities. A broader universe,
especially ladders, should be audited and fixed before evaluation, with
membership and source/receive timestamps recorded as known at the time. The
selected policy then needs independent future events and periods.

Operational work should measure scan construction, update rates, order latency,
exchange responses and fills. Learning from selected events should use the
labels those selections can actually supply. Fees, bounded capital and
settlement release need declared scenarios. New horizons or baskets require
rebuilt targets and explicit payoff and order rules. Further tuning should
answer a stated uncertainty rather than substitute for better-observed data.

## 8.4 Conclusion

This dissertation shows how to evaluate limited monitoring separately from
execution. Its contribution is a reproducible formulation and implementation
with comparisons against current-book and historical priorities. Book-based
learning retains strong opportunity capture in the frozen Dome cohort. Its
incremental benefit depends on the objective, available history and monitoring
budget, while concentration and execution assumptions qualify the economic
result. Broader deployment claims require independent data and operational
measurements.

# References

Bailey, D. H., Borwein, J. M., López de Prado, M. and Zhu, Q. J. (2017). The
probability of backtest overfitting. Journal of Computational Finance, 20(4),
39–69. https://doi.org/10.21314/JCF.2016.322

Breiman, L. (2001). Random forests. Machine Learning, 45, 5–32.
https://doi.org/10.1023/A:1010933404324

Cawley, G. C. and Talbot, N. L. C. (2010). On over-fitting in model selection
and subsequent selection bias in performance evaluation. Journal of Machine
Learning Research, 11(70), 2079--2107.
https://www.jmlr.org/papers/v11/cawley10a.html

Chen, T. and Guestrin, C. (2016). XGBoost: A scalable tree boosting system.
Proceedings of KDD, 785–794. https://arxiv.org/abs/1603.02754

Chen, W., Wang, Y. and Yuan, Y. (2013). Combinatorial multi-armed bandit:
General framework and applications. Proceedings of ICML, PMLR 28(1), 151--159.
https://proceedings.mlr.press/v28/chen13a.html

Dome (2026). Dome API: API end-of-life notice. https://docs.domeapi.io/
(accessed 1 November 2026).

Gorishniy, Y., Kotelnikov, A. and Babenko, A. (2025). TabM: Advancing tabular
deep learning with parameter-efficient ensembling. ICLR 2025.
https://arxiv.org/abs/2410.24210

Grinsztajn, L., Oyallon, E. and Varoquaux, G. (2022). Why do tree-based models
still outperform deep learning on tabular data? NeurIPS 2022.
https://arxiv.org/abs/2207.08815

Saguillo, O., Ghafouri, V., Kiffer, L. and Suarez-Tangil, G. (2025). Unravelling
the probabilistic forest: Arbitrage in prediction markets. arXiv:2508.03474.
https://arxiv.org/abs/2508.03474

Trapeznikov, K. and Saligrama, V. (2013). Supervised sequential classification
under budget constraints. Proceedings of AISTATS, PMLR 31, 581--589.
https://proceedings.mlr.press/v31/trapeznikov13a.html

Wolfers, J. and Zitzewitz, E. (2004). Prediction markets. Journal of Economic
Perspectives, 18(2), 107–126. https://doi.org/10.1257/0895330041371321

# Appendix A Reproduction and artifact guide

The standalone FYP package separates data preparation, structural rules, models,
experiments, economic replay and reporting. The notebook research.ipynb
orchestrates these stages. Its cached mode verifies saved data and fitted scores
and regenerates analyses; its full mode reconstructs examples from raw books,
retunes and refits models, and reruns persistent execution into a new run
directory. Both modes require the relevant saved local files and operate without
a parent repository or provider token. With the Dome API retired, preserving and
sharing these files is essential for reproduction.

The data and simulator guides document schemas and assumptions. The artifact
table and figure indexes map every catalogued output to a CSV source or
supporting dataset. The main report presents selected comparisons; the full
50-figure catalog supports additional inspection without filling the volume with
redundant plots.

The processed-data extension is stored separately under
artifacts/extensions/processed_review_v1. Its figures, tables, ablation
predictions, per-fit audits, input digests and source-backed design record
supplement the original 50-figure catalogue. It uses the local earlier
opportunity-value examples and saved main-study predictions. The experiment can
be rerun with: python FYP/experiments/processed_extension.py --examples
PATH_TO_EARLIER_EXAMPLES. The extension neither accesses Dome nor reconstructs
raw books. Its original-study results are checked against the saved N=3 and N=5
metrics before new analyses proceed.

The additional robustness extension is stored under
artifacts/extensions/robustness_review_v1. Running
python FYP/experiments/robustness_extension.py reproduces its four historical
controls, 252 family-withheld fits and balanced metrics from the packaged
processed examples and saved predictions. A source snapshot, frozen design,
input fingerprints, per-scan history cutoffs, per-fit family exclusions,
predictions and paired interval outputs accompany the results. Every family
scenario scores the whole eligible pool before its target-family outcomes are
summarised. These outputs add two figures to the separate extension catalogues.

The extension includes a compact copy of its earlier processed examples under
its inputs directory. The full raw-book archive and complete main-study feature
table are required for the original end-to-end reconstruction but are not used
by this extension. Running python rebuild_report.py regenerates the presentation
tables and dissertation from saved evidence without fitting models or accessing
raw books. All new ablation labels inherit the original preprocessing
assumptions and have not been independently re-established from raw snapshots in
this revision.

The allocation extension under artifacts/extensions/allocation_review_v1 uses
the same saved predictions, earlier examples and historical-control scores.
Running python FYP/experiments/allocation_extension.py reproduces token-cap
allocations, the four-week-warm-up chooser and paired cases without model
refitting. Its design and executed source accompany per-scan budget audits,
per-origin selection histories, complete case features and competing candidate
tables. Its resource curve adds one figure to the extension catalogues.

The 28 tests exercise payoff constraints, backward as-of reads, duplicate
timestamps, recording endpoints, empty versus unknown books, integer depth
sizing, uncapped quantities, tick/minimum constraints, short opportunities,
incomplete labels, half-open windows, peak-value aggregation, positive-only
amount fitting, unknown slot consumption, chronological purging, selection
invariance, rank ensembles, primary execution settings, persistent reservations
and partial/unknown execution outcomes. They test implemented safeguards, not
the completeness of the original collector or the truth of real fills.

Three additional tests under extension_tests check strict historical-label
availability, invariance to changes in later labels, and exclusion of unknown
windows from historical counts. They also check that a known zero replaces an
earlier positive as the last observed value. These checks protect the new
historical controls without changing the original 28-test suite.

Five allocation tests additionally check that expensive events are skipped
without exceeding the cap, unknown outcomes retain their resource allocation,
labels ending exactly at a chooser origin remain unavailable, changes to later
targets cannot alter policy choice, and illustrative cases use disjoint groups
of known positives. Two further rolling-window tests verify the included lower
scan boundary, the excluded label-end boundary, unknown-label exclusion and
nested available histories. The extension suites contain ten tests in addition
to the original 28.

The focused sensitivity study under artifacts/extensions/sensitivity_review_v1
is reproduced with python FYP/experiments/sensitivity_extension.py. It records
the three smoothing strengths, two rolling windows, 54 new fits and the reused
expanded controls in a frozen design and source snapshot. Saved predictions,
availability and fit audits, matched-period metrics and original-score
reconciliations support each comparison. Timing samples include every measured
call, scan size, avoided token count, timer unit, environment and declared
exclusions. Forest timing configurations preserve the saved scores. Warm timings
are retained on resume; rerunning on another machine requires a new output
directory. Presentation tables are regenerated from these saved measurements,
not retimed during a report build.

# Appendix B Configuration and provenance

inputs/cohort.json records complete basket membership and validity intervals.
inputs/raw_inventory.csv records every required raw history and its digest, a
file fingerprint used to detect changes. inputs/study.json records the feature
views and bounded configuration grids. Weekly bundle files record selected
parameters and feature order; fit audits record the actual training populations
and cutoffs. The search contains 168 inner candidate evaluations, and the weekly
audit records 199 distinct fitted artifacts or stages. Identical fixed and tuned
classifier configurations can share an artifact, so this count does not
represent independent supporting experiments. Tuning search and selected
hyperparameter CSVs provide the detailed configurations supporting Chapter 5.

Source, input and output digests detect changes during reproduction.
Verification recomputes weekly scores from fitted artifacts, checks temporal
separation, reconstructs capture metrics and reconciles cumulative known P&L
with economic summaries. These controls establish reproducibility of the
supplied study, not point-in-time availability of frozen metadata or
completeness of the feed.

# Appendix C Exact search configurations

The following tables enumerate the declared candidate lists used by the primary
study. IDs are zero-based and resolve otherwise exact tuning ties by declaration
order. Fixed classical controls use candidate zero; the two-stage forest uses
the separate settings in Table {{TABLE_NUMBER:fixed_settings}}. TabM lists the
four configurations actually searched, rather than the larger upstream
architecture catalogue. The selected settings and refit durations are recorded
below so a reader need not reconstruct them from serialised model files.

{{TABLE:grid_logistic}}

{{TABLE:grid_random_forest}}

{{TABLE:grid_xgboost}}

{{TABLE:grid_tabm_two_stage}}

{{TABLE:selected_settings}}

# Appendix D Implementation details

This appendix retains the numerical preprocessing, optimisation and validation
details supporting Chapter 5. They are part of the implemented protocol;
moving them here leaves the model fits and reported results unchanged.

## D.1 Classical preprocessing and training

The classifiers optimise their standard classification losses rather than a
differentiable top-N ranking loss. Logistic regression uses penalised binary log
loss; the forest grows Gini-criterion trees; XGBoost uses the binary logistic
objective with histogram tree construction. The fixed classifiers use class
balancing, including a training-count negative/positive ratio for XGBoost. The
two-stage gates deliberately omit this balancing because their outputs multiply
a conditional value estimate. All classical models use the declared seed
20261001 and four worker threads where applicable.

For the classical models, missing numeric values are replaced with training-set
medians, with additional indicators identifying missing values where applicable.
A signed logarithmic transform compresses large magnitudes while retaining their
signs, followed by standardisation. Categorical metadata is represented by
binary indicators for categories observed in training; unseen categories
introduce no new indicators at prediction time. All preprocessing is fitted on
the relevant training rows and saved with the model, so validation and
evaluation data do not determine medians, scales or category mappings.

The signed transform is \(\operatorname{sgn}(x)\log(1+|x|)\). Standardisation
uses training-fitted means and variances after imputation and transformation. An
entirely missing numeric feature is retained by the imputer, preserving the
declared input schema. The metadata comparison adds event type, broad topic and
creation age; it does not introduce event identifiers as category features. The
numeric age follows the same classical pipeline as book measurements.

Each tuned classical family evaluates eight hyperparameter configurations.
Logistic settings vary regularisation strength, L1/L2 penalty and class
weighting. Forest settings vary depth, leaf size and feature subsampling with
200 trees. XGBoost settings vary tree count, depth, learning rate, child weight,
subsampling and regularisation. These bounded searches compare declared
alternatives rather than exhaustively optimise each family; the exact grids and
selected settings are exported.

## D.2 TabM architecture, losses and refits

TabM uses 32 ensemble members within each stage and tests four configurations
from the declared architecture grid. Continuous variables use a training-fitted
quantile transformation to a normal distribution, and categories use one-hot
encoding. Training computes losses separately for the members; prediction
averages their probabilities or amount estimates. Batch size is 1,024, and
training runs for at most 60 epochs, where an epoch is one pass through the
training examples. Early stopping allows ten epochs without an improvement in
validation loss.

TabM uses the official tabm architecture with K=32 members and one output per
member. The four candidates have block/width/dropout settings (2,128,0.1),
(1,64,0), (2,64,0.1) and (3,128,0.1); their optimiser settings are given in
Appendix C. AdamW updates the model, with gradient norm clipped to one.
Continuous features receive median imputation and a normal-output quantile
transform fitted with at most 1,000 quantiles and no subsampling. Binary numeric
columns use most-frequent imputation; categorical columns use one-hot encoding.

For the probability stage, each member receives the binary label and contributes
a binary cross-entropy loss computed from its logit. For the amount stage,
positive target values are standardised by their training mean and standard
deviation, with scale bounded below by one millionth. Each member contributes
squared error on this standardised target. The batch losses average over
observations and members. Inference averages member probabilities for the gate
and member values for the amount model, reverses the amount scaling, and clips
negative amount predictions to zero.

Early stopping is assessed on the ensemble prediction: binary cross-entropy of
mean gate probabilities, or squared error of mean standardised amount
predictions. The validation examples fit neither feature preprocessing nor
target scaling. For a fixed-epoch weekly refit, no validation outcomes are
supplied. The three refit seeds are 20261001, 20261002 and 20261003; averaging
occurs after multiplying each seed-specific gate and amount prediction.

Inner tuning uses one fixed random seed. Weekly refits train three seed-specific
pairs of probability and amount models, multiply the two predictions within each
pair, then average the three products. For each stage, the refit duration is the
median best epoch count from the selected configuration's inner validation fits,
rounded up. This duration is retained until the next scheduled tuning date, so
subsequent evaluation outcomes do not determine when training stops.

## D.3 Exact validation boundaries

The inner boundaries are defined from distinct scheduled scan times in the
earlier history. The first block begins at the halfway indexed scan, the second
at the three-quarter indexed scan, and the second ends one hour before the outer
fitting origin. Because the first recorded scan is not midnight, these inner
boundaries need not align with midnight or whole calendar weeks. Table
{{TABLE_NUMBER:validation_windows}} records their actual timestamps. Every
candidate for a family sees the same validation candidate sets, including
unknown rows that consume slots. Known labels alone supply fitting targets and
capture denominators.

## D.4 Historical estimates and policy-choice ties

The controls rank by past event opportunity frequency, past event mean quoted
value, the event's last known quoted value, or past family mean quoted value.
For frequency and mean value, a family estimate is its observed sum plus 24
times the global observed mean, divided by its known count plus 24. An event
estimate uses the same calculation with the corresponding family estimate as
its prior. The fixed 24-observation smoothing strength represents one day's
hourly observations and is not tuned. An event without history falls back to
its family estimate; a globally empty history gives zero. Last-known value
includes observed zeros and uses family mean value only when that event has no
known history. Ties retain the main study's event-identifier ordering.

Earlier-only policy choice treats differences within $0.000000001 as ties and
uses the policy order in Section 5.7. This is a numerical tolerance for the
choice procedure, not a profit threshold. Individual scores, selected rows and
cutoff audits are retained in the allocation artifacts described in Appendix A.
