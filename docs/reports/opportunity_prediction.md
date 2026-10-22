# Opportunity prediction independently of execution

## Research question and scope

Can scheduled snapshots identify which events should receive the limited monitoring
budget during the next hour? The first layer measures both opportunity occurrence
and displayed dollar magnitude. Economic replay is a separate second layer:
latency, partial fills and unwinds must not determine these learning targets.

This completed development study compares **32 ranking policies**, including
rules, fixed models, tuned models and ensembles. It performs **480 inner
candidate evaluations** and saves **72 outer fitted models**, using only the
frozen Dome cohort. The final test from **8 January 2026 onward remains closed**.
The earlier P&L-target studies and replays are unchanged.

The strongest development results come from the occurrence classifiers, rather
than the direct value models. Book-only logistic regression captures the most
positive hours. Random-forest occurrence models capture the greatest share of
quoted value. Tuning, metadata and ensembles do not consistently improve results.
These findings support a shortlist for subsequent economic evaluation, not a
final winning or commercially profitable strategy.

## Targets: what the models learn

At each scheduled scan, features describe the state then available. No future
updates or execution outcomes enter those predictors. Two labels describe the
following half-open window `[scan, scan + W)`:

1. **Occurrence:** at least one qualifying quoted basket exists, even briefly.
   A basket already visible at the scan counts. Observed new onsets after the
   scan are also scored separately; a first observation following unknown data
   is not automatically an observed onset.
2. **Quoted value:** the largest qualifying displayed dollar margin of one basket
   at one observed state in the window. The optimizer checks feasible integer
   quantities using full depth and takes `q - purchase cost` for the best order.
   Zero means a fully observed window with no qualifying opportunity.

Both labels require full-window observation coverage. Unknown labels remain
missing and still consume shortlist slots. Quote qualification requires clean,
two-sided, noncrossed books; usable ticks/minimum sizes; at least five shares
or the largest member minimum; positive displayed margin; and positive margin
at the submitted worst-price limits. There is no artificial cash/share cap,
minimum duration, quote-age cutoff, arrival delay or unwind outcome in the labels.
The frozen cohort uses zero exchange fees. Source-time alignment and complete
capture of book changes remain assumptions.

The quoted-value label is **not total hourly profit**. Taking a maximum prevents
repeated snapshots or overlapping ladder pairs from being added as new money.
It also ignores the possibility of collecting several independent opportunities
within the same hour. Across successive hours, maxima may still refer to unchanged
liquidity. A sum of hourly maxima is a ranking-weight denominator, never an
estimate of cash a strategy could collect. Achievable returns require replay.

The feature `x_best_quoted_net_usd` remains in all feature views, together with its
previous scheduled value and change. It describes the **current** quote; the value
target describes the **future window**. The feature does not itself certify all
entry checks. Neither future label is supplied as a predictor.

## Data and separation

| Population | Events | Eligible event-hours | Known positive | Known negative | Unknown |
| --- | ---: | ---: | ---: | ---: | ---: |
| All development | 48 | 26,835 | 454 | 21,373 | 5,008 |
| Outer evaluation | 44 | 20,951 | 385 | 16,572 | 3,994 |

The full development cohort is 43 negRisk and five ladder events. In outer
evaluation, 362 positive hours are negRisk and only 23 are ladders. This is weak
evidence for a general ladder-market conclusion.

The 33 scheduled book features are exactly equal to the existing uncapped dataset.
Combined models add event type, broad topic and age: 36 raw input columns before
encoding. Raw inputs are checked against the frozen file hashes. Labels are
rebuilt from raw books, not copied from executed trades.

All **17 previously net-losing hours** now have a positive opportunity label.
Ten hours previously labelled as having no profitable attempt become positive;
all 444 previous positive-attempt hours remain positive. None of the 5,008 unknown
hours become known. There are also 75 witnessed-positive unknown hours, retained
only as diagnostics. This explicitly separates entry evidence from execution loss.

| Outer evaluation period, UTC | Known earlier training hours | Positive training hours |
| --- | ---: | ---: |
| 1?21 November 2025 | 4,859 | 69 |
| 22 November?14 December 2025 | 11,191 | 234 |
| 15 December 2025?7 January 2026 | 17,690 | 405 |

Every training label window finishes strictly before the next evaluation starts.
Each outer fold uses two chronological inner validation blocks drawn from its
training history. Preprocessing is fitted within each fit, including imputation,
scaling and category encoding. There is no random train/test split or early
stopping using outer outcomes. Changing W requires matching feature/label rebuilds.

## Models, tuning and combinations

- **Occurrence:** logistic regression, random forest, histogram gradient boosting
  and XGBoost. Each has a fixed book-only fit, a fixed combined fit, and a tuned
  combined fit. Three equal-weight percentile-rank ensembles combine their scores.
- **Direct value:** random forest, histogram boosting and XGBoost regress the
  nonnegative peak-value target using the combined features.
- **Two-stage value:** the same three families estimate `P(opportunity | X)` and
  `E(peak value | opportunity, X)`, then multiply. Gates use natural prevalence
  without class weighting; amount models fit positive windows only. Each stage
  fits its own preprocessing. Negative amount predictions are clipped to zero.
- **Value comparisons:** fixed and tuned versions of each method, plus separate
  fixed/tuned equal-weight averages of the six dollar predictions.
- **Rules:** quoted-gap, displayed-depth and freshness. Random capture is computed
  exactly from the candidate pool at each scan, without random simulation noise.

Each method has eight declared settings and two inner blocks per outer fold.
Logistic search varies C, L1/L2 penalties and class weights. Forest search varies
depth, minimum leaf size and feature sampling. Histogram boosting varies
iterations, learning rate, leaf count/minimum size and L2 regularisation. XGBoost
also varies child weight, row/column sampling and L1 regularisation. For the
hurdles, gate and amount stages share the declared tree settings; they are not
separately exhaustively tuned. Full grids and selected settings are saved.

Classifiers select settings by pooled known positive-hour recall@5, breaking ties
by mean inner average precision and then declaration order. Value models select
by captured peak-value sum@5, breaking ties by positive-hour capture and then
order. Neither uses executed P&L. N=3 is a sensitivity check on the same fitted
scores; it does not receive separate tuning. Ensemble weights are fixed equally,
not learned on outer evaluations. Class-weighted classifier scores are ranking
scores, not assumed calibrated probabilities.

## Development results

**Recall** is the fraction of known positive event-hours selected. **Value capture**
is the fraction of summed known hourly peak quote values selected. Unknown rows
are ranked before scoring and do not silently become negatives. The selected
budget is up to N events per scan, without a learned abstention threshold.

| Policy | Recall@5 | Value capture@5 | Recall@3 | Value capture@3 |
| --- | ---: | ---: | ---: | ---: |
| Quoted-gap rule | 91.69% | 97.89% | 70.91% | 60.08% |
| Book-only logistic | 97.66% | 99.43% | 88.83% | 98.07% |
| Book-only random forest | 95.32% | 99.86% | 88.05% | 98.45% |
| Forest + metadata | 95.32% | 99.87% | 86.75% | 97.76% |
| Tuned occurrence forest | 93.77% | 99.10% | 83.38% | 93.09% |
| Tuned occurrence XGBoost | 94.55% | 99.80% | 80.00% | 94.00% |
| Tuned occurrence rank ensemble | 95.06% | 99.84% | 83.12% | 87.87% |
| Fixed direct-value forest | 91.95% | 99.02% | 75.84% | 67.24% |
| Tuned direct-value forest | 90.65% | 91.11% | 76.36% | 66.91% |
| Fixed two-stage forest | 93.25% | 99.01% | 84.94% | 97.46% |
| Tuned two-stage forest | 92.73% | 99.46% | 78.18% | 70.78% |
| Tuned direct-value histogram boosting | 71.17% | 64.45% | 60.52% | 61.12% |
| Tuned direct-value XGBoost | 69.87% | 65.60% | 56.36% | 54.78% |
| Tuned two-stage histogram boosting | 67.79% | 96.48% | 58.96% | 89.10% |
| Tuned two-stage XGBoost | 55.58% | 54.80% | 45.45% | 47.19% |
| Tuned value ensemble | 90.65% | 98.96% | 75.84% | 73.17% |

Full comparisons, precision, average precision, unknown selections, fold-level
results and random expectations are in `results/opportunity_value/metrics.csv`.
The quoted-gap rule ranks on current `x_best_quoted_net_usd`, highest first, with
missing quotes last and deterministic event-name tie-breaking. It is not restricted
to positive current quotes. Models can use history, depth and quality to prioritise
events whose opportunity appears later.

At N=5, book-only logistic captures **376/385** positive hours, combined forest
**367/385**, and quoted-gap **353/385**. At N=3, book-only logistic captures
**342/385**, book-only forest **339/385**, and quoted-gap **273/385**. The occurrence
advantage is not equivalent to a profit advantage.

Fixed combined forest leads value capture@5 at **99.867%**, versus quoted-gap's
**97.892%**. Fixed book-only forest leads value capture@3 at **98.448%**, versus
**60.081%**. The best value-specific method at N=5 is the tuned two-stage forest
(**99.462%**); at N=3 its value capture falls to **70.781%**, whereas its fixed
counterpart achieves **97.460%**. Optimising the inner N=5 objective has not made
that model robust to the tighter budget.

More complex value models are not consistently stronger. Tuned direct XGBoost
captures **65.596%** of quoted value at N=5 and tuned two-stage XGBoost **54.796%**.
The tuned value ensemble reaches **98.964%** at N=5 but **73.170%** at N=3, below
simple occurrence forests. Do not discard the fixed controls because they win:
measuring the lack of reliable tuning/ensemble gains is a research result.

## Newly appearing opportunities

Of 385 positive evaluation hours, 152 have a qualifying gap at the scan; 265
contain an observed later onset, including 32 that also have a gap at the scan.
The stricter initially opportunity-free subset has **233 positive hours**.
Selections are made across the full pool before this diagnostic subset is scored.

At N=5, book-only logistic captures **224/233** of these hours, compared with
quoted-gap's **201/233**. Combined forest captures **215/233** but **99.763%** of
the subset's peak value, versus quoted-gap's **96.249%**. At N=3, book-only forest
captures **97.239%** of that value; quoted-gap captures **28.979%**. This indicates
useful prediction beyond merely recognising a gap already visible at screening,
within this recorded development population.

## Robustness, concentration and features

The distribution is highly skewed. Across the 385 positive outer-evaluation
hours, the median peak margin is **$1.168**, the 90th percentile **$66.87** and
the largest **$1,366.80**. December Fed contributes **82.83%** of summed peak values.
These are displayed margins, not profits or amounts available simultaneously.

After excluding December Fed and reranking, book-only logistic captures
**283/285** positive hours at N=5 versus quoted-gap's **278/285**. Value capture
is **99.991%** versus **99.772%**: almost a ceiling and a very small difference.
At N=3, book-only forest captures **97.566%** of remaining value, versus
quoted-gap's **83.938%**. This exclusion is a concentration diagnostic, not a fit
on unseen event families.

The forest's value-capture results are reasonably stable across chronological
folds: combined forest@5 captures **99.826%, 99.971%, 100%**; book-only forest@3
captures **97.969%, 99.789%, 99.525%**. There are only **165, 171 and 49** positive
hours in the respective evaluation periods, and related hours are not independent.

Paired resampling of nine event families gives a development interval of about
**+16.45 to +39.61 percentage points** for book-only forest's N=3 value-capture
advantage over quoted-gap. For the N=5 combined forest, the interval reaches zero.
These are exploratory cluster intervals with only nine families, many compared
methods, and no multiplicity correction; they do not establish a general-market
advantage or replace untouched testing.

Stratum results also differ: at N=5 the quoted-gap rule captures all **23/23**
positive ladder hours, compared with logistic's **21/23** and forests' **20/23**.
The aggregate model advantage mainly concerns negRisk events. Unknown selections
vary materially: logistic selects **461** unknown hours at N=5, versus the rule's
**1,518**. Models may favour better-recorded events; unobserved opportunities
cannot be scored, so capture among known hours is not full-market recall.

Within-scan permutation uses three fixed-seed repetitions for six feature groups.
Quoted-gap inputs are the strongest group for each tuned occurrence classifier;
mean recall drops range roughly from **24 to 40 percentage points** across folds.
Scheduled history contributes too. For tuned two-stage forest, shuffling quoted
gaps lowers mean value capture by **28.83 points**, and shuffling history by
**15.00 points**. Metadata and tuning gains are inconsistent: adding metadata
reduces fixed logistic recall@5 from **97.66% to 93.51%**, while fixed forest's
recall remains **95.32%** and value capture changes only slightly. Permutation
groups overlap; effects are neither additive nor causal. Coefficients for the
book-only logistic models are saved for inspection on transformed inputs.

The plausible explanation for weak direct-value fitting is the combination of
rare positives, strongly skewed amounts and few independent event families.
This is an interpretation, not a proved causal mechanism. A larger algorithm or
additional tuning cannot manufacture independent evidence absent from the data.

## Reproduction and next research layer

Run [notebook 06](../../notebooks/06_opportunity_prediction.ipynb). Its default
loads and verifies saved artifacts. Use a fresh run suffix to rebuild; completed
outputs are not overwritten. Core implementation:

| Module | Responsibility |
| --- | --- |
| `src/data/opportunities.py` | Raw-book occurrence labels and coverage |
| `src/data/opportunity_value.py` | Recheck all qualified updates and derive peak quoted value |
| `src/models/opportunities.py` | Classifiers, nested recall tuning, rank ensembles |
| `src/models/opportunity_value.py` | Direct/hurdle value models and nested value tuning |
| `src/reporting/opportunities.py` | Label, split, fit and feature audits |
| `src/reporting/opportunity_value.py` | Matched value/occurrence comparisons and robustness diagnostics |

The new labels and fitted artifacts are hash-audited. The eligible population and
all 33 snapshot predictors match the frozen parent exactly. Saved classifier
predictions are carried unchanged into the combined value comparison. All **138
tests pass**, including 10ms gaps, latency-invariant labels, unknown coverage,
window-boundary purging, unknown-budget consumption, objective-specific tuning,
repeated-liquidity peak handling and unweighted hurdle fitting. No fit warnings
were recorded. The report and notebook perform independent artifact checks.

Keep book-only logistic, book-only/combined forest occurrence policies and the
quoted-gap rule as strong development comparators; retain tuned value/hurdle
results as evidence about objective choice. A later economic study should hold
these predictions fixed while changing arrival latency and execution assumptions,
starting from the existing uncapped 100ms benchmark. The separate
[economic replay report](opportunity_economic_replay.md) evaluates these saved
predictions. Its execution assumptions do not change the first-layer labels or
fitted models. Do not transfer the earlier models' P&L to them or select a final
model using the unopened test.
