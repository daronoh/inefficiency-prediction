# Model tuning and development analysis

## Research question and fixed conditions

This study asks whether bounded hyperparameter tuning and XGBoost improve event
screening over the existing fixed-setting models and quoted-gap rule. It uses
the same Dome dataset, combined order-book and metadata features, eligible event
pool and three expanding chronological development folds. It does not acquire
new data or change the strategy's execution assumptions.

The execution contract remains 100 ms arrival, uncapped cash and share quantities,
immediate submission after candidate detection, and a one-second unwind delay
after arrival for incomplete baskets. Partial-fill losses remain in the targets
and replay. Top five is the primary monitoring budget; top three is a sensitivity.
The final test beginning 8 January 2026 remains unopened.

This is an exploratory extension after the original development results were
examined. Nested tuning prevents each outer evaluation period from selecting its
own hyperparameters, but does not erase researcher adaptation to previously seen
development findings. The final selected procedure still requires a separate
untouched evaluation.

## Models and targets

| Approach | Tuned methods | Prediction used to rank events |
| --- | --- | --- |
| Occurrence classification | Logistic regression, random forest, histogram boosting, XGBoost | Probability that at least one profitable attempt occurs |
| Direct regression | Random forest, histogram boosting, XGBoost | Signed net simulated P&L over the following hour |
| Two-stage prediction | Random forest, histogram boosting, XGBoost | Probability of any attempted trade multiplied by expected signed P&L conditional on activity |

The original combined-feature classifiers, regressors, two-stage models and
ensembles are copied from their saved predictions without refitting. The quoted-gap,
depth and freshness rules are retained. XGBoost additionally has a fixed comparator:
the first declared candidate, fitted on exactly the same outer training rows as
its tuned counterpart. "Fixed" here means a declared study configuration, not
the library defaults.

Occurrence is distinct from positive net hourly P&L: an hour can contain a
profitable attempt and still lose money overall. Direct and conditional regressors
retain negative and zero returns. Two-stage activity gates are unweighted and
use the observed prevalence, without oversampling or a hard probability cutoff.
Classifier-only XGBoost uses training-fold negative/positive class weighting;
its probabilities are ranking scores, not assumed calibrated probabilities.

## Search design and chronology

Every tuned method receives **eight configurations and two inner validation
blocks in each outer fold**. Search settings are saved in `design.json` before
fitting. The two inner blocks occupy the final two quarters of the scan times
available before the outer evaluation boundary. Each inner model is fitted on
earlier known-label rows; all events share time boundaries.

Both inner and outer training use a separation of one hour plus 1.1 seconds,
covering the label horizon, arrival and unwind. Inner validation targets also
finish before the outer evaluation begins. Imputation, scaling, one-hot encoding
and class-weight calculations are fitted separately on each training subset.
There is no random row split or early stopping. The number of boosting iterations
is part of the declared search.

| Family | Parameters varied |
| --- | --- |
| Logistic regression | C = 0.01, 0.1, 1 or 10; selected L1/L2 and balanced/unweighted combinations |
| Random forest | Depth, minimum leaf size, feature subsampling; 200 trees throughout |
| Histogram boosting | Iteration count, learning rate, leaf count, minimum leaf size, L2 regularisation |
| XGBoost | Tree count, learning rate, depth, minimum child weight, row/column subsampling, L1/L2 regularisation |

These are eight explicit combinations per family, not exhaustive Cartesian grids.
The two-stage search shares its selected structural settings between the gate and
amount model to limit computation. Its gates remain unweighted. The original
two-stage forest/boosting comparators used separate gate/amount settings, including
different conditional leaf sizes;
their matched comparison therefore measures the whole new fitting procedure,
not one isolated hyperparameter.

The inner selection score is **signed standalone top-five target P&L per scan**,
aggregated over the two validation blocks. Unknown labels remain missing, but
their events participate in ranking and consume slots. Exact ties select the
first declared configuration. The objective is not classification accuracy or
the number of profitable attempts.

This score is a computational proxy. Each hourly target starts with an empty
liquidity ledger, whereas the actual policy maintains reservations across hours.
It also evaluates only known targets. It must not be interpreted as strategy
profit, and may favour allocations with unknown outcomes. Reported coverage and
the subsequent persistent replay make these limitations visible. No candidate
is selected using top-three results.

After inner selection, the chosen configuration is refitted on all usable outer
training rows and predicts the next chronological evaluation period. The first
outer period begins on 1 November 2025, the second on 22 November, and the third
on 15 December. Development ends before 8 January 2026.

The earliest inner training block has only **31 attempted-trade hours** among
2,481 known-label examples. Its validation blocks contain 1,201 and 1,165 known
labels. This sparse activity limits the precision of hyperparameter selection,
especially for the conditional amount model. Full counts and boundaries are
recorded in `search.csv`; outer training counts are in `fit_audit.csv`.

| Outer evaluation begins | Known training hours | Hours with attempted trades | Net-losing training hours |
| --- | ---: | ---: | ---: |
| 1 November | 4,859 | 69 | 1 |
| 22 November | 11,191 | 234 | 7 |
| 15 December | 17,690 | 405 | 11 |

Thousands of mostly inactive rows should not be confused with thousands of
independent execution outcomes. In particular, one net-losing training hour in
the first fold offers very little evidence for learning execution-loss risk.
These counts concern hourly totals; a profitable hour can also contain a losing
attempt. Increasing model complexity cannot replace missing examples of loss.

## Results

Both persistent replays completed successfully. The comparison includes 28 saved
ranking policies plus monitor-all, evaluated at both budgets and both universe
scopes. Every listed replay has zero unknown attempted-trade P&Ls. This does not
make unknown hourly labels known: complete-window label coverage is a stricter
requirement than observing the outcome of a particular simulated attempt.

All figures below are net simulated P&L in USD across the development evaluation
period, including partial-fill losses. The Fed-excluded columns are separately
reranked replays, not the full-pool result minus that event's contribution.

| Policy | Top 5 | Top 3 | Top 5, Fed excluded | Top 3, Fed excluded |
| --- | ---: | ---: | ---: | ---: |
| Quoted-gap rule | $6,045.25 | $4,379.96 | $726.57 | $729.50 |
| Original forest classifier | $5,942.71 | $5,910.43 | $726.81 | $655.87 |
| Original logistic classifier | $5,925.46 | $3,749.18 | $688.34 | $678.88 |
| Original histogram classifier | $5,929.10 | $5,203.83 | $709.59 | $707.66 |
| Tuned logistic classifier | $5,897.60 | $5,881.25 | $689.18 | $667.17 |
| Tuned forest classifier | $5,940.06 | $5,830.21 | $725.75 | $680.65 |
| Tuned histogram classifier | $5,937.18 | $6,009.60 | $725.78 | $707.89 |
| Fixed XGBoost classifier | $5,933.55 | $5,815.29 | $719.33 | $655.96 |
| Tuned XGBoost classifier | $5,941.94 | $5,044.30 | $721.24 | $701.34 |
| Tuned forest P&L regression | $5,907.55 | $4,667.14 | $704.65 | $729.56 |
| Tuned histogram P&L regression | $5,754.78 | $4,677.21 | $580.10 | $590.62 |
| Tuned XGBoost P&L regression | $5,670.26 | $5,302.39 | $546.62 | $561.23 |
| Tuned two-stage forest | $5,921.84 | $4,642.60 | $715.48 | $717.24 |
| Tuned two-stage histogram | $5,783.37 | $5,558.19 | $577.34 | $554.22 |
| Tuned two-stage XGBoost | $5,709.34 | $4,485.42 | $661.66 | $672.00 |
| Monitor all | $5,951.60 | $5,951.60 | $729.15 | $729.15 |

### Primary result: top five

**No tuned model beats quoted-gap's $6,045.25.** The original forest remains the
strongest learned policy at $5,942.71; tuned XGBoost classification earns
$5,941.94, only $0.77 less. Its completed-basket profit is $6,356.69, offset by
$414.75 in partial-execution losses. Quoted-gap earns less from completed baskets
($6,328.72) but loses only $283.47 on partial attempts. The economic ordering
therefore differs from the occurrence-recall ordering.

Tuning improves the original histogram P&L regressor by **$482.79** and the forest
P&L regressor by **$113.79**. Outside the December Fed event, these gains shrink to
**$16.63** and **$5.51**. Tuned XGBoost regression improves its fixed comparator by
$36.88 overall but loses $85.17 relative to it after the event exclusion.
Tuning is useful for some weaker specifications, but does not remove concentration
or produce a new primary-policy winner.

At top five, the three fold returns for tuned XGBoost classification are
$4,499.38, $1,110.63 and $331.93. The original forest returns $4,500.13,
$1,110.64 and $331.93. Their similarity persists at the fold level, rather than
arising from large offsetting total-return differences.

### Budget sensitivity: top three

**Tuned histogram classification earns the highest top-three P&L, $6,009.60.**
This is $99.17 above the original forest and $1,629.64 above quoted-gap. However,
it earns $68.88 less than the forest on completed baskets and incurs $168.05 less
partial-execution loss. It avoids the $131.28 December Fed loss on 19 November
and the $36.70 boxing loss on 15 December that the forest takes. This does not
show that it explicitly predicted those execution failures: its objective is
opportunity occurrence, and the avoided trades arise from hourly selection.

Monitor-all earns $5,951.60. A selective policy can exceed it by avoiding losing
attempts; monitor-all is a reference policy, not a hindsight-optimal upper bound.
The tuned histogram model still incurs $245.93 in partial losses and has a
$142.08 maximum drawdown. Its leading net return is not evidence that execution
risk has been eliminated.

The December Fed event contributes **$5,309.60**, or **88.35%**, of this model's
full-pool top-three net return. After excluding it and reallocating slots, the
model earns $707.89 versus $729.50 for quoted-gap. Its $805.77 top-three improvement
over the original histogram classifier falls to only $0.23 in that exclusion
experiment. Tuned forest regression exceeds quoted-gap outside Fed by only
$0.06 ($729.56 versus $729.50), which is not a practically persuasive advantage.

Logistic tuning is also budget-dependent: it raises top-three P&L from $3,749.18
to $5,881.25, while reducing top-five P&L by $27.86. Its top-three gain likewise
fails to survive the Fed exclusion. Tuned XGBoost classification improves its
fixed comparator at top five by $8.39, but loses $770.99 at top three. A top-five
selection objective does not directly reward the ordering of the first three
positions. None of these settings was retuned using the top-three results.

### Search choices and uncertainty

Logistic selection chooses L2 regularisation with C = 0.01 in the first two
folds, and C = 1 in the last; no tested L1 or unweighted option is selected.
The forest classifier retains its original configuration in the first and last
folds, but selects depth 12, minimum leaf size 5 and all features in the middle
fold. XGBoost classification chooses the same configuration in all three folds:
200 trees, depth 4, learning rate 0.05, minimum child weight 20, 80% row and column
sampling, L1 = 1 and L2 = 10. The two-stage XGBoost search always selects its fixed
candidate and therefore changes neither predictions nor returns.

The paired bootstrap uses only **nine event families**. The tuned histogram
classifier's top-three difference from quoted-gap is $1,629.64, with an exploratory
95% interval of **-$82.09 to $4,933.48**. At top five, tuned XGBoost's difference
is -$103.31, with an interval of **-$324.18 to $12.95**. Both include zero, and
neither interval is corrected for inspecting multiple models. They do not support
claims of established market-wide superiority.

### Implication for the dissertation

The extension strengthens the model comparison without establishing that a newer
algorithm improves the primary policy. It demonstrates that regularisation can
matter, prediction objective and monitoring budget interact, and event composition
can outweigh algorithm choice. Direct P&L training and a two-stage architecture
are not automatically superior to occurrence classification on this sample.

Retain quoted-gap as the strongest primary development policy and the original
forest as a strong learned comparator. Report tuned histogram classification as
the leading top-three sensitivity, with its concentration and loss-avoidance
explanation. There is no compelling development evidence to replace the forest
with XGBoost. Further model searching would increase selection risk without
addressing the small number of independent active and losing examples. Freeze
any final model-selection rule before accessing the final test.


## Feature and robustness analysis

At top five, tuned XGBoost classification captures **359 of 376** known hours
containing a profitable attempt; the original forest captures **360**. Both
capture **226 of 242** hours with a profitable opportunity whose onset is
observed after screening. Quoted-gap captures 345 and 211, respectively.
These are occurrence counts, not counts of net-profitable hours or dollar returns.

The group permutation results for three tuned classifiers are:

| Model | Quoted-gap group | Snapshot history | Metadata | Freshness/quality |
| --- | ---: | ---: | ---: | ---: |
| XGBoost | $3,279.64 | $1,202.78 | $54.93 | $49.20 |
| Random forest | $1,849.41 | $1,127.80 | $138.96 | $107.66 |
| Logistic regression | $490.65 | $204.90 | $126.72 | -$27.64 |

Entries are decreases in **selected standalone target P&L**, averaged over three
permutations per fold and summed over folds. They are not actual replay losses.
The largest dependence is on the quoted state and its recent history. This is
consistent with the models learning refinements of a strong quote-based signal,
rather than demonstrating a wholly independent source of predictive information.
Metadata contributes less in this diagnostic; its positive contribution does not
establish generalisation to unseen topics. The negative logistic freshness result
means that shuffling that group improved this proxy on average, not that old books
are safe to execute against.

Within each outer evaluation scan, each feature group is shuffled between events
three times. The model remains fixed. A positive drop in selected standalone P&L
indicates reliance on that information; a negative drop means the perturbation
improved the proxy. This is not a causal effect or a replayed P&L difference.
History and quoted-gap groups overlap, and correlated features can substitute for
one another. Groups are not additive contributions.

Logistic coefficient tables retain the fitted feature names, including missingness
indicators and category dummies. Numeric coefficients act on standardised
signed-log inputs rather than raw prices or sizes. Their signs describe the
fitted association conditional on other predictors, not a causal effect. Different
training distributions also change the scaling between folds.

The December Fed sensitivity removes that event from the candidate pool, then
reranks and replays the same predictions. It does **not** retrain without Fed
events and therefore does not establish transfer to unseen families. The original
study's separate family-holdout analysis remains relevant.

Paired family bootstrap intervals resample actual policy-return contributions
from event families. They are exploratory, conditional on the recorded decisions,
and unadjusted for multiple model comparisons. They do not rerun training or
simulate new market trajectories. Few independent events and losses limit inference.

## Reproduction

Validation completed with **124 passing tests**, all seven notebook code cells
executed without errors, and successful fresh-process loading of the saved
two-stage models. All 16 shared baseline policies (including monitor-all)
reproduce their previous replay summaries at both budgets. Training and analysis
artifact hashes, inner split separation and recorded candidate selection were
verified. No fitting warnings were recorded across the completed study.

Run [notebook 05](../../notebooks/05_model_tuning.ipynb). Its default verifies and
displays saved outputs. Training and replay controls are opt-in and operate in a
separate run directory without changing the original benchmark pointers.

```powershell
.\.venv\Scripts\python.exe -m src.models.tuning --output results/runs/tuning_new
```

The notebook calls `replay_profitability` at budgets five and three, followed by
`src.reporting.tuning.build_report`. The pinned additional dependency is
`xgboost==3.2.0`. Fitted pipelines, software versions, source and input hashes,
all candidate scores, selected settings and fit warnings are saved.

Canonical outputs are in `results/profitability/tuning_study/`:

- `design.json`, `complete.json`: protocol, provenance and training artifact hashes.
- `search.csv`, `selected_parameters.csv`, `fit_audit.csv`, `warnings.csv`: tuning audit.
- `predictions.parquet`, `metrics.csv`, `fitted_models/`: outer predictions and models.
- `feature_importance.csv`, `gate_predictions.parquet`, `gate_calibration.csv`: diagnostics.
- `replay_top5/`, `replay_top3/`: actual policy attempts, summaries and paired comparisons.
- `analysis/`: matched model changes, fold/event breakdowns, figures and output hashes.

The methods follow the separation of tuning and evaluation described in
[scikit-learn's nested validation guide](https://scikit-learn.org/stable/auto_examples/model_selection/plot_nested_cross_validation_iris.html),
using chronological blocks rather than its illustrative random folds.
Parameter meanings are documented in the
[XGBoost parameter reference](https://xgboost.readthedocs.io/en/stable/parameter.html).
