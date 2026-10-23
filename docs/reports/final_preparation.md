# N=3 retuning and final-model preparation

## Scope and protocol

N=3 is the primary monitoring budget and N=5 is secondary. This choice followed
inspection of earlier development results, where a tighter monitoring budget
made selection differences more visible. It is not presented as a budget chosen
before development analysis. The existing N=5-tuned artifacts remain unchanged.

This study separates two operations:

1. **Development evaluation:** repeat the three expanding chronological folds,
   selecting settings specifically for N=3 using only earlier inner validation.
2. **Final fitting:** select settings afresh within the full development history,
   then fit the final models on all permitted known examples. These fits are not
   used to calculate development accuracy or P&L.

The final test from **8 January 2026 onward remains unopened**. No labels,
predictions or returns from that test influence settings, feature choices, epoch
counts, seeds or model selection. Final-test coverage, its endpoint and the
evaluation runner remain separate work.

## Models and objectives

| Method | Fitting target | Features | Setting selection |
| --- | --- | --- | --- |
| Logistic regression | Binary opportunity occurrence | 33 book/history columns | Recall@3, then mean average precision |
| Random forest | Binary opportunity occurrence | 33 book/history columns | Recall@3, then mean average precision |
| Two-stage TabM | Unweighted occurrence gate and positive-only peak quote amount | Book/history plus type, topic and age | Captured peak quote dollars@3, then positive-hour capture |

The forest is an occurrence classifier, even when its selected events capture
large quoted values. The TabM amount target is the largest qualifying displayed
margin of one basket at one state within an hour; it is not total collectible
profit. Execution losses do not enter any fitting or tuning target.

Each method uses the same eight candidate settings as the earlier studies.
Classical tuning now explicitly uses the book-only view; older tuned classical
models used combined features. Comparison with fixed book controls therefore
measures the new tuning procedure, not an isolated change in N alone.
TabM keeps the earlier combined-feature architecture and preprocessing.

Each origin has two chronological inner validation blocks, with every training
label window ending strictly before validation begins. Preprocessing fits only
the corresponding training rows. Unknown labels are excluded from fitting but
stay in the ranked pool. Parameter ties resolve by declaration order.

TabM uses 32 implicit members, inner log-loss/MSE early stopping with patience 16
and a maximum of 100 epochs, and full-history refits for the rounded-up median
best inner epoch. Gate and amount epoch counts are selected separately. Three
seeds are retained and predictions average their **products**, not the product
of the mean gate and mean amount. No seed is selected using outer results.

Fixed logistic and forest controls are refitted alongside tuned versions and
retained regardless of the new results. Their development predictions must
reproduce the earlier saved controls. The previous N=5-tuned TabM policy remains
a development comparator. The final bundle contains five fitted policies plus
the quoted-gap rule; random and monitor-all remain planned final references.

## Final fitting boundary

There are **21,827 known development examples**, including **454 positives**.
All qualify for the final fitting cutoff: their latest scan is
**7 January 2026, 22:00 UTC**, and the latest one-hour label ends at 23:00 UTC,
strictly before the final test starts. The conditional TabM amount models use
only the 454 positives; gates and classical models use all 21,827 known rows.

The final configuration is not copied blindly from the last development fold.
An additional tuning origin at 8 January repeats the same earlier-only validation
procedure with the expanded history. This allows settings to change without
looking at test data. No best-family claim is inferred from these in-sample fits.

## Economic comparison

Only the earlier-trained outer-fold predictions are replayed. The primary case
is **100 ms arrival, uncapped capital and shares**, with no imposed random failure
rate. The same immediate entry checks, per-leg fill-or-kill logic, one-second
unwind/hold and persistent liquidity reservations are reused. The three retuned
policies are evaluated at five latencies, two budgets and two event scopes:
**60 policy/scenario combinations**.

The full population and December Fed exclusion are reported. Existing classical
and TabM replay artifacts are verified and reused, not silently rerun with
different assumptions. Completed margins are booked at completion, not actual
settlement cash receipt. Funding scenarios are not measured ROI denominators.

## Development findings

The shared outer evaluation contains **385 positive event-hours** across the
three development folds. These rows were scored by models fitted on earlier
history. At the primary budget of N=3:

| Policy | Positive hours captured | Opportunity recall | Peak quoted-value capture |
| --- | ---: | ---: | ---: |
| Quoted-gap rule | 273 | 70.91% | 60.08% |
| Fixed book logistic | 342 | **88.83%** | 98.07% |
| Fixed book forest | 339 | 88.05% | 98.45% |
| Logistic retuned at N=3 | 279 | 72.47% | 56.35% |
| Forest retuned at N=3 | 330 | 85.71% | 78.50% |
| TabM two-stage retuned at N=3 | 334 | 86.75% | 99.2096% |
| Earlier TabM two-stage tuned at N=5 | 336 | 87.27% | **99.2116%** |

**Retuning does not reliably improve performance.** The fixed logistic model
captures the most positive hours in this comparison. TabM captures the greatest
quoted value, but changing its tuning budget has almost no effect on that
measure and loses two positive hours. These are different objectives, so there
is no single winner across all metrics.

The classical deterioration is concentrated in the first fold. Retuned logistic
captures 84 of 165 positive hours there, compared with 148 for its fixed control;
retuned forest captures 125 compared with 134. The first outer training set has
only 69 positives, and its inner training sets are smaller still. This pattern
is consistent with unstable parameter selection under sparse, changing event
coverage; it does not establish that tuning is intrinsically harmful. Keeping
the fixed controls exposes this weakness instead of reporting only favourable
tuning results. The forest selects its fixed configuration again in both later
folds and in the final refit.

Rescoring the same inner predictions at N=5 changes the chosen configuration
in **7 of 12 method/origin comparisons**: three for logistic, three for forest
and one for TabM. This diagnostic isolates the budget's effect on setting
selection without another training run. Both budgets choose the same early-fold
classical settings, so that fold's deterioration cannot be attributed simply
to changing N from five to three. Final TabM settings are also unchanged by
which of these two budgets scores the inner predictions.

### Economic results and concentration

The following development P&L uses **100 ms arrival and uncapped capital/shares**.
The exclusion columns rerank the remaining events after removing the December
Fed event; they are not simple subtraction from the full-population totals.

| Policy | N=3, all events | N=5, all events | N=3, December Fed excluded |
| --- | ---: | ---: | ---: |
| Quoted-gap rule | $4,379.96 | **$6,045.25** | **$729.50** |
| Fixed book logistic | **$5,999.41** | $5,903.33 | $673.07 |
| Fixed book forest | $5,845.60 | $5,939.12 | $694.89 |
| Logistic retuned at N=3 | $3,813.35 | $5,899.94 | $679.85 |
| Forest retuned at N=3 | $5,164.50 | $5,920.98 | $703.52 |
| TabM two-stage retuned at N=3 | $5,905.81 | $5,894.06 | $663.70 |
| Earlier TabM two-stage tuned at N=5 | $5,904.15 | $5,897.46 | $666.96 |

The retuned TabM's N=3 result consists of **$6,320.06 completed-basket margin**
and **-$414.25 partial-fill P&L**. It attempts 450 baskets, completes 433 and has
no unknown execution outcomes in this replay. Its value-capture advantage does
not imply the highest net P&L: execution can change the ordering.

The December Fed event contributes **$5,222.45**, or **88.4%**, of this TabM
policy's full-population net P&L. After exclusion, TabM captures 97.73% of peak
quoted value versus the fixed forest's 97.57%, a much smaller difference than
the full-population headline. The quoted-gap rule has higher simulated P&L than
every fitted policy in the N=3 exclusion comparison.

Family-level paired resampling over nine families gives a 95% interval of
approximately **-$134 to +$4,696** for retuned TabM's N=3 net-P&L difference
against quoted-gap, despite the observed +$1,526 difference. These exploratory
intervals do not correct for repeated development inspection or the post hoc
choice of N. They do not establish a reliable market-wide profitability edge.

Holding its selections fixed, retuned TabM's N=3 net P&L is **$6,502.99 at
0 ms, $6,381.33 at 50 ms, $5,905.81 at 100 ms, $5,165.46 at 250 ms and
$4,930.60 at 500 ms**. The primary run's drawdown is $206.20. Its no-settlement
funding scenario reaches about $1.42 million; uncapped sizing must not be
interpreted as a small-capital return. These remain conditional displayed-depth
simulations with the limitations stated above.

The numerical comparisons support retaining **fixed logistic for occurrence
capture and two-stage TabM for quoted-value capture** as distinct research
comparators. They do not support replacing the fixed baselines automatically
with tuned versions or declaring TabM universally superior. All prepared
policies remain saved; the final test has not selected a winner.

Full figures and tables are in
[`results/final_preparation/analysis/`](../../results/final_preparation/analysis/),
including all latency scenarios, fold breakdowns, family intervals and event
contributions.

## Final saved configurations

The last tuning origin selects logistic regression with L2 regularisation,
`C=0.1`, no class weighting and the `lbfgs` solver. The forest uses 200 trees,
maximum depth 10, minimum leaf size 20 and square-root feature subsampling.
Its tuned and fixed final policies therefore reference the **same fitted
forest**, although their earlier fold predictions differed.

Final TabM uses one block of width 64, zero dropout, learning rate 0.002 and
weight decay 0.0003. Each of its three seeds fits the gate for 50 epochs and the
conditional amount model for four epochs, as determined by earlier inner
validation. These differ from the last development fold's settings because the
final tuning origin has additional permitted history. They are saved for later
evaluation; no performance claim is made from fitting them on all development
data.

## Reproduction and use

[Notebook 09](../../notebooks/09_final_preparation.ipynb) displays verified saved
results by default. To rebuild, use fresh model and replay directories:

```powershell
.\.venv\Scripts\python.exe -m src.models.final_preparation --output results/runs/n3_new --device cuda
.\.venv\Scripts\python.exe -m src.simulator.final_preparation --models results/runs/n3_new --output results/runs/n3_new_replay
$env:MPLBACKEND = 'Agg'
.\.venv\Scripts\python.exe -m src.reporting.final_preparation --models results/runs/n3_new --replay results/runs/n3_new_replay
```

CPU training is available with `--device cpu`. Saved models use CPU inference.
Keep the complete run folder together: `final_refit_bundle.json` references the
weights under `final_models/`, and `complete.json` verifies their hashes.

The portable inference entry point is:

```python
from src.models.final_preparation import predict_final
selected = predict_final(run_folder, supplied_features, top_n=3)
```

Supply one row per eligible event/scan, UTC timestamps, and the same as-of feature
columns produced by the research pipeline. Metadata must already be supplied;
this helper does not query the local universe or any data provider. It requires
snapshots at or after the bundle's fitting cutoff and returns selections for all
fitted policies and the quoted-gap rule. It does not load final-test data or
execute trades. A different data source still needs the validated alignment,
feature construction and execution components.

`src/models/final_preparation.py` owns tuning, bundles and inference;
`src/simulator/final_preparation.py` applies the unchanged scheduler;
`src/reporting/final_preparation.py` verifies inputs, reconstructs choices and
produces development comparisons. Results are in `results/final_preparation/`
and `results/final_preparation_replay/`.

Artifact audits reconstruct N=3 capture from saved inner predictions, verify
selected configurations and fitted feature schemas, confirm final training
populations and epoch counts, and reproduce saved outer predictions. The final
serialization probe is explicitly not a performance evaluation.

Validation: **154 unit tests passed**. Model, data and replay hashes were
verified; saved inner predictions reproduce N=3 setting selection. All 18
outer bundle/policy prediction arrays reproduce their saved values (maximum
absolute difference below 4e-16), and cumulative P&L reconciles with summary
totals. The replay contains 60 policy/scenario combinations, with separate
full-period and fold summaries. Notebook 09 executes against these saved
artifacts without opening the final test.
