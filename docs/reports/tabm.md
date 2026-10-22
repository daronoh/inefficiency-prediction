# TabM on prediction-market opportunity screening

## Purpose and source

This extension tests whether a recent tabular neural method improves event
screening over the quoted-gap rule and the existing classical models. It uses
**TabM: Advancing Tabular Deep Learning with Parameter-Efficient Ensembling**,
by Gorishniy, Kotelnikov and Babenko, ICLR 2025. The
[supplied paper](../../papers/ICLR-2025-tabm-advancing-tabular-deep-learning-with-parameter-efficient-ensembling-Paper-Conference.pdf)
is recorded by SHA-256 in the run design. The
[official implementation](https://github.com/yandex-research/tabm) is pinned to
`tabm==0.0.3`, with `torch==2.10.0`.

The main finding is mixed: tuned two-stage TabM achieves the highest N=3
peak-value capture in this development comparison, but it does **not** produce
the highest simulated P&L at 100 ms. The recent neural baseline strengthens the
comparison without establishing a new final winner.

TabM represents multiple MLP members with mostly shared weights. Each member
receives its own training loss. Classification inference averages probabilities,
not logits; regression averages outputs before reversing target normalization.
This study uses the standard TabM architecture with **32 members**, rather than
a generic dense network, TabM-mini, or a pretrained foundation model.

## Data, targets and leakage controls

The frozen Dome labels, features, chronological folds and comparator predictions
are unchanged. Development covers 14 October 2025 through 7 January 2026 UTC.
**The final test from 8 January remains closed.** This is an additional exploratory
development comparison after earlier results, not independent confirmation.

The full development population contains 48 events: 43 negRisk and five ladders.
Outer evaluation contains 20,951 eligible event-hours, of which 16,957 have known
labels and 385 are positive. Unknown labels are excluded from fitting, but remain
in the ranked pool and can consume monitoring slots.

The occurrence target is any qualifying depth-supported basket in the following
hour, including a basket visible at screening. The value target is the **largest
displayed margin of one qualifying basket at one state** within that hour. It is
not total hourly profit. Direct regression fits all known windows; two-stage
prediction multiplies an unweighted occurrence gate by an amount model fitted
only to positive windows. Neither target contains latency, execution losses,
unwind proceeds or simulated returns.

All variants use the same 33 raw book/history features, with event type, topic
and age added for the combined view. Continuous values receive training-median
imputation and a training-fitted normal quantile transform. Binary 0/1 columns
are imputed without scaling. Categories use one-hot encoding and ignore unseen
categories. The standard quantile transform does not add the paper code's noise
to break ties. No additional missingness indicators or numeric embeddings are
introduced. Dollar targets use training-only mean/std normalization, not logs.
Predicted dollar amounts are clipped to zero after averaging and inversion.
The conditional amount model fits its own preprocessing on its positive subset.

## Tuning and combinations

Three expanding outer folds are evaluated: 1-21 November, 22 November-14 December,
and 15 December-7 January. Training label windows must finish strictly before
validation or evaluation starts. Each outer training history supplies two inner
chronological blocks.

Eight configurations, declared before the new outer results, vary depth (1-3),
width (64-256), dropout (0-0.2), learning rate (0.0005-0.002) and weight decay
(0.0003-0.01). Training uses AdamW, memberwise unweighted binary cross entropy or
MSE, batch size 256, gradient norm clipping at 1, and no learning-rate schedule.
Inner validation uses the ensemble's log loss or standardized MSE for early
stopping: patience 16, maximum 100 epochs. Full outer histories are then refitted
for the rounded-up median of the two selected inner epoch counts, without outer
early stopping. A conditional model with no positive inner validation rows would
use a declared 16-epoch fallback.

Occurrence settings are selected by pooled recall@5, breaking ties by average
precision. Direct and two-stage value settings are selected by captured peak
dollars@5, breaking ties by positive-hour capture. Two-stage gate and amount
share architecture settings, but have separate preprocessing and epoch counts.
The gate fit is reused where the configuration and seed are identical.

The fixed controls use candidate zero and one seed; their epoch counts are still
chosen inside the training history. A fixed book-only occurrence model measures
the metadata change. Each tuned method retains three separate seeds and reports
their equally averaged predictions as its primary tuned policy. An additional
hybrid averages the within-scan percentile ranks of tuned TabM occurrence and
the frozen combined-feature random forest. There are no fitted hybrid weights.

This is a bounded application of the paper, **not a reproduction of its full
benchmark**. The paper generally uses 50-100 Optuna trials and more evaluation
seeds; this study uses eight declared configurations and three seeds. The search
matches the existing study's configuration count, not equal compute consumption.
The paper's architecture and memberwise training are preserved, while validation
splits and ranking objectives are adapted to this forecasting task.

## Economic evaluation

Eight declared TabM or hybrid policies undergo the existing persistent replay.
The primary case is **100 ms arrival with uncapped capital and shares**. Other
latencies are 0, 50, 250 and 500 ms. Both N=5 and N=3 are evaluated, with the full
population and December Fed excluded before reranking: 160 policy/scenario
combinations. Rankings are identical across latencies; execution does not retrain
or select TabM settings.

Orders submit immediately at a qualifying quote, with fixed quantities and
limits. Per-leg fill-or-kill execution can produce a partial basket, unwound one
second after arrival. Liquidity reservations persist across hours and folds.
There is no arbitrary failure probability, quote-age cutoff, minimum persistence,
cash cap or share cap. The frozen cohort's zero-fee setting is retained. Existing
classical-policy economics are reused only after verifying the saved source,
inputs and identical execution configuration.

Completed basket margins are accounted at completion rather than cash settlement.
No-settlement funding is a separate cash scenario, not a measured live capital
requirement or an ROI denominator. Competition, source-to-observer delay, queue
position and funding/operating costs are not simulated. CPU screening cost is
measured separately and is not silently added to the assumed trade arrival delay.

## Development findings: opportunity prediction

The extension adds 17 ranking policies, including individual seeds, to the 32
existing policies. There are **150 inner network fits** and **44 unique outer
network artifacts**; identical gates are reused. None of these models is selected
as a final winner using final-test outcomes.

| Policy | Recall@5 | Peak-value capture@5 | Recall@3 | Peak-value capture@3 |
| --- | ---: | ---: | ---: | ---: |
| Quoted-gap rule | 91.69% | 97.89% | 70.91% | 60.08% |
| Book logistic | **97.66%** | 99.43% | **88.83%** | 98.07% |
| Book random forest | 95.32% | **99.86%** | 88.05% | 98.45% |
| TabM occurrence, book-only fixed | 95.84% | 99.67% | 82.86% | 92.55% |
| TabM occurrence, combined fixed | 94.81% | 99.76% | 85.19% | 97.74% |
| TabM occurrence, tuned three-seed | 95.06% | 99.76% | 85.71% | 97.47% |
| TabM direct value, fixed | 79.48% | 95.68% | 69.35% | 93.87% |
| TabM direct value, tuned three-seed | 89.61% | 98.98% | 81.82% | 97.95% |
| TabM two-stage value, fixed | 90.39% | 97.19% | 82.60% | 96.00% |
| TabM two-stage value, tuned three-seed | 95.06% | 99.68% | 87.27% | **99.21%** |
| TabM + forest rank ensemble | 96.36% | 99.84% | 87.79% | 98.15% |

Bold marks the largest value in this displayed comparison, not a final-test win.
The full comparison retains every earlier model; combined-feature forest remains
slightly higher than book forest on N=5 value capture (99.867%).

TabM's clearest improvement is its **two-stage value model at the smaller
monitoring budget**. It captures 336/385 positive hours and 99.21% of peak value
at N=3. Its occurrence model does not displace logistic regression, which captures
342/385 positive hours. The hybrid improves TabM occurrence recall, but does not
beat the strongest classical comparator on either displayed N=3 metric.

These gains are not uniform. At N=5, the tuned TabM occurrence model captures
366/385 positive hours, versus logistic's 376/385. Tuning helps TabM direct value
and two-stage prediction substantially, while occurrence tuning gives little
overall improvement. Adding metadata improves the fixed model's N=3 value
capture but reduces its N=5 positive-hour recall. The fixed controls must therefore
remain visible in the comparison.

For the 233 positive hours with no qualifying basket at the scan, tuned TabM
two-stage captures **80.69% at N=3**, versus quoted-gap's **51.93%** and logistic's
**82.83%**. Its performance is not solely recognition of already visible gaps.

### Concentration and variation

December Fed supplies 82.83% of the summed peak-value target in this evaluation.
After removing it before ranking, tuned two-stage TabM captures **97.70%** of
remaining value at N=3, versus book forest's **97.57%**, logistic's **96.42%** and
quoted-gap's **83.94%**. Its advantage over forest becomes only **0.13 percentage
points**. At N=5, the same TabM method captures 98.61% of remaining value, below
quoted-gap's 99.77%. This is a concentration sensitivity check, not training on
new event families.

Across the three separate seeds, N=3 value capture ranges from **94.57-98.61%**
for the occurrence model, **96.58-97.55%** for direct value, and **98.70-99.22%**
for two-stage value. The published tuned policies average predictions from all
three seeds; no best seed is selected after evaluation. Three seeds provide a
limited initialization diagnostic, not a confidence interval over new markets.

Within-scan feature-group permutation gives a different pattern from the earlier
tree models. For the tuned TabM occurrence ensemble, shuffling **depth/size**
reduces mean fold recall@5 by **25.51 percentage points** and value capture by
**23.90 points**. Historical features reduce recall by 7.51 points; quoted-gap
features by 6.62 points. Metadata changes recall by only 0.26 points. This suggests
that the neural classifier relies particularly on depth-related information.
The groups overlap and are correlated; these are model diagnostics, not causal
effects or additive feature contributions.

### Compute and optimization limits

Recorded network-fitting time totals about **446 seconds for inner fits** and
**169 seconds for outer refits** on the available GPU, excluding comparison
metrics, repeated CPU inference, permutation analysis, replay and report work.
Seventeen of 150 inner fits reached the 100-epoch limit. The results therefore
describe this bounded optimization protocol, not fully exhausted TabM tuning.

Warm CPU screening, including preprocessing, takes roughly **14-25 ms per
single network** for the sampled scan sizes, and **27-35 ms per two-stage seed**.
A sampled scan contains 13-17 events.
A three-seed policy must run all three models; the hybrid additionally evaluates
its forest. Timings are local measurements, not guarantees at larger event
universes or on other hardware. They are separate from the 100 ms order-arrival
assumption used by replay.

## Development findings: economic replay

The new replay contains **58,651 counterfactual attempt records** across the
160 scenarios, with no unknown attempt P&L. These are repeated evaluations of
policies/scenarios, not 58,651 unique independently executable opportunities.

| Policy | N=5 P&L, 100 ms | N=3 P&L, 100 ms |
| --- | ---: | ---: |
| Quoted-gap rule | **$6,045.25** | $4,379.96 |
| Book logistic | $5,903.33 | **$5,999.41** |
| Book random forest | $5,939.12 | $5,845.60 |
| TabM occurrence, book-only fixed | $5,892.18 | $5,757.45 |
| TabM occurrence, combined fixed | $5,900.53 | $5,901.25 |
| TabM occurrence, tuned three-seed | $5,902.60 | $5,716.94 |
| TabM direct value, fixed | $5,920.43 | $5,845.93 |
| TabM direct value, tuned three-seed | $5,924.34 | $5,853.13 |
| TabM two-stage value, fixed | $5,915.92 | $5,926.91 |
| TabM two-stage value, tuned three-seed | $5,897.46 | $5,904.15 |
| TabM + forest rank ensemble | $5,917.56 | $5,886.12 |

At N=3, tuned two-stage TabM exceeds quoted-gap by **$1,524.19**, but trails
logistic by **$95.25**. It completes 430 of 447 attempts and has 17 partial
attempts. Completed baskets contribute $6,318.40, while partial baskets lose
$414.25. Logistic earns slightly less on completed baskets ($6,282.37) but loses
only $282.97 on partial baskets. Execution therefore reverses the advantage in
completed-basket margin. Maximum drawdown is $206.15 for this TabM policy,
compared with $142.08 for logistic.

The first-layer target also measures a future peak, while trading acts on the
first qualifying opportunity and cannot wait with hindsight for that peak.
Persistent reservations and repeated liquidity further separate target sums
from collectible returns. Better peak-value prediction is useful evidence,
but it is not a sufficient condition for higher realized simulation returns.

For tuned two-stage TabM at N=3, simulated P&L changes as follows:

| Assumed arrival | 0 ms | 50 ms | 100 ms | 250 ms | 500 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Known P&L | $6,501.33 | $6,379.67 | $5,904.15 | $5,163.80 | $4,928.94 |

Its December Fed contribution is $5,222.45, about **88.45%** of the 100 ms total.
Excluding that event and reranking gives **$666.96**, versus quoted-gap's
**$729.50** and logistic's **$673.07** at N=3. The full-population gain over
quoted-gap should not be presented as a broad-market profit advantage.

Paired resampling of nine event families gives an exploratory 95% interval of
approximately **-$139 to +$4,696** for the tuned two-stage N=3 P&L difference
against quoted-gap. The corresponding N=3 peak-value-capture interval is
approximately **+1.51 to +40.05 percentage points**. Those intervals answer
different questions; neither substitutes for final testing. There are few
families, dependent event-hours and many comparisons without multiplicity
correction.

The tuned two-stage N=3 no-settlement funding scenario is approximately **$1.41
million**. Uncapped execution is therefore not a low-capital return claim. The
model is worth retaining as a modern comparator, alongside logistic, forest and
the quoted-gap rule. These development results do not justify selecting TabM
solely because its architecture is newer or its N=3 quoted-value metric is higher.

## Reproduction and artifacts

[Notebook 08](../../notebooks/08_tabm.ipynb) verifies and displays saved results by
default. Training and replay require fresh output directories. CPU training is
supported; this run uses an RTX 3060 Ti through PyTorch's CUDA 12.8 build. Saved
models are moved to CPU for portable inference. No pretrained weights are fetched.

```powershell
.\.venv\Scripts\python.exe -m src.models.tabm_study --output results/runs/tabm_new --device cuda
.\.venv\Scripts\python.exe -m src.simulator.tabm --models results/runs/tabm_new --output results/runs/tabm_new_replay
$env:MPLBACKEND = 'Agg'
.\.venv\Scripts\python.exe -m src.reporting.tabm --models results/runs/tabm_new --replay results/runs/tabm_new_replay
```

Use `--device cpu` without CUDA. The optional GPU installation is
`python -m pip install torch==2.10.0 --index-url https://download.pytorch.org/whl/cu128`
inside the project environment, subject to compatible hardware/drivers.

| Location | Contents |
| --- | --- |
| `src/models/tabm.py` | Official architecture adapter, losses, preprocessing and portable inference |
| `src/models/tabm_study.py` | Chronological tuning, refits, seeds, hybrid and artifact verification |
| `src/simulator/tabm.py` | New policy selection and unchanged scheduler reuse |
| `src/reporting/tabm.py` | Fitted-target/prediction audits, metrics, intervals and plots |
| `results/tabm/` | Design, search, epoch histories, saved models, predictions, metrics and analysis |
| `results/tabm_replay/` | Selections, attempts, scheduler audit and economic summaries |

Hash verification covers the supplied paper, source files, prepared datasets,
prior predictions and new results. Additional audits check the saved architecture,
target, training population, seeds, epoch selection and reproduced predictions.
Saved predictions reproduce on 32 sampled rows per fold and policy, with a
maximum absolute numerical difference below 0.000001. All 149 repository tests
pass, and the new notebook is executed against the completed saved artifacts.
The historical experiments remain independently reproducible.
