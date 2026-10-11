# Predicting arbitrage opportunities in event markets

This dissertation tests whether scheduled order-book snapshots can identify which
Polymarket events are worth monitoring for executable pricing inconsistencies.
Every **W minutes**, rank all eligible events, select **N**, and monitor their
updates until the next scan. The default is **W = 60 minutes, N = 5**.
The primary objective is to rank events by **expected simulated net P&L over the
next window**. Opportunity occurrence remains a secondary target. Event outcomes
themselves are not predicted.

## Data and scope

Historical token order books were collected through the Dome API; frozen
Polymarket metadata supplies market descriptions, outcome tokens and fee flags.
Dome's API is retired, so the workflow uses the saved files and makes no network
requests. Raw data is excluded from Git and must be supplied separately under
`data/` or an equivalent `DATA_ROOT` directory.

The original cohort contains 50 negRisk and 30 ladder events. Membership,
contract-rule and historical-coverage checks leave **43 negRisk and 5 ladder
events** with development ranking examples. Not every event is active at every
scan. Other original event types have no implemented arbitrage relationship.
The dataset has **26,835 eligible event-windows**, **21,827 usable labels** and
**5,008 unknown outcomes**. Known hourly net returns comprise **437 positive**,
**21,373 zero** and **17 negative** observations under the declared simulation.

Development uses **14 October 2025 to 7 January 2026 (UTC)**. Data from
**8 January 2026 onward remains an unopened final test**; active loaders reject
final-test analysis. The original universe was selected retrospectively using
resolved-market and volume information, which limits claims about live markets.

## What counts as an opportunity?

The implemented strategies buy equal quantities of a basket with a minimum
contractual payout:

| Event type | Purchases | Minimum payout per set |
| --- | --- | --- |
| negRisk | YES for every exhaustive, mutually exclusive outcome | $1 |
| Ladder | YES for a broader condition A and NO for a narrower condition B, where B implies A | \$1; some resolutions pay \$2 |

For example, a complete YES basket costing \$0.97 has a \$0.03 gross margin per
set. Qualification uses full displayed depth, minimum order sizes and price ticks,
not merely the sum of best prices. Both expected purchase cost and worst allowed
fill cost must leave a strictly positive margin. The relationships depend on the
frozen rules and membership assumptions; incomplete outcome sets are excluded.
Selling, short positions and complementary conversion routes are outside the
implemented strategy set.

A **candidate** passes the decision-time checks. The primary **P&L target** sums
all simulated returns from attempts initiated in the following window, including
losses. A secondary **occurrence label** records whether at least one attempt was
profitable. An hour can have a positive occurrence label and a negative net return.

## Models and evaluation

- Opportunity classifiers: logistic regression, random forest and boosted trees.
- Direct P&L regression: ridge regression, random forest and boosted trees.
- Two-stage models: probability of any attempted trade multiplied by conditional
  net P&L. Conditional fitting includes losing and zero-return trading hours.
- Classifier ensembles average percentile ranks; economic ensembles average
  predicted dollars. Activity gates use unweighted classes and report probability reliability.
- Quoted-gap, displayed-depth and quote-age rules; 20 fixed-seed random policies.
- Monitor-all as the economic reference, without the top-N restriction.

Each model is compared using **33 book features** and **book plus metadata**.
Metadata adds event type, broad topic, and age
since the earliest constituent market was created. Creation timestamps are
treated as stable facts; generic end dates are excluded because their historical
validity is uncertain. No intrawindow updates, event identifiers, final outcomes
or future volume enter predictors.
Training expands across three chronological folds, with a full label horizon
plus 1.1 seconds purged before evaluation. Preprocessing uses training data only.
Feature ablations, within-scan permutation importance and held-out topic families
assess what the models learn. Settings are fixed; there is no hyperparameter search.

The primary metric is **net P&L from chronological replay at N=5**. Secondary
metrics include partial-basket losses, drawdown, capital exposure, positive-window
capture, precision, average precision and dollar prediction error. Ranking
diagnostics also use N=1, 3 and 10. Unknown outcomes consume ranking slots but
never become zero-return training examples. Family holdouts, paired family
resampling and a reranked exclusion of the dominant December Fed event assess
concentration and transfer. No method is assumed to beat the quoted-gap rule.

## Execution assumptions and constraints

Labels, models and policy replay use an assumed **100 ms arrival**, with **no
artificial cash cap or share limit**. Candidate discovery and quantity-dependent
snapshot features use full integer depth. No additional failure probability is
imposed. Orders submit immediately; quantity and limits are fixed before arrival.
Incomplete baskets are unwound **one second after arrival**. There is **no
quote-age cutoff**, but labels and fills require supported
recording coverage. Carry-forward assumes complete capture of book changes.

Training windows begin with an empty liquidity ledger. Actual policy replay
preserves reservations across windows and folds, so selected label sums are
not strategy profits. All policies select up to five available events without
a learned abstention threshold; actual entry still requires a qualified candidate.
For the events used in the study, all of them have their fees waived.

Displayed-depth fills are conditional assumptions. Information-delivery latency,
competition, queue position, a shared portfolio capital budget, settlement cash
release and funding/operating costs are not modelled. Results therefore describe
**simulated P&L**, not demonstrated commercial profitability.

## Run the study

Use Python 3.12 and a local virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m ipykernel install --prefix .venv --name research --display-name "Prediction-market research"
.\.venv\Scripts\python.exe -m jupyterlab notebooks
```

Open the notebooks in order, using the research kernel:

1. **01_dataset.ipynb** inspects raw/aligned data and modelling examples. Set
   `REBUILD = True` to regenerate features and labels for the chosen W.
2. **02_models.ipynb** displays the completed comparisons. Set `RETRAIN = True`
   to fit all feature sets and save a new run.
3. **03_simulator.ipynb** displays P&L and failures and generates the findings
   report. Set `RUN_REPLAY = True` to replay the selected model run again.

Rebuilding data may take tens of minutes; fitting and replay are shorter.
Successful runs are selected through small path manifests. Repeated runs create
new output folders instead of overwriting completed results. A change to W
requires matching features, labels, models and replay settings.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```

## Repository structure

```text
notebooks/              Executable dataset, model and simulator analyses
src/
  config.py, paths.py    Experiment contract and portable locations
  data/                 Raw readers, contract rules, alignment, features, labels
  models/               Estimators, chronological evaluation, comparison training
  simulator/            Discovery, sizing, execution, scheduling, policy replay
  reporting/            Tables, figures, result audits and run management
tests/                  Causality, coverage, sizing, execution and model checks
data/                   Local raw inputs, frozen metadata and prepared datasets
results/profitability/  Current targets' model comparisons and policy executions
results/baseline/       Historical classifier and fixed-ranking execution references
results/runs/           Additional reproducible runs, when created
docs/dataset.md         Dataset schemas, alignment and feature definitions
docs/simulator.md       Trading policy, fill accounting and limitations
docs/reports/           Generated findings, separate from the reader guides
```

Read [the dataset guide](docs/dataset.md), [the simulator guide](docs/simulator.md)
and [the current findings](docs/reports/current_results.md).
