# Predicting arbitrage opportunities in event markets

This dissertation tests whether scheduled order-book snapshots can identify which
Polymarket events are worth monitoring for executable pricing inconsistencies.
Every **W minutes**, rank all eligible events, select **N**, and monitor their
updates until the next scan. The planned final evaluation uses **W = 60 minutes,
N = 3 as primary and N = 5 as secondary**. Completed development runs used N=5
for tuning and reported both budgets; their saved designs and results are unchanged.
The research separates **opportunity prediction** from **economic evaluation**.
The first layer predicts whether a depth-qualified quoted opportunity will appear
and how large its displayed dollar margin could be. The second evaluates the
same event selections under execution assumptions such as latency and partial
fills. Execution losses do not define the first layer's learning targets.
Event outcomes themselves are not predicted.

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
**5,008 unknown outcomes**. Observation-time labels comprise **454 positive**
and **21,373 negative** event-hours. These describe displayed opportunities,
not successfully executed trades.

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

A **candidate** passes the observation-time checks, with at least five shares
or the exchange minimum if higher. There is no minimum persistence requirement.
The **occurrence target** is whether any candidate exists in the following
window, including at the scan. The **value target** is the largest qualifying
displayed dollar margin of one basket at any observed state in that window.
Repeated snapshots and overlapping ladder pairs are not added together.
This peak-value target measures opportunity magnitude, not total hourly profit.
Both targets require complete-window observation coverage; otherwise they remain
unknown. Newly appearing opportunities are also evaluated separately.

## Models and evaluation

- Opportunity classifiers: logistic regression, random forest, histogram boosted
  trees and XGBoost, with fixed and chronologically tuned settings.
- Quoted-value regressors: random forest, histogram boosted trees and XGBoost.
- Two-stage value models: probability of an opportunity multiplied by its
  expected peak quoted value conditional on an opportunity. Gates use natural
  class prevalence; there is no hard probability threshold.
- Classifier ensembles average within-scan percentile ranks; value ensembles
  average predicted dollars.
- Quoted-gap, displayed-depth and quote-age rules, plus exact expected random
  capture under the same monitoring budget.
- **TabM (ICLR 2025):** the authors' parameter-efficient neural ensemble, tested
  for occurrence, direct quoted value and two-stage value prediction. The separate
  extension includes eight-setting chronological tuning, three seeds, a book-only
  comparison and a TabM/forest rank ensemble. See [the TabM study](docs/reports/tabm.md).

Earlier P&L-target regressions, two-stage activity/P&L models and persistent
policy replays remain separate comparators. They have not been relabelled as
execution-independent opportunity models.

Occurrence classifiers compare **33 book features** and **book plus metadata**;
direct and two-stage value models use the combined features.
Metadata adds event type, broad topic, and age
since the earliest constituent market was created. Creation timestamps are
treated as stable facts; generic end dates are excluded because their historical
validity is uncertain. No intrawindow updates, event identifiers, final outcomes
or future volume enter predictors.
Training expands across three chronological folds. Every training label window
must finish strictly before evaluation starts. Preprocessing uses training rows
only. Eight settings per method are compared on two inner chronological blocks:
classifiers tune positive-hour capture; value models tune captured peak quote
value. Outer evaluation and the final test do not choose those settings.
TabM additionally uses its inner validation blocks for early stopping; full outer
training histories are refitted for the median selected epoch count. Its numeric
quantile transformation and target scaling are fitted on training rows only.

The first layer reports **opportunity recall and quoted-value capture at N=3 and
N=5**. N=3 is now the primary budget for the planned final evaluation. This choice
was made after inspecting development results, where the smaller budget imposed
a more selective monitoring constraint; it was not prespecified before those
results. Existing settings were tuned at N=5. Any N=3-specific retuning must use
development data only and be saved as a separate study before final-test access.
The final-test fitting and evaluation runner has not yet been implemented.
Value capture weights each known
event-hour by its peak quoted dollar margin; its summed denominator is not
tradable profit. Precision, average precision, new-onset capture, feature-group
permutation, paired family resampling and removal of the December Fed event
provide additional diagnostics. Unknown labels consume monitoring slots but
never become zero-value training examples. These are exploratory development
results, not evidence of market-wide generalisation or a final winning model.

## Execution assumptions and constraints

Opportunity labels use **no arrival delay or unwind outcome**, with **no artificial
cash cap or share limit**. Candidate discovery and quantity-dependent snapshot
features use full integer depth. The existing economic benchmark uses an assumed
**100 ms arrival**. No additional failure probability is imposed. Orders submit
immediately; quantity and limits are fixed before arrival.
Incomplete baskets are unwound **one second after arrival**. There is **no
quote-age cutoff**, but labels and fills require supported
recording coverage. Carry-forward assumes complete capture of book changes.

The earlier P&L-target training windows begin with an empty liquidity ledger. Actual policy replay
preserves reservations across windows and folds, so selected label sums are
not strategy profits. Policies select up to the specified N available events without
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

Start with **06_opportunity_prediction.ipynb** for the current first layer. It
rebuilds observation/value targets, trains models and displays the audited
comparisons. The earlier notebooks preserve the data and economic studies:

1. **01_dataset.ipynb** inspects raw/aligned data and modelling examples. Set
   `REBUILD = True` to regenerate features and labels for the chosen W.
2. **02_models.ipynb** displays the completed comparisons. Set `RETRAIN = True`
   to fit all feature sets and save a new run.
3. **03_simulator.ipynb** displays P&L and failures and generates the findings
   report. Set `RUN_REPLAY = True` to replay the selected model run again.
4. **04_selection_diagnostics.ipynb** explains policy differences using selected
   event-hours and actual replayed trades. It separates gaps visible at screening
   from later observed onsets and reconstructs the largest forest-only loss.
   Set `RUN_DIAGNOSTICS = True` to create a fresh diagnostic run without retraining.
5. **05_model_tuning.ipynb** verifies and displays the tuning study, matched
   persistent replays at N=5 and N=3, event-concentration checks and feature analysis.
   `RUN_TRAINING` requires a fresh run directory; `RUN_REPLAY` generates missing
   policy replays. It does not replace the primary benchmark pointers.
6. **06_opportunity_prediction.ipynb** compares occurrence and quoted-value
   prediction independently of execution. Rebuilding requires fresh output
   directories; the default displays saved results. It does not execute trades.
7. **07_economic_replay.ipynb** holds those predictions fixed and compares
   0/50/100/250/500ms arrival, N=5/N=3, and December Fed exclusion. It reports
   net P&L, partial losses, drawdown, funding scenarios and conversion of selected
   opportunities into trades. `RUN_REPLAY` requires a fresh output directory.
8. **08_tabm.ipynb** reproduces the contemporary neural baseline comparison,
   seed variation, feature analysis and matched economic replay. Default execution
   verifies saved results. Training and replay require new output directories.
   CPU works; optional CUDA-enabled PyTorch accelerates training. The saved
   models perform screening on CPU and do not download pretrained weights.

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
results/opportunities/ Opportunity occurrence models and recall-based tuning
results/opportunity_value/ Direct/two-stage quoted-value models and comparisons
results/opportunity_replay/ Fixed-ranking execution and latency comparisons
results/tabm/           TabM fits, predictions and comparative analysis
results/tabm_replay/    Fixed TabM rankings under the same execution scenarios
results/profitability/  Earlier P&L-target comparisons and policy executions
results/baseline/       Historical classifier and fixed-ranking execution references
results/runs/           Additional reproducible runs, when created
docs/dataset.md         Dataset schemas, alignment and feature definitions
docs/simulator.md       Trading policy, fill accounting and limitations
docs/reports/           Generated findings, separate from the reader guides
```

Read [the dataset guide](docs/dataset.md), [the simulator guide](docs/simulator.md)
and [the opportunity-prediction findings](docs/reports/opportunity_prediction.md).
The [economic replay findings](docs/reports/opportunity_economic_replay.md)
evaluate those fixed rankings under explicit execution assumptions.
The [TabM findings](docs/reports/tabm.md) add a recent conference baseline to both layers.
The [earlier economic findings](docs/reports/current_results.md) use P&L-dependent targets.
The [selection diagnostic report](docs/reports/selection_diagnostics.md) examines
why quoted-gap leads the development P&L comparison and how fragile that lead is.

## Discovering a new event universe

`src/data/universe_detection.py` classifies a supplied market export into negRisk,
supported ladders and unresolved cases. `contract_templates.py` contains versioned
rule grammars for monthly price reach/dip, launch FDV, sale commitments and
explicit deadlines or fixed-time measurements. No event IDs or realised outcomes
are used to discover relationships. Unknown wording remains in the review output.

NegRisk classification uses metadata; an executable complete basket additionally
requires verified exhaustive membership and a consistent non-augmented contract.
Notebook 01 includes an optional discovery section. See the dataset guide for
the input contract and command. Discovery writes a separate manifest and does not
replace the frozen cohort or its trained models. A changed universe requires a
new feature/label build, chronological training and replay.

`src/data/catalogue.py` can acquire a new public metadata catalogue and run the
detector before requesting FinData books. The proposed March–August 2026 catalogue
reserves August for final evaluation; it is separate from the current Dome
baseline. Discovery produces candidate/token inventories, not coverage or
profitability claims. Commands and limitations are in the dataset guide.
