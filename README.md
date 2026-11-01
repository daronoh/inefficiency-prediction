# Predicting arbitrage opportunities in event markets

The final submission is in [`submission/`](submission/README.md), including the
[report PDF](submission/report/dissertation.pdf), the `FYP` package, the research
notebook, and saved study results. The root-level workflow below retains the
development workspace; it is separate from the standalone submission.

Git excludes `submission/data/`, fresh `submission/runs/` outputs and the large
`submission.zip` distribution archive. These remain available locally. To
reproduce the submission from a clone, restore the complete `submission/data/`
directory from the separately supplied submission bundle before running its
notebook or command-line workflow. The cached workflow needs the prepared data;
a full reconstruction also needs the raw token books. Frozen metadata and the
[raw-file inventory](submission/inputs/raw_inventory.csv) remain versioned, and
[the submission manifest](submission/MANIFEST.sha256) records the distribution's
file hashes. The ZIP checksum is in `submission.zip.sha256`; the archive itself
must be shared separately. The retired Dome API cannot be used to download the
missing raw files again.

This dissertation tests whether scheduled order-book snapshots can identify which
Polymarket events are worth monitoring for executable pricing inconsistencies.
Every **W minutes**, rank all eligible events, select **N**, and monitor their
updates until the next scan. The completed final evaluation uses **W = 60 minutes,
N = 3 as primary and N = 5 as secondary**. Earlier development runs used N=5
for tuning and reported both budgets; their saved designs and results are unchanged.
Notebook 09 adds separate N=3 retuning and full-development final-model preparation.
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
requests. A separate, opt-in PMXT data-source pilot is described below. Raw data is excluded from Git and must be supplied separately under
`data/` or an equivalent `DATA_ROOT` directory.

The original cohort contains 50 negRisk and 30 ladder events. Membership,
contract-rule and historical-coverage checks leave **43 negRisk and 5 ladder
events** with development ranking examples. Not every event is active at every
scan. Other original event types have no implemented arbitrage relationship.
The dataset has **26,835 eligible event-windows**, **21,827 usable labels** and
**5,008 unknown outcomes**. Observation-time labels comprise **454 positive**
and **21,373 negative** event-hours. These describe displayed opportunities,
not successfully executed trades.

Development uses **14 October 2025 to 7 January 2026 (UTC)**.
The final test covers **8 January 2026 to 3 February 2026, 08:00 UTC**, with the
endpoint excluded. It has now been evaluated using the previously frozen models
and protocol; it is unavailable for further model tuning under the held-out claim.
Existing development loaders retain their split guards. The original universe was selected retrospectively using
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
N=5**. N=3 is the primary budget for final evaluation. This choice
was made after inspecting development results, where the smaller budget imposed
a more selective monitoring constraint; it was not prespecified before those
results. The separate N=3 preparation retunes book-only logistic/forest occurrence
models and the combined-feature two-stage TabM model using development data only.
It retains the fixed classical controls and refits final models on all permitted
known development windows. These full-development fits are not evaluated on their
own training history. A timestamp-only audit and the
[fixed final-test protocol](docs/reports/final_test_protocol.md) now define the
evaluation interval and comparisons. The separate final-only runner has completed
the evaluation without refitting models.
See [the preparation report](docs/reports/final_preparation.md).
Value capture weights each known
event-hour by its peak quoted dollar margin; its summed denominator is not
tradable profit. Precision, average precision, new-onset capture, feature-group
permutation, paired family resampling and removal of the December Fed event
provide additional diagnostics. Unknown labels consume monitoring slots but
never become zero-value training examples. These are exploratory development
results, not evidence of market-wide generalisation or a final winning model.

The held-out test contains eight negRisk events, 1,625 known labels and only
16 positive event-hours. At N=3, every saved model and quoted-gap captures all
known positives and quoted value, with identical **$32.31 simulated P&L at
100 ms**. N=5 monitors the entire pool. These results show no final-test ML
advantage over quoted-gap and remain concentrated in January Fed.
See [the final findings](docs/reports/final_evaluation.md).

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
9. **09_final_preparation.ipynb** retunes the shortlist at N=3, compares its
   development predictions and replay with the frozen controls, and fits final
   models on all known earlier history. Default execution verifies saved results;
   rebuilding requires fresh paths. It does not open or evaluate the final test.
10. **10_final_coverage.ipynb** verifies timestamp-only final coverage and the
    fixed evaluation protocol. Eight negRisk events have later recordings,
    there are no final-period audited ladders, and N=5 monitors the whole pool.
    It does not calculate final labels, predictions or trading returns.
11. **11_final_evaluation.ipynb** displays the completed fixed-model held-out
    prediction and economic comparisons, random references, event contributions
    and latency sensitivity. Default execution verifies saved results; rebuilding
    requires fresh directories and does not refit models.

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
results/final_preparation/ N=3 development comparison and separate final model bundles
results/final_preparation_replay/ Development-only replay of the N=3-retuned policies
results/final_coverage/ Timestamp-only final-period recording audit
results/final_protocol/ Fixed final comparison and coverage figure
results/final_evaluation/ Frozen final predictions, selections and capture metrics
results/final_replay/   Final economics, concentration checks and readable figures
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
The [final-test protocol](docs/reports/final_test_protocol.md) records choices fixed
before final outcomes were opened. The [final findings](docs/reports/final_evaluation.md)
report the completed evaluation and its coverage limitations.
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

## Weekly expanding-window Dome study

[Notebook 18](notebooks/18_dome_walk_forward.ipynb) compares weekly expanding-history
fits over the full recorded research period. Initial training uses 28 days;
evaluation runs from 11 November 2025 to 3 February 2026 at 08:00 UTC. Settings
retune every four weeks using earlier chronological validation. N=3 is primary
and N=5 secondary. The [report](docs/reports/dome_walk_forward.md) compares
17 policies, feature views, tuning, ensembles and persistent economic replay
at 100 ms with uncapped capital/shares, plus latency sensitivities.

This retrospective study explicitly includes the original later segment without
replacing the completed final experiment. Data is under
`data/processed/walk_forward_v1/`, fitted models and prediction analysis under
`results/walk_forward/`, and economics under `results/walk_forward_replay/`.
Helpers are `src/data/walk_forward.py`, `src/models/walk_forward.py`,
`src/simulator/walk_forward.py` and `src/reporting/walk_forward.py`.

## Separate Dome validation study

[Notebook 17](notebooks/17_dome_validation_horizon.ipynb) fits on the first 14 or
28 calendar days and evaluates longer subsequent development periods. It compares
occurrence models, two-stage forest/TabM, a fixed rank ensemble and simple rules,
with N=3 primary and N=5 secondary. See the
[findings](docs/reports/dome_validation_horizon.md) for common-period comparisons,
uncertainty and event-concentration checks. This exploratory study adds files
under `results/validation_horizon/`; the completed final evaluation is preserved.
Its notebook defaults to verified saved results and can optionally refit.

## Separate PMXT pilot

Further PMXT development is deferred. Dome remains the primary research dataset;
the pilot and its reconstruction diagnostics are retained as reference evidence.

[Notebook 12](notebooks/12_pmxt_pilot.ipynb) audits a separate PMXT v2 data adapter.
Discovery, bounded HTTP range downloads, receive-time book reconstruction and
hourly coverage/quote diagnostics write only to `data/pmxt/` and
`results/pmxt_pilot/`. The Dome cohort, trained models and final evaluation remain
unchanged. The notebook defaults to reading saved results without network access.
This pilot does not train models or estimate execution P&L. See
[the pilot report](docs/reports/pmxt_pilot.md) for its actual replay interval,
coverage findings, conditional fee assumptions and resumable commands.
The subsequent [source-time validation](docs/reports/pmxt_validation.md) and
[notebook 13](notebooks/13_pmxt_source_validation.ipynb) add source-clock
reconstruction, explicit quality criteria and fixed-latency arrival diagnostics.
[Notebook 14](notebooks/14_pmxt_uncertainty.ipynb) reruns the same sample with
side/level uncertainty, economic blockers and separate occurrence/value bounds;
earlier results are preserved.
The [multi-day feasibility study](docs/reports/pmxt_multiday_feasibility.md) and
[notebook 15](notebooks/15_pmxt_multiday_feasibility.ipynb) audit unknown labels
and test the same universe across four predeclared May dates before committing
to a larger PMXT training dataset.
The [continuous initialization diagnostic](docs/reports/pmxt_continuous_initialization.md)
and [notebook 16](notebooks/16_pmxt_continuous_initialization.ipynb) test whether
earlier starting snapshots and all intervening updates resolve the remaining
coverage problem on a fixed small subset.
