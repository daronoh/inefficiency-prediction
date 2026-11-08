# Budgeted event screening for structural arbitrage

This study asks whether hourly order-book features can rank prediction-market
events by the opportunity they will offer during the next hour. Only the top
N events receive continuous monitoring. N=3 is primary; N=5 is secondary.

The learning targets describe **opportunities**, independently of execution.
A separate replay estimates the economic consequences of monitoring each
policy's selected events. The primary replay assumes **100 ms total delay**,
uncapped capital and shares, and zero exchange fees for the frozen cohort.

## Deliverables

| Item | Location |
| --- | --- |
| Dissertation volume, generated Markdown and authoring source | `report/dissertation.pdf`, `report/dissertation.md`, `report/content.md` |
| Python package, including 28 focused tests | `FYP/` |
| Executable research workflow | `research.ipynb` |
| CSV tables, 50 figures and indexes | `artifacts/tables/`, `artifacts/figures/`, `artifacts/*_index.csv` |
| Processed-data extension: four figures, ablations and detailed report tables | `artifacts/extensions/processed_review_v1/` |
| Robustness extension: historical controls, family-withheld fits and balanced evaluation | `artifacts/extensions/robustness_review_v1/` |
| Allocation extension: token-resource caps, earlier-only policy choice and paired cases | `artifacts/extensions/allocation_review_v1/` |
| Focused sensitivities: smoothing, training windows and cached-feature timing | `artifacts/extensions/sensitivity_review_v1/` |
| Saved weekly models and predictions | `artifacts/models/` |
| Persistent economic replay evidence | `artifacts/replay/` |
| Frozen membership, schemas and metadata | `inputs/` |
| Earlier processed examples for the extension | `artifacts/extensions/processed_review_v1/inputs/earlier_examples.parquet` |
| Original raw token books and full prepared examples | Separate local archive required by the original `data/` workflow |

Only the Dome study is included. All paths are relative to this directory;
there is no dependency on a parent repository. Source installation and local
data are separate: copying the package alone does not copy the data/artifacts.

## Review and rebuild without raw books

The processed-data extension preserves the original study's models, predictions,
labels and economic replay. It adds full-period budget curves, event and basket-type
comparisons, paired comparisons against strong controls, and 162 fixed-forest
ablations on the verified earlier subset. The latter use six feature views, three
seeds and nine chronological fits. They are exploratory experiments on inspected
data, not an independent final test.

Edit prose in `report/content.md`; `report/dissertation.md` is regenerated. With
the dependencies installed, rebuild the report from the saved evidence with:

```powershell
python rebuild_report.py
```

The report includes a fictional hourly example and a map of its three
overlapping evaluation periods. `FYP/reporting/readability.py` prepares those
reader aids from explicitly illustrative values and existing saved metrics.
They add no empirical predictions or labels. Detailed numerical preprocessing,
TabM training, validation boundaries and historical estimates are retained in
Appendix D; the main methods chapter explains their roles and safeguards.

This command does not read raw books or fit models. It checks extension evidence,
regenerates presentation tables and compiles the PDF. The build enforces the
supplied 55-page limit on main text, from the introduction through the
conclusion. Covers, front matter, references and appendices are outside that
count. `report/format_check.json` records the main-text count and its boundaries;
the complete PDF can therefore contain more than 55 pages. The original default and full
workflows below additionally require their full prepared dataset and, for raw
reconstruction or replay, the separately retained book archive.

To repeat the processed-data experiment, choose a new output directory so the
supplied evidence is preserved:

```powershell
python FYP/experiments/processed_extension.py --examples artifacts/extensions/processed_review_v1/inputs/earlier_examples.parquet --output artifacts/extensions/recomputed_review
```

The input and source digests, environment versions, feature views, per-fit audits,
predictions and score reconciliation are saved with each run. The exact experiment
source used for the submitted extension is archived as `executed_source.py`.
The earlier input ends on 7 January 2026; new fits do not reconstruct the missing
later feature table or change the original opportunity definitions. Saved full-period
predictions permit analysis through the original study endpoint.

The second extension adds four causal historical-label controls on the full
evaluation period, fits forests and logistic models with each broad subject
family removed from earlier training, and reports event- and family-balanced
capture with paired bootstrap intervals. Historical feedback updates after each
scan and assumes full earlier labels are available; it does not demonstrate that
a three-event monitor could collect those labels. Withheld-family models rank
the whole candidate pool before evaluating that family's capture. Each family
defines a separate model scenario, not one combined deployment policy.

```powershell
python FYP/experiments/robustness_extension.py --output artifacts/extensions/recomputed_robustness
python -m unittest discover -s extension_tests -v
```

The robustness script reads the packaged earlier examples, saved original
predictions and three-seed unrestricted forest predictions. It freezes its
protocol, source and input digests, resumes verified per-fit checkpoints, and
stores all scores, availability audits, family exclusions and metric outputs in
its own directory. The report rebuild verifies the submitted extensions. No
new labels or execution outcomes are constructed in these extensions.

The allocation extension adds common per-scan caps of 10, 25, 50 and 100
event-token observations, with 50 declared as its primary cap. Whole events are
included greedily in score order if their cost fits; there is no simultaneous
three-event limit. Shared tokens across events are counted separately, and
actual utilization can vary, so this is a common-cap proxy comparison rather
than measured computing or subscription cost. The earlier-only chooser retains
N=3 and uses the first four scored weeks for warm-up. It chooses among five
policies using cumulative captured value on earlier known label windows ending
strictly before each weekly origin. Evaluating candidate policies assumes full
historical feedback, including events the chooser would not have monitored.
Three outcome-selected cases illustrate disagreements and shared misses on the
earlier feature subset; they are not a representative sample or causal analysis.

```powershell
python FYP/experiments/allocation_extension.py --output artifacts/extensions/recomputed_allocation
python -m unittest discover -s extension_tests -v
```

This script performs no model refits or economic replay. It retains the frozen
design, executed source, input digests, per-scan resource audits, per-origin
candidate histories, complete case features and competing candidate pools.
The report rebuild verifies the submitted extensions and adds the
assumption/check and conclusion/evidence tables from saved results. All new
analyses remain exploratory because the dataset and earlier results had already
been inspected.

The focused sensitivity study tests historical event-mean priors of 6, 24 and
72 observations, plus 14-day and 28-day forest training windows. The latter add
54 fixed-setting fits over nine origins and three seeds, with strict label-end
cutoffs; the expanding controls reuse existing three-seed scores. Every setting
is reported without selecting a new primary policy. New fits use only the
earlier feature period, while historical controls also use the full-period
saved outcome timeline. Their different denominators remain explicit.

```powershell
python FYP/experiments/sensitivity_extension.py --output artifacts/extensions/recomputed_sensitivity
python -m unittest discover -s extension_tests -v
```

The same script measures warm scoring and N=3 allocation on 12 cached scans from
the last retained feature block, using saved forest/logistic models and quoted
gap. Five warm-ups precede 30 measured calls per scan and configuration. Forest
timings compare four jobs with one, preserving original predictions. Individual
samples, candidate counts, token savings, timer units, software/machine metadata
and exclusions are retained. These timings exclude raw reads, feature
construction, feeds, fitting and historical feedback; they do not establish
end-to-end savings. Existing timing evidence is reused on resume; choose a new
directory to measure another machine or run. Report rebuilds regenerate tables
from saved measurements without retiming or refitting.

There are 28 original tests and ten extension tests. The report integrates the
results by research question, adds a critical literature comparison, and retains
the original prediction and economic outputs. All four extensions remain
retrospective analyses of an inspected cohort. Their protocol/source digests
prevent a changed experiment from silently replacing submitted evidence.

## Reproduce

Use Python 3.12 or newer and the pinned dependencies:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -e ".[notebook,verification]"
.venv/Scripts/python.exe -m unittest discover -s FYP/tests -v
.venv/Scripts/python.exe -m FYP
.venv/Scripts/python.exe -m jupyter lab research.ipynb
```

The default workflow verifies the prepared dataset and checkpoints, reproduces
scores from all weekly models, recalculates metrics, reconciles economic
accounting, and regenerates tables, figures and the PDF. Validated saved raw
preprocessing, fits and event replays are reused. This avoids paying the full
training/replay cost when reviewing the submission.
The selected policy is computed from the saved primary metrics. The editable
report narrative describes this submitted result; a rerun with a different
winner requires revising that narrative before PDF generation.

For a complete reconstruction from raw books, including all model tuning,
weekly refits and persistent event replays, use:

```powershell
.venv/Scripts/python.exe -m FYP --full --device cpu
```

Alternatively set `FULL_RERUN=True` in the notebook. Fresh runs use a new `runs/`
directory and preserve the supplied results. CUDA is optional and can accelerate
TabM training; outputs need not be bit-identical across hardware. A full run is
substantially slower than the cached workflow. Report PDF generation is local
and does not require a TeX installation or network access.

## Data and opportunity definition

Saved Dome L2 depth and frozen event metadata cover 14 October 2025 to
3 February 2026 at 08:00 UTC. The supported cohort has 46 negRisk and 5 ladder
events. Event availability varies: the forward ranking population contains
44 distinct events and at most 19 simultaneous candidates.

- **negRisk complete set:** buy the YES token for every member of an audited
  mutually exclusive, exhaustive event. The basket pays $1 per complete share.
- **Ladder:** buy broader-condition YES and narrower-condition NO when the
  contract rules establish that the narrower outcome implies the broader one.
  The basket pays at least $1 per share. The replay books the conservative $1
  lower bound.

A qualifying displayed opportunity requires complete valid member books,
exchange minimum size and tick compatibility, at least five integer shares,
positive integrated-depth margin and positive worst-limit margin. All required
legs remain in the basket. There is no fixed dollar profit threshold, quote-age
cutoff or minimum observed persistence. Sell/complement routes are outside the
implemented study.

An hourly target is known only when every active basket is decidable throughout
the window. Missing evidence remains unknown, including a partially observed
window in which a positive gap was witnessed. Unknown event-hours still compete
for monitoring slots; they are excluded from fitting and known-outcome metrics.

## Models and evaluation

The comparison includes quoted-gap, depth and freshness rules; logistic
regression, random forest and XGBoost with fixed book-only and combined-feature
controls; tuned combined-feature classifiers; two-stage forest and TabM; and
three fixed rank ensembles. There are 33 book features and three optional
metadata features: broad event type, topic and age.

Thirteen weekly expanding-history fits follow an initial 28-day training
history. Complete label windows must finish strictly before each fitting
origin. Periodic tuning uses two earlier chronological blocks, with capture of
peak quoted dollars at N=3 as its objective. Execution P&L never enters fitting,
tuning or ensemble weights.

The fixed book-feature forest leads the saved pooled opportunity comparison.
Its estimated economic advantage is concentrated in the December Fed event;
the event-exclusion analysis is part of the main results. The study is a
retrospective chronological comparison of this cohort, not evidence of general
market profitability or an untouched final-test model selection.

## Source layout

| Directory | Responsibility |
| --- | --- |
| `FYP/data/` | Books, as-of alignment, coverage, features, labels and raw reconstruction |
| `FYP/rules/` | Structural basket membership and payoff constraints |
| `FYP/models/` | Preprocessing, estimators, tuning grids, TabM, metrics and uncertainty |
| `FYP/simulator/` | Depth sizing, signal discovery, reservations, per-leg execution and accounting |
| `FYP/experiments/` | Weekly study, persistent replay and end-to-end runner |
| `FYP/reporting/` | Verification, CSV/figure export and report rendering |
| `FYP/tests/` | Temporal, coverage, sizing, payoff and execution tests |

See [the dataset guide](docs/dataset.md) and [simulator guide](docs/simulator.md)
for intuitive definitions, schemas and assumptions. Saved digests detect changed
inputs and checkpoints; their presence does not establish that the source feed
captured every exchange update.
