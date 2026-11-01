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
| Dissertation volume and editable source | `report/dissertation.pdf`, `report/dissertation.md` |
| Python package, including 28 focused tests | `FYP/` |
| Executable research workflow | `research.ipynb` |
| CSV tables, 50 figures and indexes | `artifacts/tables/`, `artifacts/figures/`, `artifacts/*_index.csv` |
| Saved weekly models and predictions | `artifacts/models/` |
| Persistent economic replay evidence | `artifacts/replay/` |
| Frozen membership, schemas and metadata | `inputs/` |
| Relevant raw token books and prepared examples | `data/` |

Only the Dome study is included. All paths are relative to this directory;
there is no dependency on a parent repository. Source installation and local
data are separate: copying the package alone does not copy the data/artifacts.

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
