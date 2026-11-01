# Submission report plan

This is an authoring note, separate from the documentation supplied to readers
of the research package. It records the supplied assessment and format
requirements and maps the completed Dome study to a report. It is not the
dissertation itself or a claim that the submission package is complete.

## Controlling documents

| Document | Purpose | Relevant pages |
| --- | --- | --- |
| [FA-Evaluation-Form.pdf](../../papers/FA-Evaluation-Form.pdf) | Assessment criteria for the supervisor and main evaluator | 2–3 and 6–7 |
| [FYP-Report-Format-final_000.pdf](../../papers/FYP-Report-Format-final_000.pdf) | Dissertation layout, length, front matter and citations | 1–7 |
| [2602.24037v1.pdf](../../papers/2602.24037v1.pdf) | Example of research exposition and experimental organisation | 1–9 |

The report-format document governs layout. The example paper informs the
argument and presentation; its two-column layout, numeric citation style,
portfolio task, reinforcement learning methods and empirical findings do not
transfer to this dissertation. It is not a substantive prediction-market
reference.

Neither the assessment form nor the report-format document requires 28 tests
or 50 figures. Those numbers are submission inventory targets supplied
separately, not verified university minima. Count meaningful tests and distinct
figures in the eventual deliverables; do not pad the main report to meet an
assumed quota. The format document's example deliverable counts are examples.

## Required format

- Main text: at most **55 A4 pages**, from the first introduction page through
  the conclusion; **1.5-line spacing** and **Times Roman at least 12 pt**.
- Sequence: front cover, title page, abstract, optional acknowledgement,
  contents, main report, references, appendices, back cover. Lists of figures,
  tables and abbreviations can accompany the front matter.
- Cover: dissertation designation, project title, student name, department,
  School of Computing, National University of Singapore and academic year.
- Title page: also include project number, advisor and an accurate deliverable
  inventory. Obtain missing personal/project details rather than infer them.
- Abstract: at most **200 words**, followed on the same page by at most five
  keywords and at most five subject descriptors. The format's sample also lists
  implementation software/hardware.
- Optional acknowledgement: at most one double-spaced A4 page.
- Front-matter pagination: lower-case Roman numerals. Main text: Arabic numbers
  beginning at 1; continue Arabic numbering for the references. Use hierarchical
  chapter/section numbers and keep the contents hierarchy shallow.
- References and front matter do not count towards the 55-page main-text cap.
  Keep appendices small; do not move essential evidence into an appendix merely
  to avoid the cap. Bulky software manuals/data dictionaries belong separately.
- Use author–year citations, with an alphabetically ordered reference list,
  following the supplied format's author rules. Do not copy the example paper's
  numbered references.
- The supplied format specifies ring binding for physical reports. A PDF alone
  does not resolve any separate submission-portal or physical-copy requirements.

These are requirements extracted from the supplied format, dated May 2008;
they are not a claim to have checked any newer school instructions.

## Assessment coverage

Both assessment forms allocate 20% to problem understanding, 10% to extension
of knowledge, 30% to methodology/implementation/analysis and 20% to the report.
The remaining 20% covers effort/initiative for the supervisor and presentation
and questions for the main evaluator. Each form states that it contributes 35%
to the final grade; the documents do not describe the remaining assessment.

| Criterion | Evidence to provide |
| --- | --- |
| Rationale and problem definition | Explain the limited monitoring budget, the hourly decision, the prediction horizon, the candidate population and what success would mean. Distinguish this task from detecting a currently visible gap. |
| Critical literature survey | Compare structural arbitrage research, tabular prediction methods and execution-aware backtesting. Explain which assumptions and tasks differ from this study. Include the actual TabM paper; substantiate market-specific claims with relevant primary sources. |
| Extension of knowledge | State the contribution as budgeted event selection, coverage-aware labels, comparative modelling and a separate economic evaluation. Do not claim invention of random forests, TabM or a generally superior market strategy. |
| Methodology | Define targets, coverage rules, as-of features, expanding histories, purging, tuning, ranking, budgets, metrics and replay assumptions precisely. Explain why each design matches the question. |
| Implementation | Trace raw token books through event examples, model scores, selections and replay accounting. Identify reproducibility controls and what tests establish. |
| Analysis | Interpret matched comparisons, metadata ablations, concentration, unknown outcomes, latency sensitivity and partial-fill losses. Discuss negative findings as well as gains. |
| Report organisation and relevance | Build a connected argument; keep pilot studies, progress messages and superseded experiments out. Use appendices for secondary configuration and detailed result tables. |
| Presentation and questions | Prepare to explain the target/P&L distinction, temporal safeguards, cohort limits and concentration without relying on implementation jargon. |

Do not fabricate a record of independent work, supervisor meetings or effort
from these criteria. The report can evidence technical work, but cannot replace
the supervisor's assessment of the process.

## Research argument

**Question:** Can an event-level model, using only information available at a
scheduled scan, allocate a limited monitoring budget to events that will exhibit
a depth-supported structural arbitrage opportunity over the next hour?

The primary budget is N=3; N=5 is secondary. The first layer predicts opportunity
occurrence and peak quoted basket value. The second evaluates the resulting
monitoring policies under declared execution assumptions. Its primary scenario
uses 100 ms total observation-to-order-arrival delay and uncapped capital/shares.
Execution P&L is not a training label or a tuning objective in the first layer.

Use the example paper's *problem → precise method → controlled comparisons →
ablations → sensitivity → qualified conclusion* progression. In particular,
borrow its practice of making each experiment answer an explicit claim. Do not
add unrelated reinforcement learning, theoretical propositions or new model
architectures merely to resemble that paper.

## Proposed chapters

Page allocations below are planning estimates, not requirements. About 47–51
main-text pages leaves space for legible tables and figures below the 55-page cap.

| Chapter | Content | Approximate pages |
| --- | --- | --- |
| 1. Introduction | Motivation, research question, practical constraints, contributions and scope | 4 |
| 2. Background and related work | Event/market/token vocabulary; structural arbitrage; critical comparison of prior prediction, tabular-learning and evaluation methods | 6 |
| 3. Problem formulation | Event candidate sets, scan and horizon, valid baskets and payoff reasoning, occurrence/value targets, ranking and resource budget | 5 |
| 4. Data and example construction | Dome cohort, raw depth, backward as-of alignment, recording coverage, known/unknown labels, feature dictionary and descriptive statistics | 7 |
| 5. Prediction methodology | Rules; logistic regression, random forest and XGBoost; two-stage forest/TabM; rank ensembles; fixed versus tuned comparisons; chronological training and tuning; metrics | 7 |
| 6. Economic replay | Sizing, depth reservations, arrival-time support, leg outcomes, unwinding, cash accounting and latency scenarios | 5 |
| 7. Results | Budgeted capture, weekly variation, metadata/tuning/ensemble comparisons, concentration, economic outcomes, uncertainty and latency | 9–11 |
| 8. Discussion and conclusion | Answers to the research questions, limitations, practical interpretation and specific future work | 4–6 |

Appendices: configuration grids and selected settings; complete feature
dictionary if too long for Chapter 4; supplementary policy/week tables; a short
reproduction guide and test/figure indexes. Put substantive coverage and
concentration findings in the main text.

## Evidence for the current study

The following paths identify the completed study's current source artifacts.
Final packaging should copy the relevant evidence into `artifacts/` with portable
links, rather than making the submission depend on unrelated result folders.

| Report item | Current source |
| --- | --- |
| Dates, weekly origins, feature views, configurations and tuning rules | `results/walk_forward/design.json` and `results/walk_forward/folds/*/bundle.json` |
| Raw-to-example definitions and eligibility logic | Relevant data pipeline modules, frozen cohort and coverage manifests; verify against the packaged code |
| Forward population and missingness | `results/walk_forward/analysis/population.json`, `results/walk_forward/populations.csv` |
| N=3 comparison | `results/walk_forward/analysis/primary_prediction.csv` |
| N=5, event exclusion and per-week comparisons | `results/walk_forward/metrics.csv`, `results/walk_forward/analysis/weekly_metrics.csv` |
| Matched metadata ablation | `results/walk_forward/analysis/metadata_comparison.csv` |
| Tuning selections and inner comparisons | `results/walk_forward/tuning/*/search.csv` and `selected.json` |
| Event concentration | `results/walk_forward/analysis/event_capture.csv`, `event_pnl.csv` |
| Paired uncertainty estimates | `results/walk_forward/analysis/paired_capture_intervals.csv`, `paired_pnl_intervals.csv` |
| Economic summaries and latency sensitivity | `results/walk_forward_replay/summary.csv` |
| Attempt-level and cumulative accounting | `results/walk_forward_replay/attempts.parquet`, `results/walk_forward/analysis/pnl_curves.parquet` |
| Model prediction and replay verification | `results/walk_forward/analysis/verification.json` |

Suggested main-text visuals: pipeline diagram; temporal training/evaluation
diagram; candidate-pool and label-coverage history; N=3 capture comparison; N=5
comparison; matched metadata ablation; weekly capture; event concentration and
exclusion; primary economic comparison; readable cumulative P&L small multiples;
latency sensitivity; completed versus partial-basket contributions. The complete
figure catalog may be delivered separately in `artifacts/`; 50 figures need not
appear in the main report.

Every reported table must have a CSV counterpart. Every figure needs a caption
explaining its population, budget, units and relevant assumptions. Preserve
undefined metrics when their denominator is zero; do not plot them as failures.

## Claims and interpretation

The current completed results support the following account, subject to final
artifact checks during packaging:

- The study uses a frozen supported cohort of 46 negRisk and 5 ladder events.
  The forward ranking population contains 44 distinct events and 19,766
  event-hours; not every cohort event supplies usable examples in every period.
- The raw calendar is 14 October 2025–3 February 2026 at 08:00 UTC. After an
  initial 28-day history, 13 weekly fits generate chronological predictions from
  11 November onwards. Only earlier known windows that finish strictly before
  each fitting origin enter training.
- The forward population has 15,787 known outcomes, including 359 positive hours,
  and 3,979 unknown outcomes. Unknown rows can consume monitoring slots but do
  not become negatives or training labels.
- The fixed book-feature forest leads pooled quoted-value capture@3: 99.53%,
  with 340/359 positive hours captured (94.71%). Quoted-gap captures 60.76% of
  value and 264/359 positives (73.54%). This is a retrospective comparison of
  chronological predictions, not an untouched final-test model selection.
- At N=3 and 100 ms, that forest's simulated known P&L is $5,873.32, compared
  with quoted-gap's $4,370.05 and monitor-all's $5,944.97. These are historical
  replay estimates, not confirmed real fills or a capital-normalised return.
- The December Fed event supplies 83.15% of the quoted-value denominator.
  Excluding it and reranking leaves forest P&L $730.52 versus quoted-gap $736.36.
  The observed economic advantage is therefore concentrated, not established
  across a broad independent set of events.
- Tuning, metadata and ensembling have mixed effects. TabM does not lead the
  present comparison. Preserve these findings rather than imply every added
  modelling component improves results.

Describe the final study directly. A diary of abandoned designs is unnecessary.
However, do not describe retrospectively chosen policy comparisons as
preregistered, call inspected data untouched, or remove limitations that change
the interpretation. Full-window coverage, frozen retrospective membership,
missing intervals, source-time alignment, zero-fee assumptions, independent
per-leg fill support and uncapped funding must remain explicit.

Before calling the report submission-ready: complete and verify the relevant
literature review, obtain title-page details, reconcile all quoted numbers with
the packaged tables, check every figure/reference, compile and inspect the PDF,
and verify its main-text page count and formatting. The supplied documents do
not themselves establish that these steps are complete.

## Execution status

A standalone submission is assembled under `submission/`, including the report
PDF and editable source, the FYP package and 28 focused tests, an executed
`research.ipynb`, relevant raw data, 72 CSV result tables and 50 indexed figures.
The report has 24 main-text pages and a 175-word abstract. All 29,308 rows and
51 prepared columns were independently reconstructed from raw books; qualified
signal intervals also matched. All 199 fitted stages reproduce their scores,
and six fixed first-origin classifiers were freshly refitted as a smoke check.
The complete historical model search was reused during packaging, not claimed
as a new independent experiment. The notebook supports a fresh full run.

The volume is an author-review draft; factual limitations and retrospective
selection are explicit. The original research workspace is preserved separately.
