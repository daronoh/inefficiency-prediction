# DISSERTATION_PLAN — current research specification and work queue

**Status:** 26 August 2026

[PROJECT_OVERVIEW_AND_ROADMAP.md](PROJECT_OVERVIEW_AND_ROADMAP.md) is the
narrative project overview. [DATA_DICTIONARY.md](DATA_DICTIONARY.md) is the
field-level authority for stored artifacts. This file records the binding
research decisions, unresolved blockers, current evidence, and ordered work
queue. It supersedes older planning statements in the CA report and git
history.

## 1. Research objective and definition of success

### 1.1 Headline objective

The project tests whether observable, point-in-time market state can be used
to allocate scarce monitoring capacity to Polymarket event groups that are
more likely to develop a short-horizon pricing inconsistency.

The system is a screening and resource-allocation system:

1. At decision time \(t\), construct the eligible event-group universe.
2. Use information available by \(t\) to score each group.
3. Intensively monitor only a fixed-cost shortlist.
4. Detect and simulate trades only in groups shortlisted before an opportunity
   begins.
5. Compare the model-selected shortlist with repeated random selection from
   the same eligible set, using identical monitoring, capital, timing, and
   execution assumptions.

The project predicts where an inefficiency may appear; it does not predict
which real-world outcome will win.

### 1.2 Revised practical endpoint

The earlier CA-report scope ended at producing a useful shortlist. The revised
dissertation objective is stronger: the shortlist must be tested inside a
common economic strategy simulation.

Practical project success requires **both**:

1. **Absolute profitability:** positive net simulated P&L under one
   predeclared and justified realistic availability, execution, latency, fee,
   and capital configuration.
2. **Economic lift:** greater net simulated P&L than repeated
   resource-matched random monitoring under exactly the same configuration,
   with uncertainty assessed in a way that respects date and event-family
   dependence.

Predictive lift, PR-AUC, precision at \(k\), and opportunity capture are
important intermediate results. Beating random while losing money shows that
the model contains useful predictive information, but it is not a profitable
strategy. Positive profit without beating random does not show that model
curation adds value. The research tests whether profitability is achievable;
it does not assume the answer will be positive.

### 1.3 Research questions

- **RQ1 — prediction:** Can point-in-time market state predict whether a
  book-clean, displayed-depth-feasible long-side opportunity will start within
  the next hour?
- **RQ2 — economic value:** At the same monitoring and capital budget, does
  model top-\(k\) monitoring earn more net simulated profit than repeated
  random monitoring?
- **RQ3 — absolute viability:** Does the model-led strategy retain positive
  net simulated P&L under the predeclared primary execution scenario?
- **RQ4 — robustness:** Do the results survive time, unseen event/template
  families, freshness, fee, latency, size, and execution sensitivities?
- **RQ5 — explanation:** Which observable features are associated with future
  inefficiency risk, and are those relationships transferable?

The existing label is not itself a profitability label. It models a displayed
depth opportunity under a carried-forward-book assumption. Latency, multi-leg
risk, partial fills, unwind losses, capital constraints, and other execution
effects enter later in the backtest.

## 2. Fixed decisions and unresolved blockers

| Fixed before modelling | Must still be frozen on development data |
|---|---|
| negRisk is the core stratum; universe v1.1 is frozen | Point-in-time eligibility rule \(eligible(g,t)\) |
| Cross-market long/short gap definitions | Exchange snapshot time versus Dome availability time |
| Development/lockbox calendar split | Cheap broad-universe screening feature tier and refresh cost |
| Primary target meaning: y_long_100_60m, 100 shares, fee 0, clean long spell, duration at least 5 seconds, start in \((t,t+60m]\) | Primary monitoring-cost unit, \(k\), cadence, shortlist duration, and switching rule |
| 10/1,000-share and 15-minute labels are secondary robustness targets | Fixed-width event-minute feature matrix |
| Walk-forward temporal validation with at least a one-day embargo | Model-tuning objective and one headline budget |
| No shuffled split as the headline evaluation | Multi-leg entry, partial fill/unwind, latency, merge, fee, and capital rules |
| Model selection is compared with repeated resource-matched random selection | Frozen headline economic metric and uncertainty procedure |

The target semantics remain fixed. If the availability convention changes,
panels, timestamps, spells, labels, and attrition may need rebuilding without
changing what the target means.

## 3. Established market-mechanics findings

### 3.1 Mirror books

A binary market's YES and NO books are complementary views of one order pool:

\[
\operatorname{ask}_{NO}=1-\operatorname{bid}_{YES}.
\]

The project previously recorded this equality for 99.999% of approximately
1.66 million aligned rows. Therefore, buying YES and NO inside one binary
market for less than $1 is structurally ruled out by the spread. The core
inefficiency target is cross-market inconsistency inside a winner-take-all
negRisk group.

The pair-panel code remains in the repository, but the pair Parquet artifacts
and the earlier mirror-book verification table are not currently retained.
The final dissertation should retain a reproducible verification artifact.

### 3.2 Group signals

For a fully covered event with \(N\) mutually exclusive and collectively
exhaustive member markets:

\[
\operatorname{long\_gap}=1-\sum_{i=1}^{N}\operatorname{best\_ask}_i.
\]

At displayed size \(q\):

\[
\operatorname{exec\_long\_gap}_{q}
=1-\sum_{i=1}^{N}\operatorname{VWAPAsk}_{i,q}.
\]

A positive value means a complete YES basket is displayed for less than its
guaranteed $1-per-share combined payout. The signal requires complete event
membership, order-book coverage, sufficient displayed depth in every member,
and the applicable cleanliness gate.

The subset short-side lower bound is:

\[
\operatorname{exec\_short\_gap}_{q}
=\sum_{i\in S}\operatorname{VWAPBid}_{i,q}-1.
\]

Short-side spells are retained for attrition and descriptive analysis, but
their median value is too small for the current primary prediction target.

### 3.3 Fees and total value

Polymarket's taker-fee expression is:

\[
\text{fee}=q\times r\times p(1-p),
\]

where \(q\) is shares, \(r\) is fee rate, and \(p\) is the leg price. The
implemented counterfactual gap is per share:

\[
\operatorname{net\_gap}_{q,r}
=\operatorname{exec\_gap}_{q}-r\sum_i p_i(1-p_i).
\]

The corresponding basket value is:

\[
q\left[\operatorname{exec\_gap}_{q}-r\sum_i p_i(1-p_i)\right].
\]

All 949 negRisk markets in the core sample have Gamma
feesEnabled=false, so fee-zero labels describe the historical core. Present
category fees are counterfactual sensitivity tests. They are partial-
equilibrium counterfactuals: market behaviour and liquidity might have
changed had those fees existed historically.

Current project notes describe the CTF merge transaction as atomic and
gasless through a relayer. This must be supported by an authoritative source
and its operational conditions. Acquiring all \(N\) order-book legs is not
atomic, and merge exit does not erase taker fees paid on entry. Maker entry is
an upper-bound scenario unless fill probability and queue position are
modelled.

## 4. Frozen universe, coverage, and scope

### 4.1 Universe v1.1

The selection is frozen from the resolved-only Gamma JSONL; do not re-scrape a
changing live universe.

| Stratum | Event groups | Markets | Tokens | Role |
|---|---:|---:|---:|---|
| negRisk | 50 | 949 | 1,898 | Core study |
| ladder | 30 | 361 | 722 | Optional implication-arbitrage extension |
| control | 25 | 176 | 352 | Limited selection-bias robustness sample |
| serial | 752 | 752 | 1,504 | Optional fee-era microstructure comparison |
| **Total** | **857** | **2,238** | **4,476** | — |

The non-serial rules include event size at most 35, a $1 million floor for
negRisk/ladder ranking, top 50 negRisk events, top 30 genuine ladder events,
and a seed-42 sample of 25 controls. Match-style template bundles are excluded
from the ladder stratum. The control stratum is dominated by sports/esports
prop bundles and is not the random monitoring baseline.

The core is an ex-post lifetime-volume-selected top 50 from 530 eligible
negRisk candidates. It is a proof-of-concept sample, not every Polymarket
event and not automatically representative of the live platform.

### 4.2 Coverage

- The nominal download request was 14 October 2025 to 31 March 2026.
- The metadata universe freezes around 4 February 2026 and contains resolved
  outcomes.
- Actual stored label coverage is 14 October 2025 19:38 UTC to 3 February
  2026 08:22 UTC.
- Dome histories exist for 1,872 of 1,898 core tokens.
- The missing 26 token histories represent both outcomes of 13 markets:
  12 Bolivia members and one League of Legends member.
- Metadata marks 48 of 50 groups as full-cover-capable. Missing Dome data
  reduces effective long-side coverage to 46 groups.

The metadata dump being resolved-only does not imply that full order-book
coverage exists through every market's resolution.

### 4.3 Semantic-audit findings

- Ten price events are two-sided reach/dip chains. Implication holds within a
  direction only.
- what-price-will-bitcoin-hit-in-2025 contains a relisted duplicate rung;
  deduplicate by question and retain the later listing for a ladder extension.
- Eighteen weekly Elon-tweet groups have overlapping windows and form one
  dependent template family.
- Two Fed groups account for 89% of documented clean top-of-book hits above
  one cent.
- Control contains 22/25 sports or esports prop bundles and three
  treatment-like structures; describe it honestly as a limited robustness
  sample.

## 5. Point-in-time eligibility and availability

### 5.1 Eligibility population

The stored labels include four groups that can never produce the long target
under the current construction:

- bolivia-presidential-election;
- lol-worlds-2025-winner;
- super-bowl-champion-2026-731; and
- uefa-champions-league-winner.

These rows must not be ordinary negatives. Before modelling, define one
point-in-time \(eligible(g,t)\) rule and apply it identically to training,
metrics, model ranking, heuristics, random selection, and backtesting.

Across the full sample, 46 groups have effective long-side coverage. Forty-four
of them contribute development rows. The permanent-coverage development
filter leaves:

- 44 event groups;
- 1,699,115 rows;
- 12,562 positive y_long_100_60m rows; and
- prevalence 0.7393%.

The unfiltered stored denominator is 1,989,260 development rows with prevalence
0.6315%; retain it only as a descriptive artifact statistic.

supports_full_cover uses eventual-winner presence in a resolved-only dump. It
is a retrospective sample-quality filter, never a model feature. A live design
needs point-in-time event membership and completeness.

### 5.2 Availability clock

Token features retain both exchange snapshot_time and Dome indexed_at. Current
group construction uses indexed_at_ms only to choose the latest duplicate at a
given snapshot timestamp, then drops it and aligns on snapshot_time.

Therefore, all current panels, spells, labels, and attrition results use the
exchange-snapshot-time convention. They are not yet demonstrated to be safe
under Dome availability.

- If deployment assumes direct exchange subscriptions, justify snapshot_time.
- If deployment assumes Dome availability, rebuild and independently review
  group panels, eligibility, spells, labels, attrition, and latency using
  indexed_at.

Scheduled end date, category, and final event membership come from final-state
Gamma metadata. Verify that they were unchanged historically, reconstruct
their history, or exclude them in a strict model ablation.

### 5.3 Two-tier monitoring requirement

The computational motivation only holds if broad screening is cheaper than
intensive monitoring. Many current candidate features—full-depth VWAP,
imbalance, update intensity, and current group gaps—require order-book data.

Define:

1. a cheap, periodic broad-universe feature tier;
2. its API, bandwidth, compute, and refresh cost;
3. the expensive data activated only for shortlisted groups; and
4. shortlist-switching cost.

A model maintained from continuous full books for every candidate is an oracle
screening experiment. It may establish predictability, but not computational
savings.

## 6. Primary target and strategy alignment

The fixed primary target is y_long_100_60m:

\[
y_{g,t}=1
\]

if a fee-zero, book-clean, displayed-depth-feasible 100-share long spell lasting
at least five seconds starts in \((t,t+60\text{ minutes}]\).

Secondary targets are the 10- and 1,000-share versions, 15-minute horizons, and
max_gap_100_60m. The CA report's proposed count target is deprioritised; if
revived, count unique spell starts rather than repeated positive snapshots.

The target records a new spell start, not an opportunity already active at
\(t\). The primary backtest must therefore credit a selector only when the
group entered its shortlist before the spell began. Do not count already-
active spells or every positive minute as separate captured trades.

Displayed-depth feasibility is not realised profitability. Only the backtest
may convert a selected spell into simulated P&L after applying the frozen
availability, latency, multi-leg, fill, fee, merge, and capital rules.

## 7. Profitability-led evaluation protocol

### 7.1 Development and quarantined lockbox

- **Development:** times before 8 January 2026 00:00 UTC. Rows whose one-hour
  label window crosses the boundary are embargoed, and crossing spells are
  clipped.
- **Lockbox:** times from 8 January 2026 onward to the last observed panel
  time. It is a quarantined performance holdout, not literally untouched.

Lockbox panels, 226,419 label rows, and 6,247 spell-definition rows have been
mechanically materialised, and project notes record invariant-level review.
These operations are not a model or strategy performance evaluation.

Lockbox outcomes must not determine features, eligibility, model choice,
hyperparameters, selector settings, budget, execution rules, fees, or
thresholds. Commit or hash the complete specification before the single
headline performance run and log later access.

Only 10 groups have stored lockbox rows; eight are long-side eligible. Of those
eight, six appear in development and two are new. Report eligible-all,
seen-group, and new-group results separately and acknowledge that eight groups
cannot support broad generalisation claims.

### 7.2 Development validation

- Expanding walk-forward folds; no shuffled headline split.
- Embargo of at least one day.
- Hyperparameters and thresholds tuned only on inner training folds.
- Scalers and learned transformations fitted on training data only.
- Leave-event/template-family decomposition, holding all Elon weeks together.
- Event-balanced and non-overlapping-window sensitivities because one spell
  can label many preceding minutes.

### 7.3 Selector comparison

At every ranking time:

1. Construct \(eligible(g,t)\) using only declared point-in-time information.
2. Score all eligible groups using the declared broad tier.
3. Select the model's top fixed-cost shortlist.
4. Draw repeated random selectors from the identical eligible set and cost
   budget.
5. Apply the same cadence, shortlist duration, execution engine, capital
   rules, and spell de-duplication to every selector.

Selectors to report:

- repeated random monitoring — primary resource-matched baseline;
- simple liquidity/activity, current-gap, and recent-movement heuristics;
- regularised logistic regression;
- LightGBM or another declared gradient-boosted model; and
- monitor-all as a coverage/oracle ceiling, not an equal-cost baseline.

Because event sizes differ, supplement event-\(k\) with token-, market-, or
subscription-matched budgets.

Repeated random draws measure selector luck conditional on this historical
path. They are not confidence intervals for future performance. Use paired
date-block/event-family bootstrap, clustered permutation, or another
dependence-aware method for model-minus-baseline uncertainty.

### 7.4 Predictive metrics

- PR-AUC;
- precision at \(k\);
- unique-spell capture/recall at the fixed budget;
- lift over random;
- calibration;
- event-balanced metrics;
- non-overlapping decision-window sensitivity; and
- profit-capture frontier.

Accuracy is not a headline metric because the target is below 1%.

### 7.5 Backtest rules and economic metrics

Freeze before the final comparison:

- availability clock and detection time;
- trade size and entry condition;
- latency repricing at 0, 2, 10, and 60 seconds;
- leg ordering and non-atomic basket acquisition;
- partial fills, failed baskets, and unwind losses;
- depth consumption and quote reuse;
- minimum order sizes;
- concurrent exposures and reserved capital;
- merge latency/failure;
- spell/trade de-duplication; and
- treatment of overlapping shortlist windows.

Report historical zero-fee results separately from counterfactual fee regimes.
Treat maker results as a bound unless queue priority and fill probability are
modelled.

Headline economic metrics:

- gross and net cumulative P&L;
- net P&L per monitored event-hour and token-hour;
- ROI on deployed and reserved capital;
- capital utilisation and maximum simultaneous exposure;
- unique spells and opportunity value captured;
- drawdown/downside;
- monitoring reduction including screening overhead; and
- paired model-minus-random uncertainty.

## 8. Current artifact and pipeline status

| Component | Status on 26 August 2026 |
|---|---|
| Frozen universe | Materialised: 2,238 markets / 4,476 tokens; reproducible verifier retained |
| Core negRisk books | 1,872/1,898 token files; 13 markets missing |
| Token features | Materialised for all downloaded tokens: 9,460,388 rows, 40 columns |
| Group panels | 50 event-grid and 50 one-minute files under snapshot_time |
| Pair panels | Code exists; no stored pair Parquet files |
| Opportunity spells | Materialised: 33,166 rows; independently reviewed in project notes |
| Forward labels | Materialised: 2,215,679 rows; independently reviewed in project notes |
| Development attrition | Initial 10-row table materialised |
| Fixed-width model matrix | Not implemented |
| Eligibility table | Not implemented |
| Random/heuristic selectors | Not implemented |
| Logistic/LightGBM models | Not implemented |
| Economic backtest | Not implemented |
| Ladder/control/serial books | Not downloaded |
| Final lockbox model/strategy evaluation | Not run |
| Repeatable Stage 1/2 verifier artifacts | Not retained as automated tests |

The universe verifier is rerunnable. Stage 1/2 independent reimplementations
are recorded in project notes, but their code and reports are not retained;
describe them as independently reviewed, not reproducibly verified.

fee_schedule.parquet contains the one-time 8 August 2026 live CLOB snapshot
but predates universe v1.1. It has 2,227 rows, is missing 33 current ladder
markets, and retains 22 obsolete ladder rows. Core negRisk coverage is
949/949, so the core historical fee conclusion is unaffected. Preserve the
non-reproducible live fields before reconciling membership.

requirements.txt contains the data stack but not the modelling, plotting, or
testing dependencies still required.

## 9. Development-only intermediate findings

These are descriptive results, not model performance and not profitability.

| Definition | Spell count | Events |
|---|---:|---:|
| 10 shares, fee 0 | 518 | 11 |
| 100 shares, fee 0 | 405 | 11 |
| 1,000 shares, fee 0 | 219 | 8 |
| 100 shares, duration at least 5 seconds | 344 | 10 |
| Previous definition with fee rate 0.04 | 58 | 9 |
| Previous definition with fee rate 0.05 | 38 | 9 |
| Previous definition with fee rate 0.07 | 26 | 8 |
| 100 shares, at least 5 seconds, maximum staleness at most 30 seconds | 22 | 6 |

The code redetects maximal spells separately under each definition. This is
not a strict one-to-one cohort attrition chain.

Additional findings:

- median sum of asks is approximately 1.021;
- clean two-sided gating roughly halves documented top-of-book hits above one
  cent;
- two Fed events contribute 89% of those clean hits;
- y_long_100_60m has 12,562 development positives;
- short-side 100-share, five-second spells have median maximum gap 0.002; and
- the effective number of independent opportunity-bearing groups is small.

## 10. Main limitations that must appear in the dissertation

- Resolved-only metadata and ex-post lifetime-volume universe selection.
- Point-in-time Gamma metadata history is unavailable unless reconstructed.
- Incomplete/non-random Dome coverage.
- Snapshot-time versus data-availability-time uncertainty.
- Change-triggered books and severe freshness sensitivity.
- Displayed depth does not guarantee simultaneous multi-leg execution.
- Core sample was historically fee-free; current fees are counterfactual.
- Maker fills and queue position are unobserved.
- Millions of rows but very few independent opportunity-bearing events.
- Strong Fed concentration and dependent Elon template family.
- Dynamic live event membership is unresolved.
- Lockbox contains only eight eligible groups.
- Broad-tier monitoring cost may offset claimed computational savings.
- A binary opportunity label is not the same as obtainable profit.

## 11. Ordered work queue

1. Freeze availability clock, \(eligible(g,t)\), cheap screening tier,
   monitoring-cost unit, headline budget, \(k\), cadence, shortlist duration,
   tuning objective, and execution policy.
2. Rebuild and independently review panels/labels if indexed_at is selected.
3. Build the fixed-width, leakage-controlled event-minute feature matrix.
4. Complete development-only descriptive, concentration, and freshness
   figures.
5. Implement repeated resource-matched random and heuristic selectors.
6. Train and validate regularised logistic regression.
7. Train and validate LightGBM; GNN work remains optional.
8. Implement the shared, spell-deduplicated economic backtester.
9. Compare model, heuristic, random, and monitor-all results on development
   folds; run cost, latency, budget, event/template, and non-overlap
   sensitivities.
10. Add automated invariants and declare modelling/plotting/testing
    dependencies.
11. Freeze and commit/hash the full specification.
12. Run the quarantined lockbox performance evaluation once.
13. Pursue ladder/control/serial only if they do not delay the core economic
    result.
14. Write continuously, with economic evidence—not classification metrics—as
    the headline result.

Chapter mapping:

- mechanics, universe, and data → Data/Market Background;
- panels, spells, and labels → Methodology;
- attrition and descriptive findings → Empirical Anatomy;
- selectors and models → Screening;
- execution and P&L → Strategy/Economic Evaluation;
- event/time/cost/extensions → Robustness;
- fee counterfactual, timing, selection, and execution caveats → Limitations.

## 12. Data preservation and working conventions

Irreplaceable or partially irreplaceable:

- data/polymarket_markets_1y.jsonl — frozen resolved-market dump; keep an
  offsite copy;
- data/processed/fee_schedule.parquet — preserve the 8 August live CLOB
  fields before rebuilding; and
- downloaded Dome histories — regenerable in principle but API-expensive.

Environment:

- DOME_API_KEY is user-held; the older committed key is considered leaked and
  must remain rotated.
- DATA_ROOT may point worktrees to the main checkout's data directory.
- Venv Python:
  C:/Users/Daron/coding/inefficiency-prediction/.venv/Scripts/python.exe.

Working rules:

- Do not use final outcomes, realised closed time, lifetime volume, or other
  ex-post fields as prediction features.
- Verify or ablate scheduled end date, category, and final membership.
- Treat eligibility, feature availability, and execution as point-in-time.
- Preserve development/lockbox quarantine and log lockbox access.
- Verify nontrivial transformations against real data and retain the verifier.
- Update DATA_DICTIONARY.md whenever a stored schema changes.
- Commit coherent units with the research rationale in the message.
