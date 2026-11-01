# Final held-out evaluation

## Main finding

At the primary **hourly screening, N=3 and 100 ms arrival** settings, every saved
model and the quoted-gap rule captures **all 16 known positive event-hours** and
**100% of the known peak quoted-value target**. Their persistent trading replays
are identical: **$32.31 net simulated P&L**. This final sample provides no observed
capture or economic advantage for machine learning over quoted-gap.

The experiment demonstrates conditional monitoring savings within this frozen
cohort. It does not establish a generally superior model, broad Polymarket
profitability or final-period ladder performance. The final period has now been
opened and evaluated; it must not be reused for model tuning while retaining
the same held-out claim.

## Fixed protocol and data

The test covers **8 January 2026, 00:00 UTC to 3 February 2026, 08:00 UTC**,
with the endpoint excluded: 632 hourly windows. The period and model comparisons
were fixed in the [protocol](final_test_protocol.md) after a timestamp-only audit
and before examining final opportunity outcomes. N=3 is primary; N=5 is secondary.
These choices were informed by earlier development analysis, which was explored
repeatedly.

The primary universe retains the original 46 structurally supported negRisk and
five audited ladder events. Only eight negRisk events have later ranking rows;
there are no audited ladder entries in this period. Three events were already
in the frozen universe but supplied no development examples, and remain eligible
for final screening. A secondary view uses development events only.

| Final dataset | Count |
| --- | ---: |
| Eligible event-hours | 2,473 |
| Known labels | 1,625 |
| Positive hours | 16 |
| Negative hours | 1,609 |
| Unknown outcomes | 848 |
| Witnessed opportunities in unknown windows | 0 |
| Positive hours absent at the screening instant | 15 |

The 16 positives come from **January Fed (13)** and **next country the US strikes
(3)**. Six of eight events have no known positive hour. The sum of hourly peak
quoted margins is **$84.62**, of which **$84.30** belongs to January Fed.
These are one-basket maxima used as ranking weights; repeated displayed liquidity
across hours is not additive collectible profit.

Labels apply the same depth, minimum-order and worst-limit-margin checks as
development and exclude execution outcomes. Unknown rows compete for slots but
never become negatives. Full-member timestamp extents alone bracket 1,262
event-hours; 1,625 hours are label-decidable because an observed entry-blocking
leg can sometimes establish no opportunity despite another missing leg. All
figures depend on the recording-continuity and carry-forward assumptions.

## Opportunity prediction

All five fitted policy names, quoted-gap and monitor-all attain the same primary
capture. The two forest names share one final fitted artifact; they are not
independent model evidence.

| Policy/reference at N=3 | Positive-hour capture | Peak quoted-value capture | Simulated P&L at 100 ms |
| --- | ---: | ---: | ---: |
| Quoted-gap rule | 16 / 16 | 100% | $32.31 |
| Fixed book logistic | 16 / 16 | 100% | $32.31 |
| Fixed book forest | 16 / 16 | 100% | $32.31 |
| N=3-retuned book logistic | 16 / 16 | 100% | $32.31 |
| N=3-retuned book forest | 16 / 16 | 100% | $32.31 |
| N=3-retuned two-stage TabM | 16 / 16 | 100% | $32.31 |
| Monitor all | 16 / 16 | 100% | $32.31 |
| Random selection | 73.13% expected | 72.44% expected | $23.23 mean over 20 seeds |

All 16 positives occur during the 503 hours with more than three eligible events.
The competitive-hour comparison therefore also ties. Each deterministic policy
captures all 15 known positive hours that were absent at screening. Quoted-gap's
success does not require an executable opportunity already visible at the scan:
it ranks relative current basket margins, which can be negative.

Continuous-score diagnostics differ: average precision is **0.2090** for fixed
logistic, **0.1934** for TabM and **0.1904** for quoted-gap. These secondary values
do not establish an advantage at the predeclared operating budget, where capture
is saturated. With only 16 positives concentrated in two events, there is little
evidence for choosing a final ML winner.

At N=5 all 632 primary-pool hours have at most five candidates. Every policy
selects the entire pool, including random selection. Selection and deterministic
replay equality were verified; N=5 cannot test ranking quality in this period.

## Monitoring burden and unknowns

Each N=3 policy selects **1,797 event-hours**, compared with 2,473 for monitor-all,
a **27.3% reduction**. A token-window proxy also reflects how many constituent
markets each selected event contains:

| Policy | Selected token-windows | Reduction against monitoring all | Unknown selected hours |
| --- | ---: | ---: | ---: |
| Monitor all | 42,084 | 0% | 848 |
| Quoted-gap | 32,672 | 22.4% | 740 |
| Fixed logistic | 27,424 | 34.8% | 665 |
| Two-stage TabM | 25,940 | 38.4% | 600 |

Token-windows count required tokens per selected event-hour, not actual feed
messages, CPU time or measured financial cost. Feature construction and model
inference also consume resources. The differences describe allocations in this
sample and do not justify selecting a new model using the final test.

Unknown outcomes account for **41.18%** of quoted-gap's selected hours and
**33.39%** of TabM's. Capture metrics use known denominators, so 100% observed
capture cannot establish capture of unrecorded opportunities. The policies rank
different nonpositive or unscorable event-hours even though their observed
opportunities and trades coincide.

## Economic evaluation

The primary replay has uncapped capital and shares, zero imposed random failure
probability, immediate submission, fixed decision-time limits and a one-second
partial-basket unwind. The frozen zero-exchange-fee scenario and zero additional
attempt costs are retained. Each policy/scenario starts with an empty ledger
at the test boundary and preserves reservations across final hours.

The common N=3, 100 ms result is:

| Quantity | Result |
| --- | ---: |
| Attempts | 20 |
| Completed baskets | 18 |
| Partial baskets | 2 |
| Unknown attempted executions | 0 |
| Completed-basket margin | $37.93 |
| Partial-fill P&L | −$5.62 |
| Net simulated P&L | **$32.31** |
| Maximum drawdown | $4.34 |
| No-settlement funding scenario | $22,614.22 |
| Trade quantities | 5–6,445 shares |

There is no $1,000 capital or 1,000-share cap. Uncapped sizing does not imply
large profit: available depth and quote margins constrain returns. The funding
scenario assumes no completed-basket settlement cash is released during the
experiment and is not a measured portfolio ROI denominator.

| Arrival delay | Net simulated P&L, common deterministic policies |
| --- | ---: |
| 0 ms | $39.68 |
| 50 ms | $35.13 |
| **100 ms** | **$32.31** |
| 250 ms | $32.31 |
| 500 ms | **−$12.81** |

The identical 100/250 ms totals reflect the recorded as-of states in this sample,
not a claim that latency never matters. At 500 ms the strategy loses money under
the same selected events. Execution assumptions materially affect economic value
even when opportunity capture remains unchanged.

The 20 random N=3 policies have net P&L ranging from **$5.41 to $34.70**, with
mean **$23.23**. One random path can exceed monitor-all by avoiding a losing
trade; monitor-all is not an execution-P&L upper bound. This seed range is not a
confidence interval for live profitability.

## Concentration and inference limits

January Fed contributes **+$36.55**, while next country the US strikes contributes
**−$4.24**. Removing Fed and reranking leaves a loss; removing the other traded
event leaves $36.55. Removing any of the six nontraded events leaves the primary
known P&L unchanged. All saved models and quoted-gap retain their observed tie
in these symmetric leave-one-event-out replays. The development-events-only
scope excludes the new losing event and returns $36.55 for the same policies;
this is a scope sensitivity, not a preferred replacement headline.

The primary pool contains only five evaluation families, with observed positives
in two events. Paired resampling records zero differences between deterministic
policies on the observed outcomes. A zero observed difference or interval does
not demonstrate equivalence on unseen markets or unknown windows. Repeated
development inspection, retrospective universe selection, missing recordings
and the lack of final-period ladders limit generalisation.

The defensible conclusion is that **simple quoted-gap screening matched the ML
models in this held-out cohort at N=3**, while reducing monitoring relative to
monitor-all. The wider development study supports comparing occurrence and
quoted-value models, but this final period does not establish that neural
complexity improves event selection. The economic layer shows why opportunity
capture and achievable P&L must be discussed separately.

## Implementation and verification

Reusable components are:

- `src/data/final_evaluation.py`: explicit final access, backward-as-of features,
  preceding-scan history, coverage, occurrence labels and peak quote targets.
- `src/models/final_evaluation.py`: saved-bundle inference and references;
  no final labels enter fitted-model prediction and no fitting occurs.
- `src/simulator/final_execution.py`: separate copies of the frozen FOK and
  reservation kernels with explicit entry/exit boundaries.
- `src/simulator/final_evaluation.py`: persistent replay and consistency checks.
- `src/reporting/final_evaluation.py`: provenance checks, comparisons, uncertainty
  diagnostics, reconciled P&L curves and figures.

All 172 required raw input files were hashed before final book processing.
105 matched existing development inventories; the other 67 belong to the original
supported events without development examples and were explicitly frozen for
this run. Existing training models, metadata and historical source hashes were
preserved.

A feature-parity check identified an unseen-member denominator and first-grid
edge case before the first successful inference run. The first dataset build
was archived, the feature construction was corrected and the dataset rebuilt.
Direct comparisons verified that **all labels and peak targets were unchanged**.
The correction was required to reproduce development feature semantics; it did
not change models, targets, tuning or the frozen evaluation protocol. Parallel
forest summation differed at roughly 1e-16 between loads of the same artifact;
the verified alias reuses canonical scores to preserve identical tie handling.

**170 unit tests passed.** Saved predictions reproduce, P&L curves reconcile
with replay totals, and N=5 selection/economic equality holds. There are
**756 replay scenarios**: 540 across two scopes, two budgets, 27 policy/reference
names and five delays, plus 216 leave-one-event-out scenarios at N=3/100 ms.
The final period remains evaluated; later model changes require new validation
data or explicit exploratory reporting.

Saved results are in `results/final_evaluation/` and `results/final_replay/`.
The derived dataset is local under `data/processed/final_evaluation/`.
[Notebook 11](../../notebooks/11_final_evaluation.ipynb) displays the verified
results and figures. To reproduce into fresh directories:

```powershell
.\.venv\Scripts\python.exe -m src.data.final_evaluation --output data/processed/final_new --open-final
.\.venv\Scripts\python.exe -m src.models.final_evaluation --data data/processed/final_new --output results/runs/final_new
.\.venv\Scripts\python.exe -m src.simulator.final_evaluation --data data/processed/final_new --models results/runs/final_new --output results/runs/final_new_replay
$env:MPLBACKEND = 'Agg'
.\.venv\Scripts\python.exe -m src.reporting.final_evaluation --data data/processed/final_new --models results/runs/final_new --replay results/runs/final_new_replay
```
