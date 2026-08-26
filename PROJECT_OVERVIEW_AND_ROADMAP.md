# Predicting Short-Horizon Market Inefficiency on Polymarket

## Project goals, current progress, and remaining work

**Core task:** Predict which Polymarket event groups are most likely to become inefficient within a fixed future window, so a selected subset can be monitored for arbitrage more effectively than a randomly selected subset.

---

## 1. Executive summary

Polymarket contains many simultaneously active markets, making it undesirable to treat every event as equally worthy of continuous arbitrage attention. The purpose of this project is therefore not merely to detect an inefficiency after it appears. It is to build a **screening system** that predicts where an inefficiency is most likely to appear next.

At a decision time \(t\), the proposed system uses information available at or before \(t\) to assign each eligible Polymarket event group a risk score. The score estimates the probability that the group will develop a book-clean, depth-executable pricing inconsistency during a short future horizon, currently one hour. The event groups with the highest scores are then shortlisted for intensive monitoring. “Depth-executable” here is conditional on the historical carried-forward-book assumption; it is not yet a claim of guaranteed simultaneous fills.

The central practical comparison is:

- **Model strategy:** intensively monitor the model's top-\(k\) event groups.
- **Random baseline:** intensively monitor \(k\) event groups chosen randomly from the same eligible universe at the same time.

Both strategies must select the same number of event groups and use the same information timing, monitoring period, execution assumptions, and capital constraints. The final question is whether the model-selected shortlist captures more executable opportunities and produces greater net simulated profit than the distribution of outcomes from repeated random selection.

Intermediate findings—such as evidence that inefficiencies exist, useful feature relationships, good precision-recall performance, or high precision at \(k\)—are important. However, they are not the final practical result. The strongest result would be that the complete model-led strategy produces **positive net returns and a statistically credible improvement over an equally sized random-subset baseline on data quarantined from feature, model, and strategy decisions**.

The project has already completed the difficult data and label-construction stages for the core negRisk sample. It has not yet trained prediction models or run the model-versus-random economic backtest.

### 1.1 Fixed decisions and unresolved blockers

| Fixed before modelling | Still to be frozen before model comparison |
|---|---|
| Core negRisk sample and four-stratum frozen universe | Point-in-time eligibility rule for each event-minute |
| Cross-market long-gap and short-gap formulas | Direct-exchange `snapshot_time` versus Dome `indexed_at` availability convention |
| Development/lockbox calendar split | Point-in-time broad-universe screening inputs and refresh cadence |
| Main target semantics: clean 100-share long spell, at least 5 seconds, starting within 60 minutes | Primary shortlist size, ranking cadence, and shortlist duration |
| Alternative sizes and 15-minute horizon as secondary robustness targets | Model tuning objective and the one predeclared headline budget |
| Walk-forward temporal validation with embargo | Multi-leg execution, partial-fill, capital, and latency rules |

---

## 2. The problem being solved

### 2.1 Motivation

Previous work has shown that prediction markets can contain systematic distortions and that arbitrage-like opportunities have occurred on Polymarket. The systematic distortion evidence discussed in the original report came partly from Kalshi, while the direct Polymarket evidence concerned realised arbitrage; mechanisms documented on Kalshi cannot be assumed to transfer unchanged to Polymarket. Most existing work is retrospective: it identifies or measures an inconsistency once it is already visible in historical data. This project addresses a different question:

> **Can currently observable market state predict which Polymarket event groups will exhibit an executable pricing inefficiency in the next fixed time window?**

The distinction between detection and prediction is essential:

- **Detection** asks whether an opportunity exists now and generally requires the relevant books to be observed and checked continuously.
- **Prediction or screening** asks which event groups deserve that expensive continuous observation during the next period.

The intended system is therefore an allocation mechanism for monitoring capacity. It does not predict which real-world outcome will win, nor does it claim that Polymarket is broadly or permanently inefficient.

### 2.2 Screening and focused monitoring

The original monitoring burden is **continuous, detailed arbitrage surveillance of every eligible event group**. The model does something useful if it can rank the full universe and identify a smaller subset for focused monitoring. Its primary value is demonstrated when that subset captures more future opportunity value and produces more net simulated profit than an equally sized random subset.

The dissertation should therefore describe the intended system as two tiers:

```text
Broad event universe
        |
        v
Periodic, lightweight feature refresh
        |
        v
Model risk score and event ranking
        |
        v
Top-k event-group shortlist
        |
        v
Continuous full-depth monitoring and execution checks
        |
        v
Detected opportunities and simulated trades
```

A shortlist remains useful even when broad-universe data must first be collected to produce the ranking. The research evaluates the quality and economic value of the allocation decision, rather than requiring an end-to-end measurement of computational savings.

The final methodology must therefore state:

- which point-in-time information is refreshed for all candidates;
- how often the ranking is recomputed;
- what monitoring and trading rules are applied to shortlisted groups; and
- how many event groups are selected at each ranking time.

The primary shortlist budget should therefore be expressed clearly as \(k\) event groups. Because event groups vary in size, a market- or token-matched comparison may be included as a secondary robustness check.

---

## 3. Precise terminology and unit of analysis

| Term | Meaning in this project |
|---|---|
| **Outcome token** | A tradable YES or NO token belonging to one binary market. |
| **Binary market** | A single proposition, such as “Will Candidate A win?” It has complementary YES and NO outcomes. |
| **Event group** | A collection of related binary markets, such as one market for every possible election winner. |
| **negRisk event group** | A Polymarket winner-take-all group in which the members represent mutually exclusive outcomes and, when fully covered, exactly one member wins. |
| **Observation** | One event group at one decision time, currently represented on a one-minute wall-clock grid. |
| **Opportunity spell** | A maximal continuous period during which a specified executable gap remains positive after the required data-quality and cost rules. |
| **Monitoring budget** | The number of event groups, \(k\), selected for intensive monitoring at each ranking time. |

The core prediction unit is an **event-group/time observation**, not an individual YES/NO market.

---

## 4. What counts as market inefficiency

### 4.1 Why the original within-market target changed

The original FYP report proposed checking whether buying YES and NO in one binary market cost less than \$1, or whether selling both produced more than \$1. Subsequent order-book analysis showed that Polymarket's YES and NO books are complementary views of the same underlying order pool:

\[
\text{ask}_{NO} = 1 - \text{bid}_{YES}.
\]

This equality held for 99.999% of approximately 1.66 million aligned rows examined. Consequently, a supposed within-market buy-YES-plus-buy-NO opportunity is structurally ruled out by the spread. The economically meaningful inefficiencies in the core study must therefore be **cross-market inconsistencies within a negRisk event group**.

### 4.2 Long-side, full-cover opportunity

Suppose a winner-take-all event has \(N\) mutually exclusive and collectively exhaustive member markets. Buying \(q\) YES shares in every member guarantees a total payout of \(q\) dollars because exactly one member resolves YES.

Using executable volume-weighted prices rather than a single best quote, the long-side gap at size \(q\) is:

\[
\text{exec\_long\_gap}_{q}
= 1 - \sum_{i=1}^{N}\text{VWAPAsk}_{i,q}.
\]

A positive value means the complete outcome basket can be bought for less than its guaranteed \$1-per-share combined payout. This signal is valid only when:

- the event group is known to be fully covered;
- every member has usable order-book data;
- every member can fill the requested quantity \(q\); and
- the contributing books pass the cleanliness rules.

The intended exit is a merge of the complete outcome set back into collateral. More importantly, acquiring all \(N\) order-book legs is not atomic: partial fills and quote movement before the basket is complete remain major execution risks.

### 4.3 Short-side, subset opportunity

The corresponding short-side lower-bound gap is:

\[
\text{exec\_short\_gap}_{q}
= \sum_{i \in S}\text{VWAPBid}_{i,q} - 1,
\]

where \(S\) is the set of members fillable at size \(q\). The subset structure gives a payoff bound even if the frozen data does not contain every possible event outcome. Current development results show that short-side spells are numerous but generally tiny, so they are retained as an attrition result rather than the main model target.

### 4.4 Costs, persistence, and data quality

An apparent top-of-book gap is not automatically a tradable profit. The implemented labels account for several layers:

- **Depth:** executable VWAP is calculated at 10, 100, and 1,000 shares.
- **Taker fees:** the counterfactual fee for a leg at price \(p\) is based on \(q \times r \times p(1-p)\), where \(r\) is the fee rate.
- **Persistence:** a qualifying opportunity must remain open for a minimum number of seconds; the central label currently uses five seconds.
- **Book quality:** empty-sided and crossed books are excluded from long opportunities, and contributing short-side members have their own cleanliness gate.
- **Freshness sensitivity:** member staleness is recorded and a strict 30-second variant is reported, rather than silently assuming every carried-forward quote is equally reliable.
- **Latency and slippage:** these have not yet been applied in the final strategy backtest and remain outstanding work.

The implemented counterfactual net gap is:

\[
\text{net gap}_{q,r}
= \text{executable gap}_{q}
- r \sum_i p_i(1-p_i).
\]

This expression is a **per-share** net gap. The corresponding total basket value at trade size \(q\) is:

\[
q\left[\text{executable gap}_{q}-r\sum_i p_i(1-p_i)\right].
\]

An opportunity spell begins when this net gap becomes positive under the applicable cleanliness rules and normally ends at the first following update at which the condition flips. If the final observed row remains positive and there is no later flip, the stored spell is truncated at the last observed timestamp.

---

## 5. The prediction task

### 5.1 Current primary target

For event group \(g\) at decision time \(t\), the fixed main target definition is `y_long_100_60m`:

\[
y_{g,t}=1
\quad\text{if a qualifying 100-share long-side spell starts in }(t,t+60\text{ minutes}],
\]

and \(y_{g,t}=0\) otherwise.

A qualifying spell for this label is:

- long-side and full-cover;
- fillable from displayed depth at 100 shares across all members under the carried-forward-book convention;
- clean under the implemented book-quality gate;
- positive before fees, matching the historically fee-free core sample; and
- persistent for at least five seconds.

The pipeline also generates 15-minute labels, 10- and 1,000-share variants, and `max_gap_100_60m`, which records the largest qualifying gap beginning within the next hour. These support robustness checks and a possible value-based or regression formulation.

The current spell-based design deprioritises that raw count because change-triggered snapshots and repeated rows within one continuous opportunity would make it highly dependent on sampling frequency. If a count target is revived as a secondary task, it should count **unique qualifying spell starts**, not inefficient snapshots.

The label records a **new spell start** in the future interval. It does not indicate that an opportunity is already active at time \(t\), and it does not directly equal profit obtainable after execution delay. The primary backtest should therefore credit only unique spells that start after the group enters the shortlist; already-active spells at selection time must not be counted as newly captured. Any alternative “opportunity active at any point in the window” policy would require a separately declared label and should be a secondary analysis.

### 5.2 Point-in-time eligibility

The stored one-minute label table contains all 50 negRisk panels, including four groups for which the long gap is permanently disabled: two groups lack a winner in the frozen dump, and the Bolivia and League of Legends groups have incomplete Dome coverage. It also contains periods before all required member states are observable. These rows cannot simply be treated as ordinary negatives, because a model could obtain trivial lift by learning which groups are structurally unable to produce the target.

Before training, a point-in-time rule `eligible(g,t)` must be frozen and applied identically to model fitting, prediction metrics, heuristic selectors, and random selection. At minimum, it should require that a complete tradable group is identifiable at \(t\), the required broad-tier features are available, and the long-basket target is structurally possible. The four permanently disabled groups should be excluded from the primary long-side population.

`supports_full_cover` is based on whether the eventual winning market is present in the frozen resolved-only dump. It is valid as a **retrospective sample-quality filter**, but it must not be supplied to a live model as though the eventual winner were known. A deployment design instead needs a point-in-time source for event membership and negRisk completeness.

There are 46 effectively covered long-side groups across the full observed sample. Only 44 contribute rows during development; across the stored development table, the permanent-coverage filter leaves those 44 groups and 1,699,115 rows. All 12,562 positive `y_long_100_60m` rows remain, giving an eligible-population prevalence of approximately 0.7393%, compared with 0.6315% in the unfiltered all-panel table.

### 5.3 Model input and output

The model should use only information genuinely available at or before time \(t\). Candidate feature families include:

- current spreads, depth, imbalance, and VWAP capacity;
- distance from current group-level consistency bounds;
- recent quote changes, volatility, update intensity, and staleness;
- cross-member dispersion and liquidity concentration;
- group size and scheduled time to expiry;
- event category or template-family indicators, used carefully to avoid memorisation; and
- low-frequency market metadata that would have been known at the decision time.

Final resolution prices, realised closing times, lifetime volume, and any other ex-post fields must not be used as model features. Scheduled end date, category, and event membership also come from the resolved final-state Gamma dump; they are point-in-time safe only if they were not revised after the historical observation. Their history must be verified or reconstructed, otherwise a strict model ablation should exclude them. The token files retain `indexed_at`, which represents when Dome data became available, but current group construction aligns on `snapshot_time` and drops the ingestion clock after duplicate resolution. The final design must state whether it simulates direct exchange availability or later data-provider availability. If Dome availability is the deployment assumption, group panels, labels, and attrition results must be rebuilt and rechecked on `indexed_at` rather than described as already availability-safe.

The model output should be a comparable score for every eligible event group at a decision time. A ranking is more directly aligned with the operational goal than a universal probability threshold: the system needs to identify the best \(k\) groups for the shortlist, not classify every row independently.

---

## 6. Research questions and hypotheses

### Primary research question

**RQ1.** Can observable, point-in-time market state predict which Polymarket negRisk event groups will develop a book-clean, depth-executable long-side pricing inefficiency in the next hour, and does that signal survive conservative availability and execution assumptions?

### Practical research question

**RQ2.** At an equal monitoring and capital budget, does intensively monitoring a model-selected top-\(k\) subset capture more opportunity value and earn more net simulated profit than monitoring a random subset of the same cost?

### Generalisation question

**RQ3.** Does predictive performance persist out of time and, more strictly, on event groups or template families not represented during training?

### Friction question

**RQ4.** How much apparent inefficiency survives realistic trade size, book quality, persistence, fees, freshness, latency, and capital constraints?

### Explanatory question

**RQ5.** Which observable market characteristics are associated with elevated short-horizon inefficiency risk, and are the relationships stable across event types and market regimes?

The corresponding main hypotheses are:

- model top-\(k\) selections have higher future-opportunity precision and capture than repeated random-\(k\) selections;
- the resulting execution simulation produces greater net profit per unit of monitoring than random selection;
- nonlinear models may improve ranking over simple heuristics and logistic regression, but only if the gain survives temporal and event-family validation; and
- a large proportion of raw price gaps will disappear after executable-depth and execution-friction filters.

---

## 7. Study scope and data

### 7.1 Frozen study universe

The current frozen universe contains 2,238 binary markets and 4,476 outcome tokens:

| Stratum | Event groups | Binary markets | Outcome tokens | Role in the dissertation |
|---|---:|---:|---:|---|
| negRisk | 50 | 949 | 1,898 | Core winner-take-all study and model target |
| Ladder | 30 | 361 | 722 | Optional extension for logical/implication inconsistencies |
| Control | 25 | 176 | 352 | Selection-bias and descriptive robustness comparison |
| Serial | 752 | 752 | 1,504 | Optional fee-era microstructure validation; not a group-rebalancing target |
| **Total** | **857** | **2,238** | **4,476** | — |

The negRisk core consists of the top 50 eligible event groups by event volume, subject to a \$1 million volume floor, at least three members, and at most 35 members. Forty-eight contain their winning member in the frozen metadata and theoretically support a complete long basket. After missing Dome coverage is considered, 46 groups have complete effective data for long-side labels.

The ladder, control, and serial strata are frozen for later extensions, but their order books have not yet been downloaded.

### 7.2 Time coverage and split

- **Nominal order-book download request:** 14 October 2025 to 31 March 2026.
- **Effective frozen metadata/universe endpoint:** approximately 4 February 2026; every record in the dump is resolved.
- **Actual stored snapshot and label coverage:** 14 October 2025 to 3 February 2026, ending at approximately 08:22 UTC.
- **Development period:** 14 October 2025 to 7 January 2026.
- **Lockbox period:** 8 January 2026 to 4 February 2026.

The lockbox is not literally untouched: deterministic panel/label generation has already materialised 226,419 lockbox label rows, and project verification notes report mechanical checks of the stored label outputs. The defensible restriction is that lockbox outcomes must not be inspected or used for feature design, model selection, strategy design, threshold choice, or hyperparameter tuning. Before the final performance run, the complete specification should be committed or hashed and subsequent lockbox access logged.

Only 10 groups have any stored lockbox rows. After removing the two lockbox groups without effective long-side coverage, the final eligible lockbox contains eight groups: six also observed during development and two first appearing in the lockbox. This is a severe effective-sample limitation and must accompany the headline result.

Within development, models will use expanding-window, walk-forward validation with at least a one-day embargo. Scalers and other learned transformations must be fitted on training data only. A leave-event/template-family-out analysis will separately measure whether the model learns transferable microstructure rather than memorising particular event groups.

### 7.3 Data lineage

The implemented data flow is:

```text
Frozen Gamma metadata dump
        -> flattened market and token tables
        -> reproducible frozen event universe
        -> Dome order-book snapshots
        -> per-token order-book features
        -> aligned negRisk event-group panels
        -> depth-executable opportunity spells under the stored-book convention
        -> one-minute forward prediction labels
        -> future model matrix and ranked selections
        -> future strategy backtest
```

The raw Gamma dump contains 356,736 resolved markets and is not reproducible from the current live platform state. It should remain backed up offsite. Most derived artifacts are reproducible, although re-downloading order books consumes API quota and the stored live CLOB fee snapshot is time-specific.

---

## 8. Work completed so far

### 8.1 Research scope and target correction — complete

The project has moved from the broad contract-level formulation in the March 2026 FYP report to a more defensible event-group screening problem. The mirror-book finding ruled out the original within-market YES-plus-NO target and established negRisk cross-market inconsistency as the core study.

The project now has:

- a precise event-group unit of analysis;
- executable, size-aware gap definitions;
- cost and book-quality rules;
- fixed development and lockbox periods; and
- a clear intended comparison between ranked monitoring and random monitoring.

### 8.2 Universe construction and validation — complete

The frozen four-stratum universe has been selected from the 1.9 GB Gamma JSONL rather than repeatedly scraped from a changing live platform. The selection code is deterministic and a verifier re-derives key rules from the raw dump.

Validation completed to date includes:

- disjointness of the strata;
- raw negRisk-flag checks;
- whole-group membership checks;
- quota, volume-floor, and group-size rules;
- an independent top-50 negRisk ranking;
- ladder-template and match-exclusion checks;
- serial-slice checks;
- full-cover flags; and
- a manual semantic audit of selected events.

The semantic audit identified several issues that are now documented for later analysis: two-sided price ladders must not be joined across directions, one duplicate Bitcoin rung needs deduplication, and 18 weekly Elon-tweet events belong to an overlapping template family rather than representing independent observations.

### 8.3 Core negRisk order-book collection — substantially complete

The repository contains 1,872 of the 1,898 selected negRisk token histories, totalling approximately 9.46 million order-book snapshots. The 26 missing token files correspond to 13 markets: 12 members of a Bolivia election group and one League of Legends Worlds member. This does not prevent analysis of the remaining 46 effectively covered long-side groups, but it removes two otherwise full-cover events and may be a non-random form of sample loss.

No ladder, control, or serial histories have yet been downloaded. This does not prevent core analysis of the remaining negRisk sample, but it narrows that sample and blocks the planned extension and robustness chapters.

### 8.4 Per-token features — complete for downloaded negRisk tokens

Every downloaded raw token file has a corresponding 40-column feature file. Implemented feature families include:

- best bid, best ask, midpoint, spread, and displayed sizes;
- top-three and total depth on both sides;
- top-three and total order-book imbalance;
- number of levels and empty/crossed-book quality flags;
- executable VWAP at 10, 100, and 1,000 shares;
- one-update changes in bid, ask, and midpoint;
- time between snapshots;
- scheduled time to expiry and its clipped log transform; and
- the Dome ingestion timestamp retained for a future availability-safe reconstruction.

Leakage-prone Gamma fields such as final outcome, realised close time, and lifetime volume are not included in these token feature tables.

### 8.5 Group-panel construction — generated for all 50 negRisk events under the current timing convention

Under the current exchange-`snapshot_time` convention, the member token books have been backward-as-of aligned into:

- 50 event-time panels containing a row whenever any captured member updates; and
- 50 one-minute wall-clock source panels. These still require a fixed-width feature stage and a point-in-time eligibility rule before modelling.

The panels include member-level features, staleness, group quote sums, theoretical and depth-executable gaps, fee bases, cleanliness gates, and freshness flags. Project verification notes report that an independent reimplementation matched an entire stored event panel and the gating rules across all panels. The verification code/report is not retained as a repeatable repository test, and an availability-clock change would require reconstruction and renewed verification.

Two known caveats remain important downstream:

- carried-forward member states can be old; 94.8% of development rows with a non-null long gap have a maximum staleness above 30 seconds; and
- some generic group-freshness flags ignore unborn members, so downstream work must use the long-gap coverage gate and the appropriate short-side contributor gate rather than treating a freshness flag alone as proof of tradeability.

### 8.6 Fee analysis — complete for the core conclusion, but the stored table needs refresh

The project established that all 949 negRisk markets in the core sample were fee-free while alive. Therefore, zero-fee labels describe the historical sample, while current category fee rates are counterfactual sensitivity tests.

The fee formula and rollout timeline have been encoded, and a one-time live CLOB snapshot was stored on 8 August 2026. Its “current” CLOB fields mean current on that build date, not on this document's status date. `fee_schedule.parquet` also predates universe version 1.1: it has 2,227 rows, is missing 33 new ladder markets, and contains 22 obsolete ladder rows. The negRisk rows are complete, so the core fee conclusion remains supported, but the schedule must be reconciled before ladder or full-universe analysis.

### 8.7 Opportunity spells and prediction labels — implemented and reviewed under the snapshot-time convention

The label pipeline now produces:

- maximal long- and short-side opportunity spells;
- displayed-depth VWAP sizes of 10, 100, and 1,000 shares;
- counterfactual fee rates of 0, 0.04, 0.05, and 0.07;
- persistence measured in elapsed seconds rather than snapshot count;
- 15- and 60-minute forward labels;
- a continuous future maximum-gap target;
- development/lockbox tags;
- a 60-minute embargo at the split boundary; and
- development-only friction attrition.

The stored `labels_grid.parquet` contains 2,215,679 event-minute rows: 1,989,260 development rows and 226,419 lockbox rows. The main development target, `y_long_100_60m`, contains 12,562 positive rows. Its prevalence is 0.6315% in the stored all-panel denominator and approximately 0.7393% after the permanent long-coverage filter described in Section 5.2. Either population is strongly imbalanced, so ordinary accuracy would be misleading; the filtered population should be used for the primary model and baseline.

Project verification notes report that the label implementation was adversarially reviewed and independently reproduced. Corrections from that review include ending non-terminal spells at the first flip row, applying short-side cleanliness to the actual contributing members, clipping spells at the development/lockbox boundary, and removing development labels whose horizon crosses into the lockbox. The independent verifier is not retained as an automated test, and an `indexed_at` reconstruction would need to be reverified.

### 8.8 Temporal validation protocol — fixed before modelling

The following decisions are already binding:

- no random shuffling as the main validation method;
- walk-forward development folds with an embargo at least as long as the label horizon;
- hyperparameter tuning inside training folds only;
- training-only scaling and preprocessing;
- a leave-event/template-family-out decomposition;
- one final lockbox **performance** evaluation after the specification is frozen; and
- separate lockbox reporting for event groups seen during development and groups first appearing in the lockbox.

This protocol provides major controls for temporal drift, overlapping label windows, group memorisation, and leakage. It does not by itself resolve the availability clock, dynamic event membership, primary budget, or execution-policy questions listed above.

---

## 9. Current intermediate findings

All figures in this section are development-sample descriptive results. They are **not model performance and not evidence that the final strategy beats its baseline**.

### 9.1 Attrition from apparent gap to more credible opportunity

| Definition | Long-spell count under definition | Event groups represented |
|---|---:|---:|
| Executable at 10 shares, historical fee 0 | 518 | 11 |
| Executable at 100 shares, historical fee 0 | 405 | 11 |
| Executable at 1,000 shares, historical fee 0 | 219 | 8 |
| 100 shares and persistent for at least 5 seconds | 344 | 10 |
| 100 shares, at least 5 seconds, fee rate 0.04 | 58 | 9 |
| 100 shares, at least 5 seconds, fee rate 0.05 | 38 | 9 |
| 100 shares, at least 5 seconds, fee rate 0.07 | 26 | 8 |
| 100 shares, at least 5 seconds, maximum staleness at most 30 seconds | 22 | 6 |

The code redetects maximal spells separately for each size and fee condition. The rows are therefore definition-specific counts, not a one-to-one cohort in which exactly the same spells are followed through every layer. Even with that qualification, the table shows that the count of apparent opportunities falls sharply as displayed depth, persistence, fees, and strict freshness are imposed.

### 9.2 Concentration and rarity

- The sum of best asks is generally coherent, with a documented median near 1.021 rather than below 1.
- Requiring clean, two-sided books roughly halves the rate of top-of-book gaps above one cent.
- Two US Federal Reserve event groups account for approximately 89% of the surviving clean top-of-book hits above one cent.
- Only 10 development event groups contain persistent 100-share long spells.
- The main one-hour target has a positive rate of about 0.63% despite containing nearly two million development rows.

These facts imply that the effective sample is much smaller than the row count suggests. Results may be driven by a few related events, and uncertainty should be clustered by event or template family rather than calculated as if every event-minute were independent.

### 9.3 Short side

The clean, 100-share, five-second short-side definition produces 1,498 development spells across 46 events, but the median maximum gap is only 0.002, or 0.2 cents per share before additional execution frictions. The short side is therefore useful in the empirical attrition chapter but is not currently the preferred prediction target.

---

## 10. Work that remains

### Phase 1 — Finish the development-only empirical foundation

1. **Build the fixed-dimensional model-input table.**  
   `labels_grid.parquet` contains time, event ID, and targets, while group panels contain variable numbers of member-prefixed columns. A new leakage-safe aggregation stage is needed to create one fixed feature vector for each event-minute and join it to the forward label.

2. **Resolve availability-time semantics.**  
   Decide whether the simulated system observes direct exchange `snapshot_time` or Dome `indexed_at`. Apply the decision consistently to feature alignment, selection time, labels, and latency. If `indexed_at` is chosen, rebuild and reverify the panels, spells, labels, and attrition table.

3. **Define the shortlist protocol precisely.**  
   Freeze `eligible(g,t)`, the primary decision cadence, shortlist duration, and value of \(k\). Predeclare one headline configuration before model comparison, then treat alternate shortlist sizes as sensitivity curves. A token- or book-matched comparison may be included as a secondary check because event sizes differ.

4. **Define the point-in-time screening feature set.**  
   Specify which features are available for every eligible group at each ranking time and how frequently they are refreshed. The central requirement is that the features are historically available and leakage-safe; they may include order-book-derived information from across the broad universe.

5. **Complete the descriptive chapter using development data only.**  
   Produce figures and tables for gap distributions, spell durations and hazards, time to expiry, liquidity and depth attrition, event concentration, category/template heterogeneity, and freshness sensitivity.

6. **Reconcile stale documentation and fee metadata.**  
   Refresh the v1.1 fee-schedule membership without losing the original non-reproducible live snapshot, and update outdated universe and pipeline counts in the supporting documentation.

### Phase 2 — Implement screening baselines and models

7. **Implement an equally sized random-subset baseline.**  
   At each decision time, sample from exactly the same eligible event groups and select the same number of groups as the model. Repeat the draw across many fixed seeds to characterise selector luck on this historical path rather than relying on one lucky or unlucky subset. This randomisation distribution is a benchmark, not by itself a confidence interval for future performance.

8. **Implement simple, inexpensive heuristic baselines.**  
   Useful comparisons include liquidity/activity ranking, smallest current consistency gap, largest recent quote movement, or other transparent rules. These help establish whether a complex model adds value beyond an obvious screening rule.

9. **Train an interpretable statistical baseline.**  
   Begin with regularised logistic regression. Use class weights or another training-only rare-event treatment where justified, and report calibrated probabilities if probability estimates will drive selection.

10. **Train a nonlinear model.**  
   LightGBM or another gradient-boosted tree model is the planned next step. Tune it only inside the walk-forward development protocol. A GNN is optional greenfield work, not a prerequisite and not currently implemented in this repository.

11. **Analyse model behaviour.**  
    Report feature effects or importance, calibration, failure cases, event concentration, and performance stability across folds. Confirm that success is not solely memorisation of the two Fed groups or the overlapping Elon-tweet family. Because overlapping one-hour labels can represent the same future spell many times, report event-balanced metrics, unique-spell capture, and a non-overlapping decision-window sensitivity alongside row-level metrics.

### Phase 3 — Translate rankings into an economic strategy

12. **Implement `run_backtest.py`.**  
    The backtester must simulate selective visibility: future books may be used to score truth offline, but a strategy may detect and act on an opportunity only if the relevant group was shortlisted before it began.

13. **Specify execution rules before final comparison.**  
    Freeze the entry availability clock, trade size, entry condition, leg ordering or atomicity assumption, partial fills, failed baskets and unwind losses, depth consumption, quote reuse, minimum order sizes, concurrent exposures, reserved capital, merge latency/failure, opportunity de-duplication, and whether overlapping shortlist windows can trigger repeated action.

14. **Apply latency and cost scenarios.**  
    Reprice every required leg after delays of 0, 2, 10, and 60 seconds and apply the frozen partial-fill rule; merely checking whether the original gap remains positive is not enough. Report historical zero-fee results and clearly labelled counterfactual fee sweeps. Treat maker-side results as a bound unless fill probability and queue position are modelled.

15. **Compare economic outcomes at an equal shortlist size.**  
    Compare model top-\(k\), repeated random-\(k\), simple heuristics, and monitor-all as a coverage reference. The same value of \(k\), selection duration, capital assumptions, and execution engine must be used for every shortlist selector.

### Phase 4 — Robustness and extensions

16. **Run leave-event and leave-template-family checks.**  
    Hold related weekly Elon-tweet groups together. Use these results as a transferability decomposition, not as the headline replacement for the temporal test.

17. **Download and process the ladder and control strata.**  
    Ladder events support an optional logical-implication extension. The control sample supports a limited selection-bias comparison, although it is dominated by sports/esports prop bundles and is not a neutral population baseline.

18. **Use the serial stratum if time permits.**  
    Serial crypto markets cannot generate the group-rebalancing label, but they provide the only planned observation of genuinely fee-bearing market microstructure.

19. **Add repeatable tests and environment dependencies.**  
    The repository currently relies heavily on documented adversarial reimplementations. Retain critical invariants as automated tests and declare the modelling and plotting packages needed for reproducibility.

### Phase 5 — Final lockbox and dissertation

20. **Freeze and record all remaining choices.**  
    Keep the primary 100-share, five-second, 60-minute target semantics fixed; finalise the feature implementation, eligibility rule, model, hyperparameters, budget, cadence, execution, fee scenario, and headline metrics using development data alone. Commit or hash the full specification.

21. **Run the quarantined lockbox performance evaluation once.**  
    The lockbox is already mechanically materialised, but it has not yet been used for model or strategy performance. Apply the committed specification, log access, and report results for all eligible groups, the six groups seen during development, and the two groups first appearing in the lockbox.

22. **Complete the dissertation.**  
    Expand the three-paper preliminary literature review and recheck publication/bibliographic details. Then integrate the motivation, data construction, market mechanics, friction attrition, descriptive results, screening models, economic backtest, robustness, limitations, and conclusion.

---

## 11. Required evaluation design

### 11.1 Fair selector comparison

At each ranking time \(t\):

1. Construct the eligible event-group set using only information available by \(t\).
2. Compute each method's score without using future information.
3. Give every method the same shortlist size, \(k\).
4. Select the model's top-ranked groups, the heuristic groups, and repeated random groups.
5. Keep each selection active for the same monitoring interval.
6. Allow the common execution engine to act only on groups assigned to that selector.
7. Deduplicate opportunity spells and account for the same capital constraints.
8. Compare results using paired times and the same underlying opportunity set.

The same frozen `eligible(g,t)` mask must be used in training, metrics, and every selector. The primary \(k\), ranking cadence, shortlist duration, and model-tuning metric must be predeclared; alternate shortlist sizes and cadences are secondary analyses.

Random selection should be repeated enough times to form a **conditional random-selector distribution**. The model should be compared with its mean and relevant upper quantiles, not merely with one draw. This distribution measures luck in random selection on the one observed history; it is not sampling uncertainty about future markets. Inference for model-versus-baseline differences should additionally use a paired date-block/event-family bootstrap, a clustered permutation procedure, or another method that respects the small number of events and their dependence.

### 11.2 Prediction and ranking metrics

| Metric | Why it matters |
|---|---|
| Precision-recall AUC | Appropriate global ranking measure for a target with approximately 0.63% positives |
| Precision at \(k\) | Fraction of shortlisted event observations followed by a qualifying opportunity |
| Opportunity recall/capture at \(k\) | Fraction of all future spells captured by the selected \(k\) groups |
| Lift over random at \(k\) | Direct concentration improvement over an equally sized random subset |
| Calibration | Whether a stated risk of \(x\)% corresponds to approximately \(x\)% observed frequency |
| Profit-capture frontier | Share of available opportunity value captured as the monitoring budget grows |

Accuracy should not be a headline metric, because a model predicting “no opportunity” everywhere would exceed 99% accuracy. ROC-AUC may be reported but is less informative than precision-recall and top-\(k\) behaviour in this rare-event setting.

Row-level metrics also need qualification: one future spell can make many preceding one-minute rows positive. Report unique-spell capture, event-balanced or macro metrics, and a non-overlapping decision-window sensitivity so one opportunity or long-lived group is not counted as many independent successes.

### 11.3 Economic and operational metrics

The final result should include:

- gross and net cumulative P&L;
- P&L per monitored event-hour and per monitored token-hour;
- number and value of unique executable spells captured;
- return on deployed or reserved capital;
- capital utilisation and maximum simultaneous exposure;
- sensitivity to shortlist size, latency, trade size, and fee regime;
- downside or drawdown statistics where meaningful;
- improvement over the conditional distribution of repeated random baselines; and
- paired model-minus-baseline uncertainty intervals formed with date blocks and/or event-family clusters.

The dissertation should distinguish three different claims:

1. **Predictive lift:** the model ranks future opportunities better than chance.
2. **Economic lift:** the model-led strategy earns more than an equally sized random-subset baseline.
3. **Absolute viability:** the strategy remains non-negative after the chosen realistic costs and latency.

Beating random while still losing money is evidence of predictive information, but not yet evidence of a practically workable trading strategy.

---

## 12. Main risks and limitations

### 12.1 Resolved-only and selected sample

The metadata dump contains resolved markets and is frozen around 4 February 2026. The core universe is also the top 50 eligible negRisk groups by ex-post lifetime volume. The study is therefore a strong retrospective proof of concept for a selected universe, not automatically a representative estimate for every live Polymarket event. The control stratum and a point-in-time deployment discussion can mitigate, but not eliminate, this limitation.

### 12.2 Historical fees do not match the current platform

The core negRisk sample was fee-free. Applying today's fee schedules to these historical books is a partial-equilibrium counterfactual: if fees had actually existed, liquidity providers and traders might have behaved differently. Counterfactual thresholds should therefore be reported as sensitivity analysis, not as observed historical net profit. A maker-side strategy avoids taker fees but introduces uncertain fills and adverse selection.

### 12.3 Quote freshness and data-provider timing

Dome snapshots are change-triggered. A carried-forward quote may genuinely represent an unchanged live book, but a long silence may also reflect missing updates. The sharp fall from 344 persistent 100-share spells to 22 under a strict 30-second freshness cap shows that the interpretation materially affects the results.

### 12.4 Displayed liquidity is not guaranteed execution

Historical order books show displayed depth, not guaranteed simultaneous multi-leg fills. The backtest cannot fully observe queue position, market impact, adverse selection, or whether every leg would fill before another participant acts. Latency haircuts, conservative fill rules, and separate maker/taker bounds are necessary.

### 12.5 Rare events and dependence

There are millions of event-minute rows but only 50 core event groups, 46 with complete effective long-side coverage, and 10 development groups with persistent 100-share opportunities. The final lockbox has only eight effective long-side candidate groups. Overlapping one-hour labels, repeated minutes, and template-related events create strong dependence. Uncertainty estimates must operate at event, template-family, or time-block level, and any confidence claim must acknowledge the very small number of clusters.

### 12.6 Concentration and transferability

Two Fed events dominate the clean opportunity set. A model can appear successful by recognising one event family rather than learning a transferable market mechanism. Temporal performance, leave-family-out performance, and explicit concentration tables are required.

### 12.7 Dynamic event membership

Historical group panels use the final set of known event members. A live system must know which markets currently belong to an event and when new members are listed. The dissertation should explain how membership would be maintained without using future knowledge.

### 12.8 Prediction target versus obtainable value

The binary label values every qualifying spell equally. A small opportunity that disappears immediately and a large opportunity with ample depth both produce the same label. The continuous maximum-gap target and profit-aware ranking should be explored as secondary approaches, while keeping the predeclared binary label for a clean comparison.

### 12.9 Operational efficiency is a secondary benefit

Selecting fewer event groups may reduce the operational burden of continuous arbitrage surveillance, execution checks, analyst attention, and capital allocation. This is a useful potential benefit, but it is not a condition for research success and does not need to be converted into an end-to-end computational-cost estimate. The primary evidence remains whether the model-selected top-\(k\) subset captures more opportunity value and earns more net simulated profit than repeated random subsets of the same size.

---

## 13. Definition of project success

The project should be judged at three levels.

### Level 1 — Methodological contribution

The project provides a reproducible historical pipeline with major leakage controls that converts Polymarket order books into event-group panels, depth-executable opportunity spells under a carried-forward-book assumption, and short-horizon prediction labels. It also quantifies how apparent arbitrage is reduced by depth, persistence, fees, data quality, and freshness. The availability-time and dynamic-membership audits remain outstanding.

This level is largely achieved for the core negRisk sample under the current `snapshot_time` convention; it must be rechecked if `indexed_at` is chosen.

### Level 2 — Predictive contribution

On walk-forward development folds and the final quarantined lockbox performance run, a model should deliver stable lift over random and simple heuristics at fixed shortlist sizes. The lift should not disappear when dominant events or template families are held out.

This level has not yet been tested.

### Level 3 — Practical contribution

Using the same shortlist size, monitoring interval, execution engine, and capital assumptions, the model-selected shortlist must capture more net opportunity value and earn more than the repeated random baseline. Under the revised objective, it must also remain profitable under one predeclared, clearly justified realistic latency and cost configuration.

This is the project's main outstanding result and should be the headline conclusion, whether the evidence is positive or negative.

---

## 14. Recommended immediate sequence

The shortest path from the current pipeline to the main dissertation result is:

1. Freeze the availability clock, point-in-time eligibility rule, screening feature set, primary shortlist size, shortlist duration, and ranking cadence.
2. Build the fixed-width, point-in-time event-minute feature matrix and rebuild labels if the chosen clock requires it.
3. Produce the development-only descriptive figures and concentration analysis.
4. Implement repeated random-\(k\) and simple heuristic selectors.
5. Train and validate logistic regression.
6. Train and validate LightGBM.
7. Implement the common, spell-deduplicated execution backtester.
8. Compare model, heuristic, random, and monitor-all profit-capture frontiers on development folds.
9. Complete leave-event/template and cost/latency sensitivity checks.
10. Commit/hash the full specification and run the quarantined lockbox performance evaluation once.
11. Download and analyse extension strata only where they strengthen the core dissertation without delaying the main result.
12. Write the final chapters around the distinction between apparent gaps, executable opportunities, predictable opportunities, and economically captured opportunities.

---

## 15. Suggested dissertation structure

1. **Introduction and research question** — prediction-market context, monitoring problem, research gap, contributions, and hypotheses.
2. **Literature review** — prediction-market efficiency, documented arbitrage, market microstructure, rare-event prediction, and learning-to-rank or resource-allocation evaluation.
3. **Polymarket mechanics and data** — market hierarchy, negRisk, mirror books, data sources, universe construction, and leakage risks.
4. **Methodology** — token features, group alignment, executable gaps, costs, spell construction, labels, and temporal validation.
5. **Empirical anatomy of inefficiency** — attrition, persistence, size, freshness, concentration, and heterogeneity.
6. **Screening models** — baselines, logistic regression, boosted trees, calibration, feature interpretation, and walk-forward results.
7. **Strategy and economic backtest** — top-\(k\) monitoring, random comparison, execution assumptions, latency, fees, capital, and profit-capture frontier.
8. **Robustness and extensions** — leave-family-out results, seen/new lockbox groups, control/ladder/serial evidence where available.
9. **Limitations and discussion** — selection, fees, data timing, execution realism, generalisability, and practical deployment considerations.
10. **Conclusion** — whether inefficient event groups are predictable and whether prediction produces a useful monitoring and trading advantage.

---

## 16. Current status at a glance

| Component | Status | Current evidence |
|---|---|---|
| Research question and core scope | Complete | negRisk event-group screening, one-hour horizon |
| Frozen study universe | Complete | 2,238 markets, 4,476 tokens across four strata |
| Core negRisk order books | Substantially complete | 1,872/1,898 tokens; 13 markets without Dome coverage |
| Per-token feature pipeline | Complete for downloaded core data | 9.46M rows, 40 columns |
| negRisk group panels | Generated under `snapshot_time` | 50 event and 50 one-minute source panels; eligibility/availability audit pending |
| Opportunity spells | Implemented and reviewed under `snapshot_time` | displayed-depth, fee-, quality-, and persistence-aware |
| Forward prediction labels | Implemented and reviewed under `snapshot_time` | 15/60-minute labels; availability-clock recheck may be required |
| Development attrition analysis | Initial table complete | broader descriptive and availability sensitivity work remains |
| Descriptive chapter and figures | Not complete | analysis still to be produced |
| Fixed-width model feature table | Not complete | required before training |
| Random and heuristic baselines | Not complete | central comparison not yet implemented |
| Logistic/boosted-tree models | Not complete | no training code or model results |
| Economic backtest | Not complete | no model-versus-random P&L result |
| Ladder/control/serial order books | Not started | frozen selections only |
| Final lockbox performance evaluation | Not run | labels are mechanically materialised; no model/strategy result yet |
| Final dissertation | In progress | methodology foundation exists; empirical model/strategy chapters remain |

In one sentence: **the project has built and reviewed the historical data-to-label foundation under its current timing convention, but it still needs to finalise point-in-time eligibility and availability, turn the labels into a ranked shortlist, and prove through a fair, quarantined backtest whether that shortlist is more valuable and profitable than an equally sized random subset.**

---

## 17. Documentation reconciliation notes

The following older statements should not be carried into the dissertation without correction:

- The March 2026 FYP report describes preprocessing, synchronized quotes, and labels as future work; these are now substantially complete for negRisk groups.
- The original report's within-binary-market YES/NO arbitrage target has been superseded by the empirical mirror-book result and the cross-market negRisk target.
- `README.md` still reports the earlier universe of 2,227 markets and 4,454 tokens. The current frozen artifact contains 2,238 markets and 4,476 tokens.
- `README.md` describes group panels as upcoming and omits the implemented group and label stages.
- An older version of `DISSERTATION_PLAN.md` recorded Stage 2 as independently verified while retaining verification as its first “NEXT” item. The revised plan removes that stale queue entry.
- `fee_schedule.parquet` still reflects the earlier universe. Its negRisk coverage supports the core fee conclusion, but the ladder membership must be updated before extension analysis.
- The pair-panel code exists, but there are currently no stored pair-panel Parquet outputs. This is not a blocker because pairs are diagnostic and the core labels use event-group panels.
- The 99.999% mirror-book result is recorded from an earlier 1.66-million-row analysis, but the supporting pair outputs are not currently retained; the dissertation should preserve a reproducible table or verification artifact.
- An older version of `DATA_DICTIONARY.md` stated that 11.4% of feature rows have negative scheduled time to expiry. The revised dictionary records the current corpus value: 562,832 of 9,460,388 rows, or approximately 5.95%.
- An older dictionary statement that every one-row spell has zero duration predated the flip-row correction. The revised dictionary now records that a non-terminal one-row true run lasts until the next update; only a terminal one-row run with no later flip can have zero stored duration.
- The stored all-panel development prevalence for `y_long_100_60m` is 0.6315%; after excluding permanently long-disabled groups it is approximately 0.7393%. The modelling denominator must be stated explicitly.
- The current Python requirements file includes the data stack but not the modelling, plotting, or testing dependencies that the remaining work will require.
