# DISSERTATION_PLAN — agent briefing

*README.md is the human user guide; this file carries research context,
established findings, binding decisions, and the work queue. Last rewritten
2026-08-08; decisions below supersede anything older in git history.*

## 1. Project context

NUS CP4101 final-year dissertation. Research question: **can observable market state predict which
Polymarket winner-take-all event groups will exhibit short-horizon, net-of-cost
pricing inefficiency — so monitoring effort can concentrate on a shortlist?**
(“Contract screening”, not brute-force arbitrage scanning.)

The thesis answer: a layered friction (“attrition”) analysis, cost-aware labels, screening models,
and a maker-side / merge-exit strategy design that does not pay taker fees.

## 2. Established empirical findings (verified; do not re-derive)

1. **Mirror books.** A binary market's YES and NO books are one order pool:
   `ask_NO = 1 − bid_YES` exactly (99.999% of 1.66M aligned rows). Intra-market
   `BUY_YES + BUY_NO < 1` is structurally impossible (sum of asks = 1 + spread).
   All inefficiency labels are therefore **cross-market** (negRisk groups).
2. **The metadata dump is resolved-markets-only**, frozen ~2026-02-04 (every
   record `closed: true`; outcomePrices are final). Every universe market's
   full life-to-resolution is observable; nothing lived past 2026-02-04+ε.
3. **Fees.** Polymarket taker fee = `shares × feeRate × p(1−p)`; makers pay 0
   (+ rebates). Rollout: serial 15-min crypto ~Oct 2025 → all crypto Jan 2026
   (serial scope only in practice) → sports 2026-02-18 → most categories
   2026-03-30; geopolitics permanently 0. **All 949 negRisk / 350 ladder / 176
   control markets have gamma `feesEnabled=false` — the study sample is
   fee-free.** Only 568 of 752 serial markets carried fees. Live CLOB
   `base_fee` is retroactive current config — NOT historical fee state.
4. **Group completeness: 48/50 negRisk groups contain their winner**
   (`supports_full_cover` in event_universe.parquet). Incomplete (event
   concluded after the dump): Super Bowl 2026, UEFA CL Winner → short-side
   labels only (decision “option 2”, 2026-08-08).
5. **Frictions cheat sheet**: tick mostly $0.01 (min quoted spread); CTF
   split/merge is atomic + gasless (merge-exit → no lockup/oracle risk);
   gas ≈ $0 (relayer); two-leg taker break-even = `2·feeRate·p(1−p)`;
   opportunity cost ≈ 0.8–1.2 bp/day if held to resolution; UMA oracle tail
   risk only on hold-to-resolution.
6. **Counterfactual fee analysis caveat** (write into thesis as Limitations):
   the sample is zero-fee-era; applying today's fees is partial-equilibrium
   (Lucas critique). Signed conservative via limits-to-arbitrage; mitigations:
   threshold-sweep evaluation, maker-side design pays no fees, serial stratum
   as the one fee-era observation.
7. **event-pred repo** (github.com/casperngeen/event-pred): graph-*dataset*
   scaffolding only (T,N,F tensors, temporal windowing, semantic edges). No
   models/training inside; Kalshi trade-stream adapters; no orderbook support.
   Reuse = scaffolding; any GNN work is greenfield.

## 3. Scoping decisions (binding)

- **negRisk stratum is the study.** Ladder = extension chapter (time
  permitting), control = selection-bias robustness table, serial = optional
  fee-era microstructure validation (single binaries — cannot produce
  rebalancing labels). Serial download deferred.
- **Universe is FINAL (v1.1, 2026-08-08)**, selected from the frozen JSONL (no
  re-scrape): 50 negrisk + 30 ladder + 25 control events (complete groups,
  event-size ≤ 35, volume floor $1M, control seed 42, match-style events
  excluded from ladder) + 752 serial markets = 2,238 markets / 4,476 tokens.
  Study window 2025-10-14 → 2026-03-31 (all markets resolve by ~Feb 4).
  v1→v1.1: a semantic audit caught 4 LoL BO5 prop bundles in ladder (same
  template, no implication structure) → `is_match` exclusion added, replaced
  by 4 genuine ladders; negrisk/control/serial unchanged.
- **Stratification verification** (2026-08-08, three layers — rerun any time):
  (1) `data-pipeline/verify_universe.py` re-derives the rules from the raw
  JSONL: disjointness, raw negRisk flags, group completeness, quotas/floors,
  independent top-50 ranking, template rule, serial slice, full-cover flags;
  (2) an independent agent re-implemented the selection from spec without
  reading the code — exact match (530 eligible negrisk events, top-50
  identical); (3) a semantic audit read every selected event. Residual audit
  findings for label code (Stage 2/ladder extension):
  * 10 price events are TWO-sided (reach + dip chains) — implication holds
    within a side only; never pair across sides.
  * `what-price-will-bitcoin-hit-in-2025` contains a re-listed duplicate rung
    (two markets, same question "dip to $100,000") — dedupe by question, keep
    the later listing.
  * 18/50 negrisk events are weekly Elon-tweet buckets with overlapping
    windows — not independent observations; account for this in
    leave-event-out folds (hold out the whole template family) and in any
    independence claims.
  * Control stratum is 22/25 sports/esports prop bundles and includes 3
    treatment-like structures — describe it honestly, not as a neutral
    baseline.
- **Group signals** (negRisk, N sibling markets, YES books only):
  `long_gap = 1 − Σ best_ask_i` (full-cover groups only);
  `short_gap = Σ best_bid_i − 1` (valid on any subset / incomplete groups —
  NO basket on subset S guarantees ≥ |S|−1).

## 4. Evaluation protocol (binding — fixed BEFORE label work)

- **Lockbox holdout 2026-01-08 → 2026-02-04**: touched once, at the end, for
  headline out-of-sample model + backtest results. No exploration, label
  tuning, or feature design may use it. Lockbox start may only move later.
- **Development sample 2025-10-14 → 2026-01-07**: all Stage 2–4 exploration;
  label definitions (sizes Q, persistence k, horizon H) frozen here.
- **Walk-forward folds inside development** (e.g. train ≤Nov 30 / test Dec
  1–21; train ≤Dec 21 / test Dec 22–Jan 7), 1-day embargo (≥ label horizon).
  Hyperparameters on inner folds only. No shuffling; scalers fit on train only.
- **Leave-event-out check** within development: separates transferable
  microstructure signal from group memorization (report both). Known caveat:
  contemporaneous cross-group correlation makes it optimistic — it is the
  decomposition exhibit, never the headline. Event-random 80/20 as the PRIMARY
  split was considered and rejected (2026-08-08): overlapping lifetimes leak
  calendar/regime information across the split and erase drift.
- **Lockbox reporting**: split metrics by groups-seen-in-development vs
  groups-first-appearing-in-lockbox (the latter is out-of-time AND
  out-of-event — the strictest available test).
- First thing any label code does: tag rows `development` / `lockbox`.

## 5. Data inventory

Irreplaceable: `data/polymarket_markets_1y.jsonl` (1.9 GB frozen dump — user
should keep an offsite copy) and `data/processed/fee_schedule.parquet` (holds
the one-time 2026-08-08 live CLOB fee snapshot). Frozen selection:
`event_universe.parquet` + `universe_token_ids[_stratum].parquet`. Everything
else (markets/tokens tables, ob_*.parquet, feat_*.parquet, pair panels)
regenerates from those via the scripts. Orderbooks re-download with
DOME_API_KEY (user-held; old committed key is leaked — must stay rotated).
Data lives in the MAIN checkout; from a worktree set
`DATA_ROOT=C:/Users/Daron/coding/inefficiency-prediction/data`.
Venv python: `C:/Users/Daron/coding/inefficiency-prediction/.venv/Scripts/python.exe`.

## 6. Pipeline status (updated 2026-08-10)

### DONE — built, run on real data, adversarially verified
- Stage 0: negrisk orderbooks downloaded (1,872/1,898 tokens; 13 markets have
  no Dome coverage — 12 Bolivia pre-window + 1 LoL, tracked via
  `members.parquet.has_data`). Features (40 cols incl. VWAP-at-size
  {10,100,1000} shares) for all downloaded tokens.
- Stage 1: group panels for all 50 negrisk events
  (`processed/groups/{event,grid_1m}/`, members map). Verification: an
  independent re-implementation matched a full event panel on ALL rows/cols
  and every gating rule across all 100 panels. Known semantic caveats:
  **F1** backward as-of never expires dead members' quotes — 94.8% of dev
  non-null long_gap event-rows are >30s stale; ALWAYS condition on
  freshness/cleanliness downstream. **F2** valid_group flags skip unborn
  members' null staleness (2,932 dev rows flagged fresh on partial groups) —
  long_gap is protected by its own gate; short-side consumers beware.
- Stage 1 data-sanity verdicts: sum-of-asks center is coherent (median 1.021);
  headline top-of-book rates halve under the two-sided-book gate
  (>1c: 0.83%→0.44% of fresh full-cover rows) and the surviving hits
  concentrate in 2 Fed events (89% of clean hits) — leave-event-out will be
  fragile, report honestly. Resolution-transition phantoms are fully removed
  by the clean-book gate; market-open seeding transients exist on the short
  side (dust value); persistence must be seconds-based, never snapshot-count.
- Stage 2: `src/labels.py` + `run_labels.py` — executable net-gap spells
  (long side gated on `all_books_clean`), seconds-based persistence,
  counterfactual fee sweep via `fee_basis`, grid labels
  (`y_long_{q}_{15m,60m}`, `max_gap_100_60m`), development/lockbox tagging,
  attrition table. VERIFIED (2026-08-10): independent re-implementation
  matched every spell/label/attrition cell; three design findings fixed
  same-day — spells now end at the FLIP row (books unchanged between
  event-grid rows, so opportunity life runs to the next update), short
  spells gated on contributing-member cleanliness (short_clean_q), and a
  60m label embargo + straddle clip at the dev/lockbox boundary.
  Post-fix development attrition (2025-10-14→2026-01-07): 518 executable
  spells @10sh → 405 @100sh → 344 persistent ≥5s → 58/38/26 after
  counterfactual fees 0.04/0.05/0.07 (8 events survive the harshest
  regime). Dev label base rate y_long_100_60m ≈ 0.63% (~12.5k positive
  grid rows). Short side even fair-gated stays dust at the median
  (0.2c) — attrition exhibit only, not a model target for now.
  Column semantics: DATA_DICTIONARY.md.

### NEXT (in order)
1. **Adversarial verification of Stage 2** (spell detection, label windows,
   attrition math) — gate before any modeling, same pattern as Stage 1.
2. **Descriptive chapter** (development data only): gap anatomy, spell
   duration/hazard, concentration, near-resolution behavior, per-event
   heterogeneity; first thesis figures.
3. **Models**: (group, minute) unit on labels_grid; baselines → logistic →
   LightGBM; walk-forward folds + embargo per §4; PR-AUC, precision@k,
   profit-capture frontier; leave-event-out decomposition (hold out template
   families together — the Elon-tweet weeks are one family).
4. **`run_backtest.py`**: taker + merge-exit at VWAP, latency haircuts
   Δ ∈ {0,2,10,60}s, layered net results, capital usage; maker variant as
   explicit bound; benchmarks monitor-all / random-k. Lockbox opened once,
   at the very end, metrics split by seen/new groups.
5. Parallel (user): ladder + control downloads (same command, swap the list);
   serial optional.

Chapter mapping: labels+panels → Methodology; attrition+descriptive →
Empirical anatomy; models → Screening; backtest → Strategy; ladder/control/
serial → Robustness; §2.6 caveat → Limitations.

## 7. Working conventions for agents

- Verify against real data; prefer adversarial review for nontrivial changes
  (workflow pattern used at commits b63f7f1, 848cf7d).
- No lookahead: any new feature must be computable from information available
  at snapshot time (`indexed_at` is the honest availability clock; scheduled
  `end_date` only, never `closed_time`; no lifetime volume).
- Commit per coherent unit on this branch; messages explain research rationale.
- Timeline: panels+labels ≈ wk 2–4, descriptive 4–6, models 5–8, backtest
  10–13, writing from 12. Semester ends ~Nov 2026.
