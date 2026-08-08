# DISSERTATION_PLAN — agent briefing

*Audience: a coding agent working on this repo. Read this fully before touching
anything. README.md is the human user guide; this file carries research context,
established findings, binding decisions, and the work queue. Last rewritten
2026-08-08; decisions below supersede anything older in git history.*

## 1. Project context

NUS CP4101 final-year dissertation (Daron Oh, supervisor Prof. Lu Yao, thesis
due ~Nov 2026). Research question: **can observable market state predict which
Polymarket winner-take-all event groups will exhibit short-horizon, net-of-cost
pricing inefficiency — so monitoring effort can concentrate on a shortlist?**
(“Contract screening”, not brute-force arbitrage scanning.)

Supervisor's standing critique (must be answered head-on): *a plain YES+NO
strategy is impossible once fees are considered; work harder on analysis and
modelling to derive a practically workable strategy.* The thesis answer: a
layered friction (“attrition”) analysis, cost-aware labels, screening models,
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
- **Universe is FINAL**, selected from the frozen JSONL (no re-scrape): 50
  negrisk + 30 ladder + 25 control events (complete groups, event-size ≤ 35,
  volume floor $1M, control seed 42) + 752 serial markets = 2,227 markets /
  4,454 tokens. Study window 2025-10-14 → 2026-03-31 (all markets resolve by
  ~Feb 4).
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
  microstructure signal from group memorization (report both).
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

## 6. Pipeline status and specs

Built and adversarially verified: paths / data_loader / features / pairs /
selector / fee schedule (see README for usage). Downloads: **user-blocked** —
negrisk stratum first (1,898 tokens), then ladder, control; serial deferred.

To build (in order), once negrisk orderbooks are on disk:

1. **`src/groups.py` + `data-pipeline/run_group_panel.py`** — generalize
   pairs.py from 2 tokens to N sibling markets per event: as-of align YES-book
   feature streams onto event grid + wall-clock 1-min grid (ceil start to
   minute); per-market staleness + validity caps {2,5,10,30}s; quality-flag
   passthrough; `long_gap`/`short_gap` columns; respect `supports_full_cover`.
   Output `data/processed/groups/{event,grid_1m}/group_<event_slug>.parquet`.
2. **`src/labels.py` + `run_labels.py`** — executable labels: walk raw book
   JSON (`data/raw_orderbook`) for VWAP at Q ∈ {$10,$100,$1,000} per leg;
   net gap; persistence filters (k snapshots / s seconds sensitivity);
   counterfactual fee threshold sweep (0→4¢, today's category rates marked);
   binary target Y=1 if net-executable opportunity in (t, t+H], H ∈ {15m,60m},
   + continuous max-gap; **attrition table** (headline exhibit); rows tagged
   development/lockbox.
3. **Descriptive notebook(s)** — gap anatomy on development data only.
4. **Models** — (group, minute) unit; baselines → logistic → LightGBM;
   PR-AUC, precision@k, profit-capture frontier; protocol §4.
5. **`run_backtest.py`** — taker + merge-exit at VWAP, latency haircuts
   Δ ∈ {0,2,10,60}s, layered net results, capital usage; maker variant as
   explicit bound; benchmarks: monitor-all, random-k.

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
