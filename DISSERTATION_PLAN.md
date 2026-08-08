# Dissertation Work Plan — Detecting Short-Horizon Inefficiency on Polymarket

*Drafted 2026-08-06, based on a full review of this repo, the CA report (`FYP_CA_report.pdf`), the
[event-pred](https://github.com/casperngeen/event-pred) repo, and current Polymarket fee/microstructure research.
Assumes a ~14-week semester (Aug–Nov 2026) ending in thesis submission.*

---

## STATUS UPDATE (2026-08-07) — Week 1–2 executed; three findings change the plan

Week 1–2 items (§2) are **done** (repo/leakage fixes, pair panels, fee schedule; all
adversarially verified against the full 3.3M-row dataset). Three empirical findings:

1. **The intra-market YES+NO label is structurally empty.** The two books of a binary
   market are mirrored views of one order pool: `ask_NO = 1 − bid_YES` holds exactly in
   ~99.999% of aligned snapshots, so `sum of asks = 1 + spread ≥ 1` by construction.
   Only ~63 of 1.66M aligned rows violate it (capture desync, not opportunities).
   **Week 2–4 labels must therefore target cross-market structure from day one**:
   negRisk event groups (subset-NO / full-cover-YES rebalancing) and logically related
   markets. The sample supports this: 33/70 markets are negRisk and 40/70 belong to 8
   multi-market events (Pro Football Champion ×18, Fed ×4, UCL winner ×4, Iran ×4, …) —
   though group coverage is partial, so consider downloading the missing sibling
   markets of these events (a small, targeted expansion).
2. **The whole sample is fee-free: 70/70** (gamma `feesEnabled=false` for every market
   over its sampled life; the Jan-2026 crypto fees hit *serial 15-minute* markets only,
   which the sample doesn't contain). So §4's "historical fees" layer is exactly zero,
   and the fee analysis is purely counterfactual-current — state this explicitly in the
   thesis, and treat the fee-bearing-regime backtest as the forward-looking design.
3. **The mirrored-book fact strengthens the fee argument**: at the coarsest common
   tick (1¢) the mechanical minimum of `sum of asks` is $1.01, so the naive taker
   strategy needs a *cross-book desync* just to reach break-even before fees — another
   structural reason the professor's critique holds.

§4's centrepiece (cost-aware net-executable-profit labels with the attrition table)
stands, but computed on **negRisk group panels** rather than binary-pair panels; the
binary pair panels remain useful as the alignment/QA layer and for spread analytics.

---

## 1. Where the project stands

**Done:**
- CA report: research question (arbitrage discovery reframed as contract screening), literature review
  (Kalshi maker/taker economics; Saguillo et al. 2025 arXiv:2508.03474 — ~$40M realised Polymarket arbitrage;
  cross-platform calibration), proposed methodology (labels, features, models, evaluation).
- Data pipeline (`src/`, `data-pipeline/`):
  - 356,736 Polymarket markets parsed from a 1.9 GB Gamma-API JSONL → `markets.parquet` / `tokens.parquet`.
  - Top-70 closed markets by volume (end date > 2026-01-01) → 140 outcome tokens.
  - 3,322,971 full-orderbook snapshots (Dome API), 2025-10-14 → 2026-02-04, one parquet per token.
  - 34-column per-token feature tables: top-of-book prices, spread, depth (top-3 + total), imbalance,
    level counts, metadata (fee, negRisk, expiry), one-lag deltas.

**Not yet built:** the label, any model, any backtest, any plots. The core research object — the
inefficiency event — does not exist in the data yet.

---

## 2. Blocking fixes before any modelling (Week 1–2)

### 2.1 Repo bugs
| # | Fix | Where |
|---|---|---|
| 1 | `ORDERBOOK_DIR` points at `data/orderbook/` but data lives in `data/raw_orderbook/` — pipeline won't re-run as committed | `src/paths.py` |
| 2 | Rotate the Dome API key and move it to an env var (it is committed in git history) | `src/data_loader.py` |
| 3 | Delete the dead helper family in `data_loader.py` (`get_depth_top_n` has a no-op sort bug; `features.py` has the correct versions) | `src/data_loader.py` |
| 4 | `--drop-json-cols` is inert (`store_true` + `default=True`) | `run_data_loader.py` |
| 5 | Deduplicate snapshots on `(token_id, snapshot_timestamp_ms, indexed_at_ms)` (pagination overlaps survive) | feature pipeline |
| 6 | Delete `data/raw_orderbook.zip` (265 MB duplicate) | `data/` |

### 2.2 Leakage / validity fixes (these would sink the thesis in review)
| # | Problem | Fix |
|---|---|---|
| 1 | `time_to_expiry_hours` uses `closed_time` (realised resolution time — unknowable at snapshot time) | Use scheduled `end_date` only |
| 2 | `volume`, `volume_clob` are *lifetime* totals broadcast onto every snapshot (an October row carries February's final volume) | Drop them, or replace with point-in-time activity proxies (book-update intensity, depth churn) |
| 3 | Market selection conditions on `closed_time` non-null + final lifetime volume (ex-post) | Acknowledge as a scope limitation now; if data is expanded, select point-in-time (markets active at t, ranked by trailing activity) |
| 4 | `indexed_at_time` (data-availability clock) is dropped | Keep it; evaluate signals against `indexed_at` for honest "known-at" timing |
| 5 | ~8% of snapshots have an empty ask side; no crossed-book guard | Explicit null/crossed handling + a data-quality flag column |

### 2.3 Build the market-level panel (prerequisite for labels)
YES and NO tokens are captured **asynchronously** (bursty cadence: median gap 2 s within bursts,
14 s overall median, gaps up to hours). The arb condition needs both books at once:
- As-of join NO onto YES per market with a **staleness cap** (e.g. ≤ 5 s; sensitivity at 2/10/30 s).
- Also build a regular 1-minute grid (last-obs-carried-forward + `staleness_seconds` column) for modelling.
- Deliverable: `data/processed/pairs/pair_<market_id>.parquet`.

---

## 3. The fee reality (the professor's point, quantified)

Polymarket fee regime (verified Aug 2026, docs.polymarket.com/polymarket-learn/trading/fees):
- **Taker-only** fee: `fee = shares × feeRate × p × (1−p)`; makers pay 0 and earn rebates (~15–25% of taker fees).
- feeRate by category: **crypto 0.07**; sports/economics/culture/weather/other 0.05; finance/politics/tech/mentions 0.04; **geopolitics 0**.
- Timeline: zero fees before Oct 2025 → 15-min crypto (Oct 2025) → all crypto (Jan 2026) → sports (Feb 18 2026) → 8 more categories (Mar 30 2026).
- No gas (relayer-sponsored), no settlement fee, no deposit/withdrawal fee. Live per-token rate: `GET https://clob.polymarket.com/fee-rate?token_id=...`.

**Break-even for a two-leg taker YES+NO arb** (fee on both legs = `2·feeRate·p(1−p)` per $1 pair):

| Category | at p=0.50 | at p=0.10/0.90 |
|---|---|---|
| Crypto (0.07) | 3.5¢ | 1.26¢ |
| Sports/econ (0.05) | 2.5¢ | 0.9¢ |
| Politics/finance (0.04) | 2.0¢ | 0.72¢ |
| Geopolitics (0) | 0¢ | 0¢ |

Plus non-fee frictions: 1¢ typical tick (0.001/0.0025 sub-regimes), thin top-of-book (must walk the book →
VWAP at size Q), quote staleness (gaps live seconds-to-minutes), leg risk, capital lockup if held to
resolution (≈0.8–1.2 bp/day at stablecoin yields), UMA oracle tail risk.

**Two structural escape hatches** the strategy design should exploit:
1. **CTF split/merge is atomic and gasless**: buy YES+NO below $1 → *merge* into $1 USDC immediately
   (no resolution wait, no oracle risk, no lockup). Over-$1: *split* $1 → sell both legs.
2. **Maker execution pays zero fee** and earns rebates — prediction is exactly what makes maker-side
   positioning possible (you know *where* dislocation is likely before it happens, so you can rest
   quotes instead of crossing the spread).

**Critical nuance for your sample:** your 70 markets (NHL sports, tweet-count "mentions") were
**fee-free during almost your entire window** (2025-10-14 → 2026-02-04; sports fees began 2026-02-18).
The Gamma `fee_decimal = 0.02` field in your data does not match any documented feeRate — resolve it
against the live fee-rate endpoint. So: report **layered labels** — historical fees (≈0) *and*
counterfactual current fees — and let the attrition between layers be a headline result.

---

## 4. Core deliverable 1: cost-aware label construction (Week 2–4)

Replace `ask_YES + ask_NO < 1` with **net executable profit at size**:

```
π_taker(Q, t) = Q · (1 − VWAP_ask_YES(Q,t) − VWAP_ask_NO(Q,t))
              − Q · feeRate · [p_y(1−p_y) + p_n(1−p_n)]        # 0 under historical regime
```
- VWAPs from walking the raw books (`bids_json`/`asks_json` — kept in `data/raw_orderbook/`, currently
  discarded downstream; add a book-walking stage).
- Compute at Q ∈ {$10, $100, $1,000}; over-$1 side analogously with bid VWAPs (split-then-sell).
- **Persistence filter**: opportunity counts only if π > 0 for ≥ k consecutive snapshots spanning ≥ s
  seconds (k ∈ {1,2,3}, s ∈ {0,5,30}) — kills quote-flicker and staleness artifacts.
- **Labels**:
  - `Y_t = 1` if any net-profitable opportunity (at Q, persistence k) occurs in (t, t+H], H ∈ {15m, 60m}.
  - Continuous: `max π` over the horizon (ranking/regression target).

**Deliverable — the attrition table** (this alone answers the professor):

> raw sum<1 violations → after book-walking at Q → after fees (historical | current counterfactual)
> → after persistence → after opportunity cost

per market, per Q, per category. Precedents to cite: Saguillo et al. (VWAP book-walking, $0.05 ad-hoc
filter, zero-fee era) and Krause SSRN 6984101 (layered ROI: 5.08% → 4.82% → 2.67%).

**Risk gate (end of Week 3):** if net-of-current-fee events are near-zero in this sample, that is a
*publishable finding* ("2026 fees eliminated intra-market taker arbitrage in these categories") — pivot
the prediction target to the historical-fee labels + the maker-side strategy, and say so explicitly.

---

## 5. Descriptive chapter (Week 4–6)

First plots of the thesis (there are currently none):
- Spread/depth/imbalance distributions; cadence and staleness structure.
- Where do net opportunities cluster: time-to-expiry, price level (tails vs midpoint — note fees are
  parabolic in p, so tails are fee-cheap), spread regime, volatility, book thinness, category.
- Event studies around dislocations: what does the book look like 5 min before?
- Hazard-style analysis: opportunity duration (how long does π > 0 persist? → defines the realistic
  reaction window and motivates *prediction* over *scanning*).

---

## 6. Core deliverable 2: screening models (Week 5–8)

- **Feature enrichment** (all point-in-time): rolling mid volatility (5m/30m/2h), quote-update intensity,
  book slope (depth-weighted distance from touch), microprice, distance-to-arb (`ask_YES + ask_NO − 1`),
  recent inefficiency counts (self-exciting), pair-level joint features, staleness features.
- **Splits**: temporal walk-forward (e.g. 4 folds over the 3.7 months); no shuffling; embargo gap ≥ H.
- **Models** (in order): always-0 baseline → threshold rules (e.g. distance-to-arb < 2¢ and spread ≤ tick)
  → logistic regression → gradient boosting (LightGBM). Class weighting for rarity.
- **Metrics**: PR-AUC, precision@k, top-k hit rate, and the *economic* metric — % of total net profit
  captured while monitoring only the top-k of contracts (the search-efficiency frontier vs exhaustive
  and random-k scanning). Report per category and per fold.

---

## 7. Core deliverable 3: spatio-temporal graph model via event-pred (Week 8–11)

Honest scoping of [casperngeen/event-pred](https://github.com/casperngeen/event-pred) ("stg"):
- **What it is**: a strategy-pattern *graph-dataset construction* framework — builds `(T,N,F)` node-feature
  tensors + `(T,N,N)` adjacency from market data, with sliding-window/event-driven temporal slicing,
  KNN/cosine/shared-attribute/semantic-title (SentenceTransformer) edge strategies, and forward-looking
  label builders. Serialisation + masks for dynamic node sets included.
- **What it is not**: there is **no GNN, no training loop, no eval** (torch/torchcde are unused declared
  deps); all built-in adapters are **Kalshi trade-stream** based; it has **no orderbook ingestion**.
- **Reuse plan**: keep the scaffolding (GraphBuilder, temporal strategies, label pattern, semantic edges —
  they work on Polymarket question text unchanged). Write: a Polymarket column-mapping adapter (~1 day),
  a `PolymarketOrderbookNodes` strategy computing your spread/depth/imbalance features per window
  (~2–3 days), and the model + training + eval stack yourself (~1–2 weeks for a first baseline).
- **Model**: start minimal — GCN or GAT layer(s) + GRU over window embeddings, predicting the Phase-4
  cost-aware label per node. PyTorch Geometric. 140 nodes is tiny — this is fast to train.
- **Edges**: same-event/negRisk-group (hard logical coupling), semantic similarity of questions,
  same-category. Ablate each.
- **Research question it answers**: *does cross-market structure add predictive power over per-contract
  features?* Run a 1-week pilot first (Week 8); if the GNN can't beat LightGBM, report it as a clean
  ablation finding — that is still a contribution, and the framing protects you either way.

---

## 8. Core deliverable 4: the "practically workable strategy" backtest (Week 10–13)

Convert the screener into a strategy and evaluate net of *all* frictions:
1. **Taker strategy**: monitor model's top-k contracts; when net π(Q) > threshold θ, execute both legs at
   book VWAP + fees, exit via CTF merge. Latency haircut: fill at the book Δ seconds later, Δ ∈ {0, 2, 10, 60}.
2. **Maker strategy (stretch)**: in predicted-dislocation contracts, rest the passive leg at fee-zero;
   requires a fill model → needs a **trades feed** (Polymarket Data-API `/trades` or Dome). If trades
   can't be fetched in time, bound it: assume fills only when a later snapshot shows the book trading
   through the quoted price, and label the result an upper/lower bound. Do not claim more.
3. Report: net P&L by layer (gross → after spread/VWAP → after fees → after latency), capital usage and
   ROI vs ~4–5% APY stablecoin benchmark, results under historical vs counterfactual current fees,
   per category, per fold. Compare monitored-subset economics vs exhaustive scanning cost.

---

## 9. Data expansion (parallel track, only if Weeks 1–4 go well)

> **DATA REGIME (2026-08-08, supersedes earlier window notes):** the markets JSONL is a
> **resolved-markets-only dump** (every record `closed: true`, last resolution ~Feb 4 2026).
> Consequences: (1) every universe market's full life-to-resolution fits inside a download
> window ending 2026-03-31 — "entire orderbook history till resolution" is guaranteed;
> (2) nothing in the universe lived past Feb 18, so NO fee transition is in-sample for
> event strata (fee analysis is fully counterfactual there); the `serial` stratum is the
> sole genuinely fee-bearing subsample (serial crypto fees since ~Oct 2025; 568 markets
> with feesEnabled=true); (3) winner-take-all groups whose events concluded after the dump
> are missing their surviving markets: **48/50 negRisk groups are complete (winner present)**;
> incomplete: Pro Football Champion 2026 (finalists missing) and UEFA CL Winner (contenders
> missing) — restrict full-cover long-side labels to the 48; short-side subset labels remain
> valid on all 50, or fetch the few missing siblings from live Gamma as a targeted supplement.
> `data/` was reset to: the JSONL, the 5 universe token lists, `event_universe.parquet`
> (the frozen market→event→stratum mapping), and `fee_schedule_universe.parquet` (contains
> the non-reproducible Aug-8 live CLOB fee snapshot). Everything else regenerates from these.
>
> **DECISION (2026-08-08): the universe is FINAL as selected from the existing JSONL** —
> no metadata re-scrape; the study window stays 2025-10-14 → 2026-02-04 and the fee
> analysis is explicitly counterfactual (no in-sample fee transition). The optional
> `data-pipeline/refresh_markets_jsonl.py` (Gamma keyset scraper, no API key needed)
> exists unused if the extended-window diff-in-diff is ever wanted.
>
> **Download runbook** (needs `DOME_API_KEY` set; rotate the old leaked key first).
> 88 pilot tokens are already on disk; `--skip-existing` skips them and makes every
> command below resumable after interruption — just re-run it:
> ```
> python data-pipeline/run_data_loader.py --token-ids-path data/processed/universe_token_ids_negrisk.parquet --skip-existing
> python data-pipeline/run_data_loader.py --token-ids-path data/processed/universe_token_ids_ladder.parquet --skip-existing
> python data-pipeline/run_data_loader.py --token-ids-path data/processed/universe_token_ids_control.parquet --skip-existing
> python data-pipeline/run_data_loader.py --token-ids-path data/processed/universe_token_ids_serial.parquet --skip-existing
> ```
> Then rebuild features and pairs over the expanded set:
> ```
> python data-pipeline/run_feature_pipeline.py --filtered-tokens-path data/processed/universe_token_ids.parquet
> python data-pipeline/run_pair_panel.py --filtered-tokens-path data/processed/universe_token_ids.parquet
> ```
> `fee_schedule_universe.parquet` (per-market fee reference for all 2,227 markets) is
> built independently of the downloads.
>
> **STATUS (2026-08-07): selector built and run** — `data-pipeline/select_event_universe.py`
> selected 105 complete event groups + 752 serial crypto markets (2,227 markets / 4,454
> tokens; est. ~9.5M snapshots likely, 70.8M worst case, ~2–16 GB): 50 negRisk events,
> 30 ladders, 25 seeded controls, 3-day BTC/ETH serial slice. Token lists are staged
> per stratum in `data/processed/universe_token_ids_<stratum>.parquet`; download order:
> negrisk → ladder → control → serial. Requires `DOME_API_KEY` (rotate the old key
> first). Selection uses lifetime volume only to cap the download universe — re-select
> point-in-time within analyses and use the control stratum to quantify residual bias.

Current sample = 70 closed, high-volume sports/mentions markets over 3.7 months → selection-biased and
category-narrow. If time permits (Dome history starts 2025-10-14; budget API time):
- Add **crypto** markets (fee-bearing since Jan 2026 — real in-sample fee regime!) and **geopolitics**
  (permanently fee-free — the naive strategy's last habitat). This enables a genuine cross-regime
  comparison instead of only a counterfactual.
- Extend the window past 2026-02-18 so sports fees are in-sample too.
- Point-in-time selection: markets active at t with trailing-window activity ranking.
- Fetch per-token feeRates from the live endpoint and build a dated fee schedule table.

---

## 10. Writing plan and repositioned contribution (Week 12–14)

Thesis contributions, restated to absorb the professor's critique:
1. **Friction anatomy**: first layered attrition analysis of intra-market Polymarket arbitrage under the
   2025–26 fee transition (raw → executable → net-of-fee → persistent → net-of-opportunity-cost).
2. **Cost-aware labelling framework**: net-executable-profit-at-size labels with persistence, replacing
   the naive sum-of-quotes definition (which the thesis now *shows* is uninvestable in fee-bearing categories).
3. **Screening models**: rules → linear → GBM → spatio-temporal GNN, evaluated on the search-efficiency
   frontier (profit captured per contract monitored).
4. **A workable strategy**: merge-exit taker execution where net-positive; prediction-guided maker
   positioning as the fee-era design; full net-of-friction backtest with latency haircuts.

Chapter skeleton: Intro → Background & fee regime → Data & panel construction → Label construction &
friction attrition (core) → Descriptive analysis → Models → Strategy backtest → Robustness → Conclusion.

### Week-by-week summary
| Weeks | Work |
|---|---|
| 1–2 | Repo fixes, leakage fixes, pair panel, fee schedule |
| 2–4 | Book-walking VWAP, cost-aware labels, **attrition table**, risk gate |
| 4–6 | Descriptive chapter + plots |
| 5–8 | Features, walk-forward, baselines → LightGBM, economic metrics |
| 8 | event-pred pilot (adapter + orderbook nodes + tiny GCN) |
| 9–11 | Graph model + ablations |
| 10–13 | Strategy backtest (taker + maker-bound), robustness |
| 12–14 | Writing, figures, cleanup |

Fallbacks: if the GNN pilot stalls → GBM + graph-*features* (neighbor aggregates) instead of a GNN.
If net-of-fee events are too rare → historical-fee labels as primary + counterfactual as the punchline.
If trades data is unobtainable → drop maker fill claims, keep the maker design as discussion.
