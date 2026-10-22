# Bug Report: Polymarket Events Have NULL `market_ids`

## 1. Summary

Every row in `prediction_markets.polymarket_events` has a NULL `market_ids` column. The event→market relationship — the primary way to enumerate which markets belong to an event — is therefore not queryable from the events table, and the only working link is the reverse direction (markets embedding their parent event in a jsonb payload), which requires full-table scans that exceed query timeouts.

## 2. Environment

- **Platform:** Lumid Data Warehouse
- **Schema:** `prediction_markets`
- **Table:** `polymarket_events` (~500K rows)
- **Related tables:** `polymarket_markets` (~3.4M rows), `polymarket_orderbook_snapshots` (~188M rows)
- **Date Observed:** June 2026

## 3. Steps to Reproduce

1. Query the events table for any populated `market_ids` value:

   ```sql
   SELECT count(*) FROM prediction_markets.polymarket_events
   WHERE market_ids IS NOT NULL;   -- → 0
   ```

2. Attempt to enumerate the markets of an event from the events side:

   ```sql
   SELECT count(*) FROM prediction_markets.polymarket_events,
       jsonb_array_elements_text(market_ids)
   WHERE id = '30829';             -- → 0 rows (column is NULL)
   ```

3. Attempt the reverse (market → event) enumeration for a population query, e.g. "how many total markets does each neg-risk event have":

   ```sql
   -- lateral jsonb scan across ~3.4M market rows
   SELECT ... FROM prediction_markets.polymarket_markets m,
       jsonb_array_elements(m.raw->'events') e
   WHERE ...;                      -- → query timeout (tried 3 formulations)
   ```

## 4. Expected Behavior

- `market_ids` should contain the ids of the event's member markets, matching the event↔market links embedded in `polymarket_markets.raw->'events'`.
- Event→market enumeration should be a cheap indexed lookup, enabling coverage and completeness analyses (e.g. "does every market of this event have orderbook data?").

## 5. Actual Behavior

- `market_ids` is NULL on **all 499,511 rows**:
  - non-NULL: 0, empty array `'[]'`: 0, `jsonb_array_length > 0`: 0
- The event's `raw` payload does **not** carry the market listing either — its keys are `category, closed_time, creation_date, description, enable_order_book, end_date, icon_url, id, image_url, neg_risk, neg_risk_market_id, resolution_source, series, slug, start_date, status, tags, ticker, title`. No `market_ids` or `markets` key. (`neg_risk_market_id` is a single neg-risk market id, not the member list.)
- The only working event↔market link is `polymarket_markets.raw->'events'` (each market embeds its parent event). Whole-population enumeration of an event's market roster therefore requires a lateral jsonb scan of the full markets table, which times out.
- Concrete downstream impact: the question "how many events have ALL of their markets' orderbook snapshot data?" is **not answerable** from the warehouse — we can count per-event *snapshot* markets (up to 23 observed for events 30829 / 630631) but cannot compare against the event's true market total.

## 6. Impact

- Event-centric analytics (market coverage, completeness checks, event-level aggregation over member markets) are blocked or forced into timeout-prone workarounds.
- The reverse market→event link is a one-way jsonb dependency: an event's market list is only reconstructable by scanning all markets, and only for events that have ≥1 market row in the warehouse.
- Analysis quality degrades: results that depend on "event X has N markets" must be caveated as partial.

## 7. Suggested Investigation

- **Root cause:** confirm whether the Gamma API event payload ever includes a market listing (evidence suggests it does not — the field may be aspirational rather than a dropped value).
- **Fix option 1 (derive at ingest):** on each market upsert, append its id to its parent event's `market_ids` (merge with `||`); backfill with a quarter-sliced pass over `polymarket_markets.raw->'events'` to stay inside query limits.
- **Fix option 2 (derived table):** drop reliance on the column and maintain an explicit `event_market_links(event_id, market_id)` table as the query surface.
- **Verification after fix:**
  ```sql
  SELECT count(*) FROM prediction_markets.polymarket_events
  WHERE market_ids IS NOT NULL AND jsonb_array_length(market_ids) > 0;
  -- expect ≈ number of events with ≥1 market row
  ```
