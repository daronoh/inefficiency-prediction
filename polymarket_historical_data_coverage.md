# Upstream Data Issue: Incomplete Polymarket Metadata and Historical Book Coverage

## 1. Summary

We are a downstream consumer of FinData, using historical Polymarket order books
for event-level arbitrage research. We need to enumerate an event's constituent
markets, identify their outcome tokens, and obtain overlapping historical books
for the contracts in a trading basket.

Our January–June 2026 audit found two main blockers:

1. Event/market links exist, but some linked markets have no corresponding
   metadata, and some recorded tokens cannot be mapped through the available
   metadata.
2. Many candidate baskets have a required market without usable overlapping
   history in the inspected snapshot table. There are also intervals with no
   snapshot rows from any source.

We are requesting **reconciled metadata, clarification of historical coverage and
book semantics, and backfill where an archive exists**. An explicitly documented
covered subset would let us define a defensible downstream experiment.

## 2. Environment

- **Service:** FinData, accessed through the attached `FINDATA_URL` endpoint.
- **Schema:** `prediction_markets`.
- **Tables:** `polymarket_events`, `polymarket_event_markets`,
  `polymarket_markets`, `polymarket_orderbook_snapshots`.
- **Requested historical range:** `[2026-01-01T00:00:00Z, 2026-07-01T00:00:00Z)`.
- **Sources observed:** `polymarket`, `predexon`, `pmxt`, `polymarket_ws`.
- **Audit evidence collected:** 28 October 2026, UTC.
- **Report date:** 29 October 2026, Singapore time.

Findings concern the inspected warehouse tables and linked candidate cohort.
They are not an exhaustive assessment of every provider archive. The audit used
contract metadata and recording identifiers/counts/timestamps; June prices,
labels and profitability were not inspected.

## 3. Steps to Reproduce

1. Check whether linked market records exist for an affected event:

   ```sql
   SELECT
       COUNT(DISTINCT em.market_id) AS linked_markets,
       COUNT(DISTINCT m.id) AS markets_with_metadata,
       COUNT(DISTINCT em.market_id)
           FILTER (WHERE m.id IS NULL) AS missing_market_metadata
   FROM prediction_markets.polymarket_event_markets em
   LEFT JOIN prediction_markets.polymarket_markets m
       ON m.id = em.market_id
   WHERE em.event_id = '100229'
   LIMIT 1;
   ```

   Observed: **15 linked markets, four with retrieved metadata, 11 missing**.

2. Check a historical interval without retrieving book prices:

   ```sql
   SELECT COUNT(*) AS snapshot_rows
   FROM prediction_markets.polymarket_orderbook_snapshots
   WHERE snapshot_ts >= '2026-04-16T00:00:00Z'::timestamptz
     AND snapshot_ts <  '2026-05-22T00:00:00Z'::timestamptz
   LIMIT 1;
   ```

   Observed: **zero rows across all sources**. The separate interval
   `[2026-02-10T00:00:00Z, 2026-02-23T00:00:00Z)` also contained zero rows.

These examples can be run directly against the warehouse without this project's
Python code. Results may change after repairs or backfills.

## 4. Expected Behavior / Data We Need

There are two core requests:

1. **Complete event-to-market mappings and contract details.** For a selected
   event, we can retrieve all constituent markets, their rules, and the correct
   YES/NO token IDs.
2. **Historical books for the required markets.** All markets needed by a
   basket have usable book states over the same evaluation window. Each market
   needs an initial state at or before the window and observation coverage during
   it; an unchanged book can carry forward when recording completeness supports
   that interpretation. A new snapshot every hour is not required. For example,
   verified observation periods of 10:00–14:00 and 11:00–15:00 overlap from
   11:00–14:00. These are observation periods, not merely first/last update times.
   A confirmed complementary NO-book representation can supply YES liquidity;
   duplicate recordings of both sides are not required.

## 5. Actual Behavior

### Metadata gaps

The audited snapshot inventory contains **175,177,129 rows and 6,278 distinct token
IDs**. Warehouse metadata links those recordings to **924 candidate event IDs**.
Within that discovery scope:

| Observation | Count |
| --- | ---: |
| Events with at least one linked-member metadata gap | 556 |
| Event/market links without retrieved market metadata | 7,336 |
| Recorded tokens we could not connect to a market and event | 1,060 |
| Candidate event missing event-level metadata (`605797`) | 1 |

These results do not establish whether each gap reflects incomplete ingestion,
a stale link, identifier normalization, a different metadata surface, or an
unavailable upstream record. That distinction needs upstream investigation.

### Incomplete basket recordings

Of 139 negRisk candidates passing our preliminary structural metadata checks:

| Recording result across January–June | Events |
| --- | ---: |
| At least one required YES token has no direct history in the inspected interval | 119 |
| Histories exist, but no recorder is shared by all required legs | 1 |
| Shared recorder/day exists, but no qualifying overlap after listing dates | 3 |
| Pass a preliminary one-hour recording-span check somewhere in the range | 16 |


### Recording gaps and changing sources

No snapshot rows were found on **10–22 February** or **16 April–21 May**, inclusive.
PMXT's inspected recordings run from 23 February to 15 April; websocket recordings
begin on 22 May.

`ingest_ts` alone cannot establish when a historical quote was available to a
trader. First/last recording timestamps also do not distinguish a quiet market
from a recorder outage. Some recordings concern contracts nominally dated earlier
than the recording period, requiring clarification of actual trading lifetimes.

The preliminary audit uses first/last update timestamps as a conservative coverage
proxy. If unchanged books are omitted while observation continues, this can
underestimate usable history. The low passing count is provisional until that
recording behavior is clarified; an old snapshot is not automatically stale.

## 6. Downstream Impact

- We cannot reliably verify complete outcome baskets when member records or
  token mappings are unresolved.
- A market with neither a direct usable book nor a supported complementary
  representation prevents pricing the complete basket.
- Missing recording periods must remain unknown; treating them as no-opportunity
  observations would bias the experiment.
- High row counts do not establish a broad concurrently covered event universe.
  January–May currently yields only four preliminary negRisk matches and one
  supported ladder match under our checks.
- Sparse snapshots do not establish intermediate book states for a 100 ms replay.
  We need the recording semantics to choose defensible evaluation assumptions.

Contract-relationship detection, features, labels, model training and execution
simulation remain downstream responsibilities.

## 7. Suggested Investigation and Requested Resolution

1. **Repair or explain missing metadata.** Investigate the missing member records
   and token mappings, starting with event `100229`. Backfill valid missing
   records, correct stale links, or point us to the existing authoritative lookup.
2. **Identify recoverable historical coverage.** Check whether missing books and
   empty intervals exist in another table or archive. Backfill if possible;
   otherwise identify which events and historical periods are actually supported.
3. **Explain how to interpret quiet books.** Confirm whether each recorder saves
   every observation or only changes. Explain how consumers can identify periods
   when a token was still being observed, and any known interruptions. Confirm
   whether the stored YES/NO representation supports complementary reconstruction.

**Resolution evidence:** The example metadata discrepancy is resolved or
explained; a covered event/time subset is queryable; and consumers have enough
information to interpret unchanged books and recording gaps. A minimum update
frequency is not requested.

The desired deliverable is a **usable historical subset plus an explanation of
its coverage**. The upstream team can choose the appropriate tables, views,
endpoints or backfill approach. Contract validation, features, labels, model
training and execution simulation remain downstream work.
