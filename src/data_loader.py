import json
import numpy as np
import pandas as pd

import json
import asyncio
import aiohttp
import random
from pathlib import Path

# Define a helper function to safely parse fields that are JSON-encoded strings.
def parse_json_list(value):
    # If the value is already a list, return it directly.
    if isinstance(value, list):
        return value
    
    # If the value is missing, return an empty list.
    if value is None:
        return []
    
    # If the value is a string, try to parse it as JSON.
    if isinstance(value, str):
        # Remove surrounding whitespace.
        value = value.strip()
        
        # Return an empty list for blank strings.
        if value == "":
            return []
        
        # Parse the string as JSON.
        return json.loads(value)
    
    # For anything unexpected, return an empty list.
    return []


# Define a helper function to convert fee strings into decimal form.
def parse_fee_decimal(value):
    if value is None:
        return np.nan
    try:
        s = str(value).strip()
        if s == "":
            return np.nan

        # If already a decimal string like "0.02"
        if "." in s or "e" in s.lower():
            return float(s)

        # If integer-like and likely wei-scaled
        i = int(s)
        return i / 1e18 if i > 1 else float(i)
    except Exception:
        return np.nan


# Define a helper function to safely convert values to float.
def safe_float(value):
    # Return NaN if the value is missing.
    if value is None:
        return np.nan
    
    # Try to cast the value to float.
    try:
        return float(value)
    except Exception:
        return np.nan


# Define a helper function to compute best bid from a list of bid levels.
def get_best_bid(bids):
    # Return NaN if there are no bids.
    if not bids:
        return np.nan
    
    # Return the maximum bid price across all levels.
    return max(float(level["price"]) for level in bids)


# Define a helper function to compute best ask from a list of ask levels.
def get_best_ask(asks):
    # Return NaN if there are no asks.
    if not asks:
        return np.nan
    
    # Return the minimum ask price across all levels.
    return min(float(level["price"]) for level in asks)


# Define a helper function to get the size at the best bid.
def get_best_bid_size(bids):
    # Return NaN if there are no bids.
    if not bids:
        return np.nan
    
    # Find the bid level with the highest price.
    best_level = max(bids, key=lambda level: float(level["price"]))
    
    # Return its size as float.
    return float(best_level["size"])


# Define a helper function to get the size at the best ask.
def get_best_ask_size(asks):
    # Return NaN if there are no asks.
    if not asks:
        return np.nan
    
    # Find the ask level with the lowest price.
    best_level = min(asks, key=lambda level: float(level["price"]))
    
    # Return its size as float.
    return float(best_level["size"])


# Define a helper function to sum depth across the top n levels.
def get_depth_top_n(levels, n):
    # Return 0.0 if there are no levels.
    if not levels:
        return 0.0
    
    # Sort levels by price.
    sorted_levels = levels
    
    # Sum the size of the first n levels.
    return sum(float(level["size"]) for level in sorted_levels[:n])


# Define a helper function to compute imbalance.
def compute_imbalance(bid_depth, ask_depth):
    # Compute the denominator.
    denom = bid_depth + ask_depth
    
    # Return NaN if the denominator is zero.
    if denom == 0:
        return np.nan
    
    # Return the imbalance.
    return (bid_depth - ask_depth) / denom

def read_market_to_df(market_path) -> pd.DataFrame:
    # Create an empty list to collect market-level rows.
    market_rows = []

    # Open the market file for reading.
    with open(market_path, "r", encoding="utf-8") as f:
        # Loop through each line in the file.
        for line in f:
            # Parse the current line as JSON.
            market = json.loads(line)
            
            # Parse the outcomes field, which is stored as a JSON string.
            outcomes = parse_json_list(market.get("outcomes"))
            
            # Parse the outcomePrices field, which is stored as a JSON string.
            outcome_prices = parse_json_list(market.get("outcomePrices"))
            
            # Build a clean market-level row.
            market_rows.append({
                "market_id": market.get("id"),
                "condition_id": market.get("conditionId"),
                "question": market.get("question"),
                "slug": market.get("slug"),
                "description": market.get("description"),
                "start_date": market.get("startDate"),
                "end_date": market.get("endDate"),
                "closed_time": market.get("closedTime"),
                "active": market.get("active"),
                "closed": market.get("closed"),
                "archived": market.get("archived"),
                "accepting_orders": market.get("acceptingOrders"),
                "enable_order_book": market.get("enableOrderBook"),
                "neg_risk": market.get("negRisk"),
                "fee_decimal": parse_fee_decimal(market.get("fee")),
                "volume": safe_float(market.get("volume")),
                "volume_clob": safe_float(market.get("volumeClob")),
                "market_best_bid": safe_float(market.get("bestBid")),
                "market_best_ask": safe_float(market.get("bestAsk")),
                "last_trade_price": safe_float(market.get("lastTradePrice")),
                "resolution_source": market.get("resolutionSource"),
                "uma_resolution_status": market.get("umaResolutionStatus"),
                "created_at": market.get("createdAt"),
                "updated_at": market.get("updatedAt"),
                "n_outcomes": len(outcomes),
                "outcomes_raw": json.dumps(outcomes),
                "outcome_prices_raw": json.dumps(outcome_prices),
            })

    # Convert the list of dicts into a pandas DataFrame.
    markets_df = pd.DataFrame(market_rows)
    return markets_df

def read_token_to_df(token_path) -> pd.DataFrame:
    market_token_rows = []

    with open(token_path, "r", encoding="utf-8") as f:
        for line in f:
            market = json.loads(line)

            outcomes = parse_json_list(market.get("outcomes"))
            outcome_prices = parse_json_list(market.get("outcomePrices"))
            token_ids = parse_json_list(market.get("clobTokenIds"))

            for i, token_id in enumerate(token_ids):
                market_token_rows.append({
                    "market_id": market.get("id"),
                    "token_id": str(token_id),
                    "outcome_index": i,
                    "outcome": outcomes[i] if i < len(outcomes) else None,
                    "outcome_price_initial": safe_float(outcome_prices[i]) if i < len(outcome_prices) else np.nan,
                    "question": market.get("question"),
                    "slug": market.get("slug"),
                    "description": market.get("description"),
                    "start_date": market.get("startDate"),
                    "end_date": market.get("endDate"),
                    "closed_time": market.get("closedTime"),
                    "active": market.get("active"),
                    "closed": market.get("closed"),
                    "archived": market.get("archived"),
                    "accepting_orders": market.get("acceptingOrders"),
                    "enable_order_book": market.get("enableOrderBook"),
                    "neg_risk": market.get("negRisk"),
                    "fee_decimal": parse_fee_decimal(market.get("fee")),
                    "volume": safe_float(market.get("volume")),
                    "volume_clob": safe_float(market.get("volumeClob")),
                })

    market_tokens_df = pd.DataFrame(market_token_rows)
    return market_tokens_df

API_KEY = "beb79777a5762ef81b41fbaae1dbb75d23fcee28"
DOME_HEADERS = {"Authorization": f"Bearer {API_KEY}", "x-api-key": API_KEY}

DOME_URL = "https://api.domeapi.io/v1/polymarket/orderbooks"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

async def extract_orderbook_from_ids_async(
    token_ids,
    start_date,
    end_date,
    output_path,
    max_concurrent=5,
    max_retries=6,
):
    output_path = Path(output_path)
    
    dome_history_start = pd.Timestamp("2025-10-14", tz="UTC")
    
    def _to_utc_timestamp(value):
        if value is None or pd.isna(value):
            return None
        ts = pd.Timestamp(value)
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")

    async def _fetch_all_snapshots(session, token_id, start_ms, end_ms):
        params = {
            "token_id": str(token_id),
            "start_time": int(start_ms),
            "end_time": int(end_ms),
            "limit": 200,
        }

        all_snaps = []

        while True:
            for attempt in range(1, max_retries + 1):
                try:
                    async with session.get(DOME_URL, params=params, headers=DOME_HEADERS, timeout=30) as resp:
                        if resp.status in RETRYABLE_STATUS:
                            raise aiohttp.ClientResponseError(
                                request_info=resp.request_info,
                                history=resp.history,
                                status=resp.status,
                                message=f"retryable status={resp.status}",
                                headers=resp.headers,
                            )
                        resp.raise_for_status()
                        payload = await resp.json()
                        break
                except Exception as e:
                    if attempt == max_retries:
                        raise RuntimeError(f"Failed token_id={token_id} after {max_retries} retries: {e}")
                    backoff = min(30, (2 ** attempt) + random.random())
                    print(f"Retry {attempt}/{max_retries} token_id={token_id} in {backoff:.1f}s ({e})")
                    await asyncio.sleep(backoff)

            snaps = payload.get("snapshots", [])
            all_snaps.extend(snaps)

            pagination = payload.get("pagination", {}) or {}
            has_more = pagination.get("has_more", False)
            pagination_key = pagination.get("pagination_key") or pagination.get("paginationKey")

            if not has_more or not pagination_key:
                break

            params["pagination_key"] = pagination_key
            await asyncio.sleep(0.05)

        return all_snaps
    
    start_ts = _to_utc_timestamp(start_date)
    end_ts = _to_utc_timestamp(end_date)
    
    if start_ts is None or end_ts is None:
        raise ValueError("start_date and end_date must be valid timestamps")

    if start_ts < dome_history_start:
        print(f"Warning: start_date {start_ts} is before Dome history start {dome_history_start}. Adjusting to Dome history start.")
        start_ts = dome_history_start
        
    # convert timestamps to epoch ms for API payload
    start_ms = int(start_ts.value // 1_000_000)
    end_ms = int(end_ts.value // 1_000_000)
    
    sem = asyncio.Semaphore(max_concurrent)

    async def _save_orderbook_of_token(i, token_id, session):

        async with sem:
            print(f"Processing token_id={token_id}, {i}/{len(token_ids)}")
            try:
                snapshots = await _fetch_all_snapshots(session, token_id, start_ms, end_ms)
            except Exception as e:
                print(f"Skipping token_id={token_id}: {e}")
                return 0

        if not snapshots:
            return 0

        snapshots = sorted(
            snapshots,
            key=lambda s: (s.get("timestamp", 0), s.get("indexedAt", 0))
        )

        out = []
        for snap in snapshots:
            snap_ts = pd.to_datetime(snap.get("timestamp"), unit="ms", utc=True, errors="coerce")
            indexed_ts = pd.to_datetime(snap.get("indexedAt"), unit="ms", utc=True, errors="coerce")

            bids = snap.get("bids", []) or []
            asks = snap.get("asks", []) or []

            out.append({
                "token_id": token_id,
                "snapshot_time": snap_ts,
                "snapshot_timestamp_ms": snap.get("timestamp"),
                "indexed_at_time": indexed_ts,
                "indexed_at_ms": snap.get("indexedAt"),
                "market_hash": snap.get("market"),
                "asset_id": snap.get("assetId"),
                "tick_size": float(snap["tickSize"]) if snap.get("tickSize") is not None else np.nan,
                "min_order_size": float(snap["minOrderSize"]) if snap.get("minOrderSize") is not None else np.nan,
                "orderbook_neg_risk": snap.get("negRisk"),
                "bids_json": json.dumps(bids),
                "asks_json": json.dumps(asks),
            })
        
        token_df = pd.DataFrame(out).sort_values(["snapshot_time"], kind="stable").reset_index(drop=True)
        token_df.to_parquet(output_path / f"ob_{token_id}.parquet", index=False)
        print(f"Saved {len(token_df):,} rows for token_id={token_id} to {output_path / f'ob_{token_id}.parquet'}")
        return len(token_df)

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            _save_orderbook_of_token(i, token_id, session)
            for i, token_id in enumerate(token_ids, start=1)
        ]
        counts = await asyncio.gather(*tasks)

    print(f"Done processing all tokens. Total rows: {sum(counts):,}")
