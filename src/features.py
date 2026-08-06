import json
from pathlib import Path

import numpy as np
import pandas as pd


def parse_orderbook_side(side_json):
    """
    Parse a bids_json / asks_json string into a list of dicts:
    [{"price": float, "size": float}, ...]
    for consistency.
    """
    if side_json is None:
        return []

    if isinstance(side_json, float) and np.isnan(side_json):
        return []

    if isinstance(side_json, str):
        try:
            levels = json.loads(side_json)
        except Exception:
            return []
    elif isinstance(side_json, list):
        levels = side_json
    else:
        return []

    out = []
    for level in levels:
        try:
            out.append({
                "price": float(level["price"]),
                "size": float(level["size"]),
            })
        except Exception:
            continue

    return out


def get_best_bid(levels):
    if not levels:
        return np.nan, 0.0
    best = max(levels, key=lambda x: x["price"])
    return best["price"], best["size"]


def get_best_ask(levels):
    if not levels:
        return np.nan, 0.0
    best = min(levels, key=lambda x: x["price"])
    return best["price"], best["size"]


def get_top_n_bid_depth(levels, n=3):
    if not levels:
        return 0.0
    top = sorted(levels, key=lambda x: x["price"], reverse=True)[:n]
    return float(sum(x["size"] for x in top))


def get_top_n_ask_depth(levels, n=3):
    if not levels:
        return 0.0
    top = sorted(levels, key=lambda x: x["price"])[:n]
    return float(sum(x["size"] for x in top))


def get_total_depth(levels):
    if not levels:
        return 0.0
    return float(sum(x["size"] for x in levels))


def extract_snapshot_features(bids_json, asks_json, depth_n=3):
    bids = parse_orderbook_side(bids_json)
    asks = parse_orderbook_side(asks_json)

    best_bid, best_bid_size = get_best_bid(bids)
    best_ask, best_ask_size = get_best_ask(asks)

    if np.isnan(best_bid) or np.isnan(best_ask):
        mid_price = np.nan
        spread = np.nan
    else:
        mid_price = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid

    # Data-quality flags: downstream label/panel code must be able to tell a real
    # quote from a degenerate book state instead of silently consuming NaNs.
    flag_no_bids = len(bids) == 0
    flag_no_asks = len(asks) == 0
    flag_crossed_book = (
        not flag_no_bids and not flag_no_asks and best_bid > best_ask
    )

    bid_depth_n = get_top_n_bid_depth(bids, n=depth_n)
    ask_depth_n = get_top_n_ask_depth(asks, n=depth_n)

    bid_depth_total = get_total_depth(bids)
    ask_depth_total = get_total_depth(asks)

    denom_n = bid_depth_n + ask_depth_n
    imbalance_n = (bid_depth_n - ask_depth_n) / denom_n if denom_n > 0 else np.nan

    denom_total = bid_depth_total + ask_depth_total
    imbalance_total = (
        (bid_depth_total - ask_depth_total) / denom_total
        if denom_total > 0 else np.nan
    )

    return {
        "best_bid": best_bid,
        "best_bid_size": best_bid_size,
        "best_ask": best_ask,
        "best_ask_size": best_ask_size,
        "mid_price": mid_price,
        "spread": spread,
        "bid_depth_3": bid_depth_n,
        "ask_depth_3": ask_depth_n,
        "imbalance_3": imbalance_n,
        "bid_depth_total": bid_depth_total,
        "ask_depth_total": ask_depth_total,
        "imbalance_total": imbalance_total,
        "n_bid_levels": len(bids),
        "n_ask_levels": len(asks),
        "flag_no_bids": flag_no_bids,
        "flag_no_asks": flag_no_asks,
        "flag_crossed_book": flag_crossed_book,
    }


def attach_token_metadata(
    feat_df: pd.DataFrame,
    token_meta_df: pd.DataFrame,
    metadata_cols=None,
    add_time_features=True,
) -> pd.DataFrame:
    # NOTE (lookahead): `closed_time` (realised resolution time) and `volume` /
    # `volume_clob` (lifetime totals as of the metadata dump) are deliberately
    # excluded — none of them are knowable at snapshot time. `fee_decimal` is
    # also excluded: the gamma `fee` field is absent on modern markets (all-NaN
    # here); fee rates live in data/processed/fee_schedule.parquet instead.
    if metadata_cols is None:
        metadata_cols = [
            "token_id",
            "market_id",
            "outcome_index",
            "end_date",
            "neg_risk",
        ]

    cols_to_use = [c for c in metadata_cols if c in token_meta_df.columns]
    meta_small = token_meta_df[cols_to_use].copy()

    overlapping = [c for c in cols_to_use if c != "token_id" and c in feat_df.columns]
    if overlapping:
        feat_df = feat_df.drop(columns=overlapping)

    out = feat_df.merge(meta_small, on="token_id", how="left")

    if add_time_features:
        out["snapshot_time"] = pd.to_datetime(out["snapshot_time"], utc=True, errors="coerce")
        out["end_date"] = pd.to_datetime(out.get("end_date"), utc=True, errors="coerce")

        # Time to expiry uses the SCHEDULED end date only. The realised
        # `closed_time` is not knowable at snapshot time and using it leaks the
        # actual resolution moment into the features.
        delta = out["end_date"] - out["snapshot_time"]

        out["time_to_expiry_hours"] = delta.dt.total_seconds() / 3600.0
        out["log_time_to_expiry_hours"] = np.log1p(
            out["time_to_expiry_hours"].clip(lower=0)
        )

    return out


def add_token_time_series_features(feat_df: pd.DataFrame) -> pd.DataFrame:
    # Work on a copy so the original dataframe is not modified in place.
    out = feat_df.copy()

    # Make sure snapshot_time is a proper datetime column.
    out["snapshot_time"] = pd.to_datetime(out["snapshot_time"], utc=True, errors="coerce")

    # Sort so "previous row" actually means previous snapshot in time for each token.
    out = out.sort_values(["token_id", "snapshot_time"], kind="stable").reset_index(drop=True)

    # Previous timestamp within the same token.
    out["prev_snapshot_time"] = out.groupby("token_id")["snapshot_time"].shift(1)

    # Previous best bid within the same token.
    out["prev_best_bid"] = out.groupby("token_id")["best_bid"].shift(1)

    # Previous best ask within the same token.
    out["prev_best_ask"] = out.groupby("token_id")["best_ask"].shift(1)

    # Previous mid price within the same token.
    out["prev_mid_price"] = out.groupby("token_id")["mid_price"].shift(1)

    # Change in best bid from previous snapshot.
    out["delta_best_bid"] = out["best_bid"] - out["prev_best_bid"]

    # Change in best ask from previous snapshot.
    out["delta_best_ask"] = out["best_ask"] - out["prev_best_ask"]

    # Change in mid price from previous snapshot.
    out["delta_mid_price"] = out["mid_price"] - out["prev_mid_price"]

    # Time gap between this snapshot and the previous one, in seconds.
    out["seconds_since_prev_snapshot"] = (
        out["snapshot_time"] - out["prev_snapshot_time"]
    ).dt.total_seconds()

    return out


def build_token_feature_table_from_parquet(
    input_path,
    output_path=None,
    depth_n=3,
    drop_json_cols=True,
    token_meta_df=None,
):
    """
    Read one raw orderbook parquet for a single token,
    compute token-level base-table-3 features,
    and optionally save to parquet.
    """
    input_path = Path(input_path)

    df = pd.read_parquet(input_path)

    # Deduplicate snapshots: Dome pagination can return overlapping pages, and the
    # same book timestamp can be re-indexed. Keep the latest-indexed row per
    # (token, book timestamp).
    df = (
        df.sort_values(["snapshot_timestamp_ms", "indexed_at_ms"], kind="stable")
        .drop_duplicates(subset=["token_id", "snapshot_timestamp_ms"], keep="last")
        .reset_index(drop=True)
    )

    rows = []
    for row in df.to_dict("records"):
        feats = extract_snapshot_features(
            bids_json=row["bids_json"],
            asks_json=row["asks_json"],
            depth_n=depth_n,
        )

        out_row = dict(row)
        out_row.update(feats)
        rows.append(out_row)

    feat_df = pd.DataFrame(rows).sort_values(
        ["token_id", "snapshot_time"],
        kind="stable"
    ).reset_index(drop=True)

    if drop_json_cols:
        drop_cols = [c for c in ["bids_json", "asks_json"] if c in feat_df.columns]
        feat_df = feat_df.drop(columns=drop_cols)

    if token_meta_df is not None:
        feat_df = attach_token_metadata(feat_df, token_meta_df)
        feat_df = add_token_time_series_features(feat_df)
        # `indexed_at_time`/`indexed_at_ms` are kept: they are the honest
        # "data available at" clock for latency-sensitive evaluation.
        feat_df = feat_df.drop(
            columns=[
                "market_hash",
                "asset_id",
                "prev_snapshot_time",
                "prev_best_bid",
                "prev_best_ask",
                "prev_mid_price",
                "outcome",
                "end_date",
                "closed_time",
            ],
            errors="ignore",
        )
    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        feat_df.to_parquet(output_path, index=False)

    return feat_df

