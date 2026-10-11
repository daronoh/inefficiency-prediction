"""Manually audited implication rules and exact frozen historical fee flags."""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json
import re
from itertools import combinations
import pandas as pd
from src.config import SPEC, require_split_access
from src.paths import MARKETS_PARQUET, TOKENS_PARQUET, EVENT_UNIVERSE, MARKET_JSONL
@dataclass(frozen=True)
class PairDefinition:
    pair_id: str
    event_slug: str
    broad_market_id: str
    narrow_market_id: str
    kind: str
    description_hash_broad: str
    description_hash_narrow: str
    rule_summary: str
    window_end_exclusive: str


AUDITED_FAMILIES = (
    PairDefinition("shutdown", "will-the-government-shutdown-end-by-763", "638734", "638733", "deadline",
        "99a50f2f58142836dd1f7ebd6c2d1d214104c526c43c196801c641762bf8703a",
        "99a50f2f58142836dd1f7ebd6c2d1d214104c526c43c196801c641762bf8703a",
        "OPM qualifying end announcement by Oct31 implies by Nov15; identical ET/source rules.", "2025-11-01T04:00:00Z"),
    PairDefinition("monad_fdv", "monad-market-cap-fdv-one-day-after-launch", "556074", "556075", "threshold",
        "c76acbae57a56733c41f6778a8464348703115aa75e01123bd0791ea9c6c8966",
        "5a9e4c5b2e0e3b0011ea454a10611d29de1461254b53c45e49625dabacc5e9b9",
        "Same token FDV 24h after launch: >6B implies >2B; identical no-launch fallback.", "2026-01-08T00:00:00Z"),
    PairDefinition("gensyn", "gensyn-public-sale-total-commitments", "908024", "908025", "threshold",
        "355ec7cb90f1dedba432a8cd036b2519e42f75eb10904ba012ffc517192ed530",
        "355ec7cb90f1dedba432a8cd036b2519e42f75eb10904ba012ffc517192ed530",
        "Same Sonar raise: commitments >400M implies >200M; same verification fallback.", "2026-01-01T05:00:00Z"),
    PairDefinition("btc_november", "what-price-will-bitcoin-hit-in-november-2025", "659241", "659240", "reach",
        "f6f709191a6bbc258054d25ed2816ad6891bbbb7b991e1f29360dfa8713dbd66",
        "f6f709191a6bbc258054d25ed2816ad6891bbbb7b991e1f29360dfa8713dbd66",
        "Same November ET Binance BTC/USDT 1m High: reach120k implies reach115k.", "2025-12-01T05:00:00Z"),
    PairDefinition("eth_november", "what-price-will-ethereum-hit-in-november-2025", "659262", "659261", "reach",
        "b6bf73cc6f1706f5dd9f8a5dce2c2f8aabb488ca2c80bdca9637f51bb538c770",
        "b6bf73cc6f1706f5dd9f8a5dce2c2f8aabb488ca2c80bdca9637f51bb538c770",
        "Same November ET Binance ETH/USDT 1m High: reach4400 implies reach4200.", "2025-12-01T05:00:00Z"),
)


def ladder_payoff(broad_yes: bool, narrow_yes: bool) -> int:
    if narrow_yes and not broad_yes:
        raise ValueError("Narrow outcome must imply broad outcome")
    return int(broad_yes) + int(not narrow_yes)


def frozen_fee_evidence(market_ids: list[str]) -> pd.DataFrame:
    """Read exact selected-market flags, never infer fees from category defaults."""
    wanted = set(market_ids)
    rows = []
    with MARKET_JSONL.open(encoding="utf-8") as stream:
        for line in stream:
            market = json.loads(line)
            mid = str(market.get("id"))
            if mid in wanted:
                rows.append(dict(market_id=mid, fees_enabled_gamma=market.get("feesEnabled"),
                                 neg_risk=market.get("negRisk")))
                wanted.remove(mid)
                if not wanted:
                    break
    if wanted or not all(row["fees_enabled_gamma"] is False for row in rows):
        raise ValueError("Missing exact-market support for historical zero-fee scenario")
    return pd.DataFrame(rows)

AUDITED_IDS = {
    "shutdown": "638735 638736 638732 638733 638734 641250 651677",
    "monad_fdv": "556075 556074 582679 628682 628683 628687 649847 666437 666615",
    "gensyn": "908025 908027 908026 908028 908024 909442 910092 938218 941372 942772 948730 965249",
    "btc_november": "659238 659250 659240 659234 659244 659236 659242 659248 659237 659235 659246 659241 659243 659251 659247 659245 659233 659249 659239 659252",
    "eth_november": "659257 659268 659270 659254 659258 659256 659255 659263 659266 659260 659264 659272 659262 659271 659259 659267 659261 659269 659265 659273",
}


MONAD_TEMPLATE = "4a3c8d2e50cc2225c7692f5b530b9c34594f1cfa1bcb141da33058272daed9a0"


DIP_HASHES = {
    "btc_november": "cfe2e3a46d1ee67cd31eb13f15bf0a86309f62c0205e120946989fc5681054b7",
    "eth_november": "e01415a66b2707e820e0a9eca726a745d8f0394bb7adfc1cb2e4abb2b086c682",
}


def broad_first(kind: str, a: float, b: float) -> bool:
    if a == b:
        raise ValueError("Distinct thresholds required")
    return a > b if kind in {"deadline", "dip"} else a < b


def build_manifest(scope='audited5') -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if scope != 'audited5':
        raise ValueError('Unknown ladder scope')
    require_split_access("development")
    markets = pd.read_parquet(MARKETS_PARQUET).set_index("market_id")
    tokens = pd.read_parquet(TOKENS_PARQUET)
    universe = pd.read_parquet(EVENT_UNIVERSE)
    tokens.market_id = tokens.market_id.astype(str)
    markets.index = markets.index.astype(str)
    records = []
    for base in AUDITED_FAMILIES:
        ids = AUDITED_IDS[base.pair_id].split()
        found = set(universe.loc[universe.event_slug.eq(base.event_slug), "market_id"].astype(str))
        if found != set(ids):
            raise ValueError(f"Audited event membership changed: {base.event_slug}")
        for mid in ids:
            row = markets.loc[mid]
            text, question = row.description, row.question
            kind, expected = base.kind, base.description_hash_broad
            end = pd.Timestamp(base.window_end_exclusive)
            if base.pair_id == "shutdown":
                match = re.fullmatch(r"Will the Government shutdown end by ([A-Za-z]+) (\d+)\?", question)
                if match is None:
                    raise ValueError(question)
                year = 2026 if match[1] == "January" else 2025
                day = pd.Timestamp(f"{match[1]} {match[2]}, {year}")
                end = (day + pd.Timedelta(days=1)).tz_localize("America/New_York").tz_convert("UTC")
                threshold = end.timestamp()
            else:
                match = re.search(r"\$([\d,]+)([BM])?", question)
                if match is None:
                    raise ValueError(question)
                threshold = float(match[1].replace(",", "")) * {None: 1, "M": 1e6, "B": 1e9}[match[2]]
                if base.pair_id == "monad_fdv":
                    # Verify the description's numeric amount matches the title,
                    # then check every other byte against the audited template.
                    amount = re.search(r"\$([\d,]+)", text)
                    if amount is None or float(amount[1].replace(",", "")) != threshold:
                        raise ValueError("Title/description threshold mismatch")
                    text = re.sub(r"\$[\d,]+", "THRESHOLD", text)
                    expected = MONAD_TEMPLATE
                elif "dip to" in question:
                    kind, expected = "dip", DIP_HASHES[base.pair_id]
            if hashlib.sha256(text.encode()).hexdigest() != expected:
                raise ValueError(f"Unaudited rules: {mid}")
            ids_by_side = {}
            for side in ("YES", "NO"):
                chosen = tokens.loc[tokens.market_id.eq(mid) & tokens.outcome.str.upper().eq(side)]
                if len(chosen) != 1:
                    raise ValueError(f"Ambiguous token: {mid} {side}")
                ids_by_side[side.lower() + "_token"] = str(chosen.iloc[0].token_id)
            records.append(dict(event_slug=base.event_slug, family=base.pair_id, kind=kind,
                market_id=mid, threshold=threshold, question=question, description=row.description,
                description_sha256=hashlib.sha256(row.description.encode()).hexdigest(),
                start=max(pd.Timestamp(row.start_date), pd.Timestamp(SPEC.development_start)),
                end=min(end, pd.Timestamp(SPEC.lockbox_start)), **ids_by_side))
    rules = pd.DataFrame(records)
    pairs = []
    for (slug, kind), group in rules.groupby(["event_slug", "kind"], sort=True):
        for a, b in combinations(group.to_dict("records"), 2):
            broad, narrow = (a, b) if broad_first(kind, a["threshold"], b["threshold"]) else (b, a)
            start, end = max(a["start"], b["start"]), min(a["end"], b["end"])
            if start >= end:
                continue
            pairs.append(dict(pair_id=f'{broad["market_id"]}_{narrow["market_id"]}',
                event_slug=slug, kind=kind, broad_market_id=broad["market_id"], narrow_market_id=narrow["market_id"],
                broad_yes_token=broad["yes_token"], narrow_no_token=narrow["no_token"],
                window_start=start, window_end=end))
    pairs = pd.DataFrame(pairs).sort_values(["event_slug", "pair_id"]).reset_index(drop=True)
    needs = []
    for p in pairs.itertuples():
        for token in (p.broad_yes_token, p.narrow_no_token):
            needs.append(dict(token_id=token, start=p.window_start, end=p.window_end))
    downloads = pd.DataFrame(needs).groupby("token_id", as_index=False).agg(start=("start", "min"), end=("end", "max"))
    return pairs, downloads, rules

