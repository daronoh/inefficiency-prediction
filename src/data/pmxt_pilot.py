"""Isolated discovery/acquisition entry point for a development-only PMXT pilot.

Run ``python -m src.data.pmxt_pilot --help``. Existing Dome code/data are read
only. No model is fitted and no old split or result is replaced.
"""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

import pandas as pd
import requests

from src.common import sha256
from src.paths import DATA_DIR
from src.data.universe_detection import detect_universe, normalise_market, normalise_event
from src.data.pmxt_archive import (TransferBudget, dome_fingerprint, verify_dome,
    inventory_hour, extract_hour, isolated, write_json)

DEFAULT_ROOT = DATA_DIR/'pmxt/pilot_v1'
DEFAULT_CATALOGUE = DATA_DIR/'raw/catalogues/march_august_2026_partitioned/discovery_partial'


def sector(slug):
    if re.search(r'-2026-\d{2}-\d{2}$',slug): return 'sports'
    if any(t in slug for t in ['decision','inflation','gdp','deficit','unemployment']): return 'macro'
    if any(t in slug for t in ['bitcoin','ethereum','crypto','token']): return 'crypto'
    if any(t in slug for t in ['ipo','company','market-cap','revenue']): return 'business'
    if any(t in slug for t in ['election','president','minister']): return 'politics'
    return 'other'


def ranked_candidates(events, markets, inventory, start, end, maximum_members=12):
    """Metadata and warm-up identifier coverage only; never rank on prices."""
    present = set(inventory.asset_id)
    grouped = dict(tuple(markets.groupby('event_id',sort=False)))
    # Filter the large catalogue before per-event structural checks.
    events = events[(events.classification=='negrisk') & (events.augmented==False)
        & events.markets.between(2,maximum_members)
        & (pd.to_datetime(events.nominal_end,utc=True,errors='coerce')>=end)]
    candidates = []
    for row in events.to_dict('records'):
        if row['classification'] != 'negrisk' or row.get('augmented') is not False:
            continue
        members = grouped[row['event_id']]
        listed = pd.to_datetime(members.listed_at,utc=True,errors='coerce')
        close = pd.to_datetime(row.get('nominal_end'),utc=True,errors='coerce')
        if (not 2 <= len(members) <= maximum_members or listed.isna().any()
            or listed.max() > start or pd.isna(close) or close < end
            or not set(members.yes_token)<=present):
            continue
        row['sector'] = sector(row['event_slug'])
        row['selection_hash'] = hashlib.sha256(('pmxt-pilot-v1:'+row['event_id']).encode()).hexdigest()
        candidates.append(row)
    # Round-robin sectors avoids the thousands of sports listings dominating a
    # small engineering pilot. This is not a representative population sample.
    groups = {}
    for row in sorted(candidates,key=lambda r:r['selection_hash']):
        groups.setdefault(row['sector'],[]).append(row)
    result = []
    while groups:
        for key in sorted(list(groups)):
            result.append(groups[key].pop(0))
            if not groups[key]: del groups[key]
    return result


def metadata_event(session, event_id, folder):
    """Persist whitelisted structural/current fee metadata, never outcome prices."""
    folder = isolated(folder)
    folder.mkdir(parents=True,exist_ok=True)
    path = folder/f'{event_id}.json'
    if path.exists(): return json.loads(path.read_text(encoding='utf-8'))
    response = session.get(f'https://gamma-api.polymarket.com/events/{event_id}',timeout=(10,40))
    response.raise_for_status()
    raw = response.json()
    event = normalise_event(raw)
    event['title'] = raw.get('title')
    event['fetched_at'] = pd.Timestamp.now(tz='UTC').isoformat()
    event['historical_metadata_verified'] = False
    members = []
    for original in raw.get('markets',[]):
        record = dict(original,event_id=event['event_id'],event_slug=event['event_slug'])
        member = normalise_market(record)
        member.update(condition_id=original.get('conditionId'),
            order_min_size_current=original.get('orderMinSize'),
            tick_size_current=original.get('orderPriceMinTickSize'),
            fees_enabled_current=original.get('feesEnabled'),
            fee_type_current=original.get('feeType'),
            fee_schedule_current=original.get('feeSchedule'),
            created_at_current=original.get('createdAt'))
        members.append(member)
    result = dict(event=event,markets=members,
        source=f'https://gamma-api.polymarket.com/events/{event_id}')
    write_json(path,result)
    return result


def discover(root=DEFAULT_ROOT, catalogue=DEFAULT_CATALOGUE, *, start='2026-05-04T01:00:00Z',
             end='2026-05-11T01:00:00Z', target_negrisk=10,target_ladder=10,budget=None):
    root = isolated(root)
    root.mkdir(parents=True,exist_ok=True)
    if (root/'plan.json').exists():
        plan = json.loads((root/'plan.json').read_text())
        if (pd.Timestamp(plan['start']) != pd.Timestamp(start)
            or pd.Timestamp(plan['end']) != pd.Timestamp(end)
            or plan['targets'] != {'negrisk':target_negrisk,'ladder':target_ladder}):
            raise ValueError('Run already frozen with a different design; use a new isolated run')
        return plan
    before_path = root/'dome_before.json'
    if not before_path.exists(): write_json(before_path,dome_fingerprint())
    start,end = pd.Timestamp(start),pd.Timestamp(end)
    if start.tzinfo is None or end.tzinfo is None or start >= end or start != start.floor('h') or end != end.floor('h'):
        raise ValueError('Nonempty timezone-aware whole-hour window required')
    warmup = start-pd.Timedelta(hours=1)
    inventory,manifest = inventory_hour(warmup,root/'discovery',budget or TransferBudget())
    catalogue = Path(catalogue)
    events,markets = (pd.read_parquet(catalogue/f'{name}.parquet') for name in ('events','markets'))
    pairs = pd.read_parquet(catalogue/'ladder_pairs.parquet')
    candidates = ranked_candidates(events,markets,inventory,warmup,end)
    # A supported ladder pair is independently sufficient; an absent unrelated
    # threshold does not invalidate the pairs whose two tokens are observed.
    active = pairs[(pd.to_datetime(pairs.window_start,utc=True) < end)
        & (pd.to_datetime(pairs.window_end,utc=True) > start)
        & pairs.broad_yes_token.isin(inventory.asset_id)
        & pairs.narrow_no_token.isin(inventory.asset_id)]
    ladders = events[(events.classification=='ladder') & events.event_id.isin(active.event_id)]
    ladders = sorted(ladders.to_dict('records'),key=lambda r:r['event_slug'])
    session = requests.Session()
    accepted,all_markets,packages,audits = [],[],[],[]
    seen = set(inventory.asset_id)
    counts = Counter()
    for candidate in candidates+ladders:
        kind = candidate['classification']
        target = target_negrisk if kind == 'negrisk' else target_ladder
        if counts[kind] >= target: continue
        fetched = metadata_event(session,candidate['event_id'],root/'metadata')
        event,members = fetched['event'],fetched['markets']
        # This establishes current structural completeness ONLY. A fixed
        # historical membership/rules assumption is recorded in the plan.
        event['complete_set_verified'] = True
        result = detect_universe(pd.DataFrame(members),pd.DataFrame([event]),
            window_start=warmup.isoformat(),window_end=end.isoformat(),metadata_mode='frozen')
        supported = result.packages.to_dict('records')
        supported = [p for p in supported if p['stratum']==kind and set(p['token_ids'])<=seen]
        if kind == 'negrisk' and len(members)>12: supported = []
        audits.append(dict(event_id=event['event_id'],event_slug=event['event_slug'],
            requested_kind=kind,accepted=bool(supported),
            detector=result.events.to_dict('records'),coverage_packages=len(supported)))
        if not supported:
            print(f"Rejected {event['event_slug']}: structure/coverage",flush=True)
            continue
        accepted.append(dict(event_id=event['event_id'],event_slug=event['event_slug'],
            classification=kind,sector=sector(event['event_slug']),
            historical_metadata_verified=False,current_inventory_members=len(members)))
        for member in members:
            member['event_id'] = event['event_id']
        all_markets.extend(members)
        packages.extend(supported)
        counts[kind] += 1
        print(f"Selected {kind} {counts[kind]}: {event['event_slug']}, {len(supported)} baskets",flush=True)
    required = sorted({str(t) for p in packages for t in p['token_ids']})
    condition_map = inventory.groupby('asset_id').market.agg(lambda v:sorted(set(v)))
    asset_conditions = {}
    for asset in required:
        values = condition_map[asset]
        if len(values) != 1: raise ValueError('Ambiguous condition ID in archive')
        asset_conditions[asset] = values[0]
    for m in all_markets:
        for side in ('yes_token','no_token'):
            token = m[side]
            if token in asset_conditions and asset_conditions[token] != m['condition_id']:
                raise ValueError('Gamma/archive condition mapping differs')
    pd.DataFrame(accepted).to_parquet(root/'events.parquet',index=False)
    pd.DataFrame(all_markets).to_parquet(root/'markets.parquet',index=False)
    pd.DataFrame(packages).to_parquet(root/'packages.parquet',index=False)
    write_json(root/'discovery/selection_audit.json',audits)
    plan = dict(version=1,provider='pmxt_v2',start=start.isoformat(),end=end.isoformat(),
        warmup_start=warmup.isoformat(),targets={'negrisk':target_negrisk,'ladder':target_ladder},
        selected=dict(counts),assets=required,asset_conditions=asset_conditions,
        catalogue_complete=False,catalogue_path=str(catalogue.resolve()),
        catalogue_sha256={n:sha256(catalogue/f'{n}.parquet') for n in ['events','markets','ladder_pairs']},
        selection='sector round robin, fixed hash; <=12 negRisk members; warmup identifier presence; no prices',
        metadata_mode='retrospective fixed rules/membership assumption; NOT point-in-time verified',
        historical_fees_verified=False,final_test_accessed=False,models_trained=False,
        files_sha256={n:sha256(root/f'{n}.parquet') for n in ['events','markets','packages']})
    write_json(root/'plan.json',plan)
    return plan


def load_plan(root):
    root = isolated(root)
    plan = json.loads((root/'plan.json').read_text(encoding='utf-8'))
    for name,digest in plan['files_sha256'].items():
        if sha256(root/f'{name}.parquet') != digest:
            raise ValueError('Frozen PMXT pilot universe changed')
    return root,plan


def download(root=DEFAULT_ROOT, *, hours=None, transfer_gb=2):
    root,plan = load_plan(root)
    clocks = pd.date_range(plan['warmup_start'],plan['end'],freq='h',inclusive='left')
    if hours is not None:
        if hours < 1: raise ValueError('Positive hour count required')
        clocks = clocks[:hours]
    budget = TransferBudget(int(transfer_gb*1e9))
    for clock in clocks:
        _,manifest = extract_hour(clock,root/'raw',plan['assets'],set(plan['asset_conditions'].values()),budget)
        if clock==pd.Timestamp(plan['warmup_start']):
            discovery = json.loads((root/'discovery/inventory_manifest.json').read_text())
            if discovery['etag'] != manifest['etag']:
                raise ValueError('Warmup archive changed since identifier-based selection')
    return dict(requested_hours=len(clocks),transfer_bytes=budget.used)


def verify(root=DEFAULT_ROOT):
    root = isolated(root)
    result = verify_dome(json.loads((root/'dome_before.json').read_text(encoding='utf-8')))
    write_json(root/'dome_verification.json',dict(result,checked_at=pd.Timestamp.now(tz='UTC')))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('step',choices=['discover','download','audit','verify'])
    parser.add_argument('--root',type=Path,default=DEFAULT_ROOT)
    parser.add_argument('--catalogue',type=Path,default=DEFAULT_CATALOGUE)
    parser.add_argument('--hours',type=int,default=2,help='Total cached prefix hours, including warmup; 169 for planned week')
    parser.add_argument('--transfer-gb',type=float,default=2,help='Hard logical range-byte budget per download invocation')
    args = parser.parse_args()
    if args.step == 'discover': result = discover(args.root,args.catalogue)
    elif args.step == 'download': result = download(args.root,hours=args.hours,transfer_gb=args.transfer_gb)
    elif args.step == 'verify': result = verify(args.root)
    else:
        from src.data.pmxt_audit import audit
        result = audit(args.root)
    if args.step == 'discover':
        result = {k:result[k] for k in ['start','end','targets','selected','catalogue_complete']}
    print(json.dumps(result,indent=2,default=str))


if __name__ == '__main__': main()
