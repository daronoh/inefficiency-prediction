"""Measure historical book seeding without inventing a starting depth state."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import isolated, write_json
from src.data.pmxt_multiday import STUDY_ROOT


def seed_statistics(frame, assets, capture, start, end):
    """A snapshot must lie on/after source capture to seed the frozen v3 replay.

    Receipt tails may supply late source records. Report that clock assumption
    explicitly rather than treating a late native record as a timely delivery.
    """
    books=frame.loc[frame.event_type.eq('book')]
    updates=frame.loc[frame.event_type.eq('price_change') & frame.timestamp.ge(capture) & frame.timestamp.lt(end)]
    qualifying=books.loc[books.timestamp.ge(capture)]
    first=qualifying.groupby('asset_id').timestamp.min()
    seed=pd.to_datetime(updates.asset_id.map(first.to_dict()),utc=True)
    unseeded=seed.isna() | updates.timestamp.lt(seed)
    records=[]
    for asset in assets:
        at=first.get(asset,pd.NaT)
        records.append(dict(asset_id=asset,first_qualifying_book_source=at,
            seeded_before_scan=bool(pd.notna(at) and at<start),
            seeded_before_end=bool(pd.notna(at) and at<end),
            price_updates_before_seed=int((unseeded & updates.asset_id.eq(asset)).sum())))
    lag=(books.timestamp_received-books.timestamp).dt.total_seconds()
    stats=dict(raw_book_messages=len(books),unique_book_source_groups=len(books.drop_duplicates(['asset_id','timestamp'])),
        books_before_source_capture=int(books.timestamp.lt(capture).sum()),
        assets_with_any_book=int(books.asset_id.nunique()),
        assets_seeded_before_first_scan=sum(r['seeded_before_scan'] for r in records),
        assets_seeded_before_analysis_end=sum(r['seeded_before_end'] for r in records),
        assets_without_any_qualifying_book=sum(pd.isna(r['first_qualifying_book_source']) for r in records),
        price_updates_before_seed=int(unseeded.sum()),price_updates_in_capture_analysis=len(updates),
        unseeded_update_fraction=float(unseeded.mean()) if len(updates) else None,
        median_book_receipt_lag_seconds=float(lag.median()) if len(lag) else None,
        p99_book_receipt_lag_seconds=float(lag.quantile(.99)) if len(lag) else None)
    return stats,pd.DataFrame(records)


def audit_seeds(root=STUDY_ROOT):
    root=isolated(root)
    design_path=root/'study_design.json'
    design=json.loads(design_path.read_text())
    rows=[]
    assets=[]
    evidence={str(design_path.relative_to(PROJECT_ROOT)):sha256(design_path)}
    for block in design['blocks']:
        folder=root/block['name']
        plan=json.loads((folder/'plan.json').read_text())
        inputs=[]
        for clock in pd.date_range(block['warmup_start'],block['receipt_end'],freq='h',inclusive='left'):
            path=folder/'raw'/f'{clock:%Y-%m-%dT%H}.parquet'
            manifest=json.loads(path.with_suffix('.json').read_text())
            if sha256(path)!=manifest['sha256']:
                raise ValueError('Raw cache changed before seed audit')
            evidence[str(path.relative_to(PROJECT_ROOT))]=manifest['sha256']
            inputs.append(pd.read_parquet(path,columns=['asset_id','event_type','timestamp','timestamp_received']))
        stats,tokens=seed_statistics(pd.concat(inputs,ignore_index=True),plan['assets'],
            pd.Timestamp(block['warmup_start']),pd.Timestamp(block['start']),pd.Timestamp(block['analysis_end']))
        rows.append(dict(block=block['name'],**stats))
        tokens['block']=block['name']
        assets.append(tokens)
    output=isolated(PROJECT_ROOT/'results/pmxt_pilot'/root.name,'results')
    output.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(rows).to_csv(output/'seed_coverage.csv',index=False)
    pd.concat(assets,ignore_index=True).to_csv(output/'seed_tokens.csv',index=False)
    write_json(output/'seed_audit.json',dict(evidence_sha256=evidence,
        implementation_sha256={str(Path(__file__).relative_to(PROJECT_ROOT)):sha256(Path(__file__))},
        interpretation='No qualifying seed is a limitation of these isolated blocks and their warm-up, not proof that continuous PMXT history lacks the required snapshot. No state was backfilled.'))
    return pd.DataFrame(rows)


if __name__=='__main__':
    print(audit_seeds().to_string(index=False))
