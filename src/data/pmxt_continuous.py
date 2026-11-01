"""Compare isolated and continuously seeded PMXT reconstruction on a fixed subset.

Select one small negRisk basket and one ladder pair using seeding evidence and
fixed hashes only. Search backwards by complete receipt hours; never carry books
across unobserved gaps, backfill future states, or alter the frozen v3 policy.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import archive_url, isolated, write_json
from src.data.pmxt_multiday import STUDY_ROOT, SharedBudget, FileBudget, RetryingRangeFile
from src.data.pmxt_pilot import load_plan


ROOT = STUDY_ROOT.parent/'continuous_seed_v1'
START = pd.Timestamp('2026-05-06T09:00:00Z')
END = pd.Timestamp('2026-05-06T15:00:00Z')
EARLIEST = pd.Timestamp('2026-05-04T00:00:00Z')
ISOLATED = 'seed_v1_isolated'
CONTINUOUS = 'seed_v1_continuous'


def select_packages(packages, seeded):
    """Seed availability selects the diagnostic; prices/outcomes are not inputs."""
    missing = {str(t) for t, known in seeded.items() if not known}
    rank = lambda p: hashlib.sha256(('continuous-seed-v1:'+p['package_id']).encode()).hexdigest()
    records = packages.to_dict('records')
    negrisk = [p for p in records if p['stratum']=='negrisk' and set(p['token_ids']) & missing]
    ladders = [p for p in records if p['stratum']=='ladder' and set(p['token_ids']) <= missing]
    if not negrisk or not ladders:
        raise ValueError('Need an unseeded negRisk basket and an entirely unseeded ladder pair')
    return [min(negrisk,key=lambda p:(len(p['token_ids']),rank(p))),min(ladders,key=rank)]


def freeze(root=ROOT):
    root=isolated(root)
    root.mkdir(parents=True,exist_ok=True)
    source, parent=load_plan(STUDY_ROOT/'may06')
    seeding=PROJECT_ROOT/'results/pmxt_pilot/pilot_multiday_v1/seed_tokens.csv'
    seeds=pd.read_csv(seeding,dtype={'asset_id':str})
    seeds=seeds.loc[seeds.block.eq('may06')]
    selected=select_packages(pd.read_parquet(source/'packages.parquet'),dict(zip(seeds.asset_id,seeds.seeded_before_scan)))
    assets=sorted({str(t) for p in selected for t in p['token_ids']})
    design=dict(version=1,source_plan_sha256=sha256(source/'plan.json'),seeding_evidence_sha256=sha256(seeding),
        start=START.isoformat(),analysis_end=END.isoformat(),receipt_end=(END+pd.Timedelta(hours=2)).isoformat(),
        isolated_capture_start=(START-pd.Timedelta(hours=1)).isoformat(),earliest_capture_start=EARLIEST.isoformat(),
        selection='Smallest unseeded negRisk basket, fixed hash tie-break; fixed hash of entirely unseeded ladder pairs. No price/label/return input.',
        package_ids=[p['package_id'] for p in selected],assets=assets,
        scope='One complete negRisk basket and ONE pair from a ladder event; not the complete ladder event.',
        stopping_rule='Search complete preceding receipt hours until every token has a pre-scan snapshot on/after the contiguous capture boundary, or the fixed lower limit is reached.',
        maximum_new_range_bytes=8_000_000_000,maximum_selected_cache_bytes=500_000_000,
        replay='Unchanged v3 source-clock policy; one continuous state per token, no resets at hourly boundaries.',
        rules_sha256={p:sha256(PROJECT_ROOT/p) for p in ['src/data/pmxt_revalidation.py','src/data/pmxt_uncertainty.py','src/data/pmxt_diagnostics.py','src/data/pmxt_source.py']},
        execution='100ms primary; uncapped capital and shares; displayed support only; no PnL.',
        historical_parameters_verified=False,models_trained=False,new_final_test_accessed=False)
    path=root/'study_design.json'
    if path.exists() and json.loads(path.read_text())!=design:
        raise ValueError('Continuous diagnostic design already frozen differently')
    if not path.exists():
        write_json(path,design)
    packages=pd.DataFrame(selected)
    events=pd.read_parquet(source/'events.parquet')
    events=events.loc[events.event_id.isin(packages.event_id)].copy()
    events['diagnostic_scope']=events.classification.map({'negrisk':'complete_basket','ladder':'single_pair_only'})
    markets=pd.read_parquet(source/'markets.parquet')
    market_ids={str(m) for p in selected for m in p['market_ids']}
    markets=markets.loc[markets.market_id.isin(market_ids)]
    for name,table in [('events',events),('markets',markets),('packages',packages)]:
        target=root/f'{name}.parquet'
        if target.exists():
            pd.testing.assert_frame_equal(pd.read_parquet(target),table.reset_index(drop=True))
        else:
            table.reset_index(drop=True).to_parquet(target,index=False)
    write_json(root/'asset_conditions.json',{t:parent['asset_conditions'][t] for t in assets})
    return design


def extract_selected(hour, folder, assets, conditions, shared):
    """Read only row groups containing selected conditions and token IDs."""
    folder=isolated(folder)
    folder.mkdir(parents=True,exist_ok=True)
    path=folder/f'{hour:%Y-%m-%dT%H}.parquet'
    manifest_path=path.with_suffix('.json')
    if manifest_path.exists():
        saved=json.loads(manifest_path.read_text())
        if saved['sha256']!=sha256(path) or set(saved['assets'])!=set(assets):
            raise ValueError('Selected raw cache changed')
        return path,saved
    budget=FileBudget(shared)
    parts=[]
    with RetryingRangeFile(archive_url(hour),budget) as remote:
        archive=pq.ParquetFile(remote)
        condition_bytes=[str(c).encode('ascii') for c in conditions]
        index=archive.schema_arrow.get_field_index('market')
        wanted=pa.array(assets,type=pa.string())
        offset=0
        read_groups=0
        for i in range(archive.num_row_groups):
            group=archive.metadata.row_group(i)
            stats=group.column(index).statistics
            if stats and stats.has_min_max and not any(stats.min<=c<=stats.max for c in condition_bytes):
                offset+=group.num_rows
                continue
            identifiers=archive.read_row_group(i,columns=['asset_id'],use_threads=False)
            mask=pc.is_in(identifiers['asset_id'],value_set=wanted)
            if pc.any(mask).as_py():
                remote.cache_row_group(group)
                table=archive.read_row_group(i,use_threads=False)
                table=table.append_column('archive_row',pa.array(range(offset,offset+len(table)),type=pa.int64()))
                parts.append(table.filter(mask))
                read_groups+=1
            offset+=group.num_rows
        selected=pa.concat_tables(parts) if parts else pa.Table.from_batches([],schema=archive.schema_arrow.append(pa.field('archive_row',pa.int64())))
        metadata=dict(url=remote.url,etag=remote.etag,object_bytes=remote.size,assets=assets,
            row_groups=archive.num_row_groups,read_groups=read_groups,selected_rows=len(selected),
            transfer_bytes=budget.used,accounting='Per-hour charged logical ranges, including retry allowance; no whole-object staging.')
        archive.close()
    temporary=path.with_suffix('.parquet.partial')
    pq.write_table(selected,temporary,compression='zstd')
    temporary.replace(path)
    metadata['sha256']=sha256(path)
    write_json(manifest_path,metadata)
    print(f'{hour:%Y-%m-%dT%H}: {len(selected):,} selected records, {budget.used/1e6:.2f} MB charged',flush=True)
    return path,metadata


def reuse_cache(root,design):
    """Filter existing verified 200-token subsets; originals remain read-only."""
    folder=isolated(root/'cache/raw')
    folder.mkdir(parents=True,exist_ok=True)
    for name in ['may04','may06']:
        for original in (STUDY_ROOT/name/'raw').glob('*.parquet'):
            clock=pd.Timestamp(original.stem,tz='UTC')
            if not EARLIEST<=clock<pd.Timestamp(design['receipt_end']):
                continue
            path=folder/original.name
            if path.with_suffix('.json').exists():
                saved=json.loads(path.with_suffix('.json').read_text())
                if saved['sha256']!=sha256(path) or set(saved['assets'])!=set(design['assets']):
                    raise ValueError('Reused selected cache changed')
                continue
            parent=json.loads(original.with_suffix('.json').read_text())
            if sha256(original)!=parent['sha256']:
                raise ValueError('Prior pilot raw cache changed')
            table=pq.read_table(original,filters=[('asset_id','in',design['assets'])])
            pq.write_table(table,path,compression='zstd')
            metadata=dict(parent,assets=design['assets'],selected_rows=len(table),transfer_bytes=0,
                reused_from=str(original.relative_to(PROJECT_ROOT)),parent_sha256=parent['sha256'],sha256=sha256(path))
            write_json(path.with_suffix('.json'),metadata)


def seed_anchors(folder,assets,capture,start,receipt_end):
    """Read a complete contiguous receipt range and find pre-scan anchors."""
    latest={}
    for clock in pd.date_range(capture,receipt_end,freq='h',inclusive='left'):
        path=folder/f'{clock:%Y-%m-%dT%H}.parquet'
        if not path.with_suffix('.json').exists():
            raise ValueError(f'Unobserved receipt-hour gap: {clock}')
        metadata=json.loads(path.with_suffix('.json').read_text())
        if sha256(path)!=metadata['sha256']:
            raise ValueError('Selected history changed')
        frame=pd.read_parquet(path,columns=['asset_id','event_type','timestamp'])
        frame=frame.loc[frame.event_type.eq('book') & frame.timestamp.lt(start) & frame.timestamp.ge(EARLIEST)]
        for token,stamp in frame.groupby('asset_id').timestamp.max().items():
            if token not in assets:
                raise ValueError('Unexpected token in selected cache')
            latest[token]=max(latest.get(token,stamp),stamp)
    return latest


def anchors_sufficient(anchors,assets,capture):
    return set(anchors)==set(assets) and all(stamp>=capture for stamp in anchors.values())


def download(root=ROOT,workers=3):
    root=isolated(root)
    design=freeze(root)
    reuse_cache(root,design)
    ledger_path=root/'download_ledger.json'
    previous=json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
    budget=SharedBudget(design['maximum_new_range_bytes'],previous.get('charged_range_bytes',0))
    conditions=json.loads((root/'asset_conditions.json').read_text())
    folder=root/'cache/raw'
    # Resume the saved contiguous prefix rather than repeating the search.
    capture=pd.Timestamp(previous.get('capture_start',design['isolated_capture_start']))
    receipt_end=pd.Timestamp(design['receipt_end'])
    error=None
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while True:
                anchors=seed_anchors(folder,design['assets'],capture,START,receipt_end)
                if anchors_sufficient(anchors,design['assets'],capture) or capture==EARLIEST:
                    break
                clocks=[capture-pd.Timedelta(hours=i) for i in range(1,workers+1) if capture-pd.Timedelta(hours=i)>=EARLIEST]
                if shutil.disk_usage(root).free<2_000_000_000:
                    raise RuntimeError('Insufficient free space reserve')
                futures=[pool.submit(extract_selected,t,folder,design['assets'],set(conditions.values()),budget) for t in clocks]
                for future in futures:
                    future.result()
                capture=min(clocks)
                if sum(p.stat().st_size for p in folder.glob('*.parquet'))>design['maximum_selected_cache_bytes']:
                    raise RuntimeError('Selected cache allowance exceeded')
                write_json(ledger_path,dict(capture_start=capture,charged_range_bytes=budget.used,
                    snapshot_tokens_found=len(anchors),updated_at=pd.Timestamp.now(tz='UTC')))
                print(f'Continuous prefix now starts {capture}; {len(anchors)}/{len(design["assets"])} snapshot anchors found before next batch.',flush=True)
    except Exception as exc:
        error=str(exc)
        raise
    finally:
        write_json(ledger_path,dict(capture_start=capture,charged_range_bytes=budget.used,error=error,
            updated_at=pd.Timestamp.now(tz='UTC')))
    anchors=seed_anchors(folder,design['assets'],capture,START,receipt_end)
    result=dict(capture_start=capture,receipt_end=receipt_end,seeded_tokens=len(anchors),required_tokens=len(design['assets']),
        sufficient=anchors_sufficient(anchors,design['assets'],capture),latest_pre_scan_snapshots=anchors,
        charged_range_bytes=budget.used,raw_cache_bytes=sum(p.stat().st_size for p in folder.glob('*.parquet')),
        contiguous_receipt_hours=len(pd.date_range(capture,receipt_end,freq='h',inclusive='left')))
    write_json(root/'seed_search.json',result)
    materialize(root,design,ISOLATED,pd.Timestamp(design['isolated_capture_start']))
    materialize(root,design,CONTINUOUS,capture)
    return result


def materialize(root,design,name,capture):
    folder=isolated(root/name)
    folder.mkdir(exist_ok=True)
    raw=folder/'raw'
    raw.mkdir(exist_ok=True)
    for label in ['events','markets','packages']:
        target=folder/f'{label}.parquet'
        origin=root/f'{label}.parquet'
        if target.exists() and sha256(target)!=sha256(origin):
            raise ValueError('Diagnostic universe changed')
        if not target.exists():
            shutil.copy2(origin,target)
    parent=json.loads((STUDY_ROOT/'may06/plan.json').read_text())
    plan=dict(parent,start=design['start'],end=design['receipt_end'],warmup_start=capture.isoformat(),
        assets=design['assets'],asset_conditions=json.loads((root/'asset_conditions.json').read_text()),
        selection=design['selection'],diagnostic_scope=design['scope'],
        files_sha256={n:sha256(folder/f'{n}.parquet') for n in ['events','markets','packages']})
    path=folder/'plan.json'
    if path.exists() and json.loads(path.read_text())!=plan:
        raise ValueError('Diagnostic replay plan already frozen differently')
    if not path.exists():
        write_json(path,plan)
    for clock in pd.date_range(capture,design['receipt_end'],freq='h',inclusive='left'):
        for suffix in ['.parquet','.json']:
            origin=root/'cache/raw'/f'{clock:%Y-%m-%dT%H}{suffix}'
            target=raw/origin.name
            if not origin.exists():
                raise ValueError('Cannot materialize a gap in receipt history')
            if target.exists() and sha256(target)!=sha256(origin):
                raise ValueError('Materialized raw file changed')
            if not target.exists():
                shutil.copy2(origin,target)
    return folder


def replay(root=ROOT):
    from src.data.pmxt_revalidation import rerun
    from src.data.pmxt_diagnostics import audit
    root=isolated(root)
    design=freeze(root)
    for p,digest in design['rules_sha256'].items():
        if sha256(PROJECT_ROOT/p)!=digest:
            raise ValueError('Frozen replay rules changed')
    for name in [ISOLATED,CONTINUOUS]:
        rerun(root/name,analysis_end=design['analysis_end'])
        audit(root/name)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('step',choices=['freeze','download','replay'])
    parser.add_argument('--root',type=Path,default=ROOT)
    parser.add_argument('--workers',type=int,choices=range(1,5),default=3)
    args=parser.parse_args()
    result=download(args.root,args.workers) if args.step=='download' else globals()[args.step](args.root)
    if result is not None:
        print(json.dumps(result,indent=2,default=str))


if __name__=='__main__':
    main()
