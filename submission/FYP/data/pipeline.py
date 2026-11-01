"""Reconstruct scheduled event examples and observation labels from saved books."""
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd
from FYP.common import sha256
from FYP.config import ScreeningSpec
from FYP.paths import PROJECT_ROOT, PROCESSED_DIR

ROOT = PROCESSED_DIR
START = pd.Timestamp('2025-10-14T00:00:00Z')
FIRST = pd.Timestamp('2025-11-11T00:00:00Z')
END = pd.Timestamp('2026-02-03T08:00:00Z')
HORIZON = pd.Timedelta(hours=1)
# Coverage partitions preserve the audited recording endpoints. This boundary
# is independent of all model fitting, tuning and forward evaluation origins.
PARTITION = pd.Timestamp('2026-01-08T00:00:00Z')


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str)+'\n', encoding='utf-8')


def plans():
    result=json.loads((PROJECT_ROOT/'inputs/cohort.json').read_text())
    for p in result:
        p['paths']={t:PROJECT_ROOT/f for t,f in p['paths'].items()}
        p['packages']=[(ids,pd.Timestamp(a),pd.Timestamp(b)) for ids,a,b in p['packages']]
    from FYP.rules.baskets import validate_plan
    return [validate_plan(p) for p in result]


def validate(frame):
    if frame.duplicated(['event_slug','time']).any():
        raise ValueError('Duplicate event/scan')
    if not (frame.time.ge(START)&(frame.time+HORIZON).le(END)).all():
        raise ValueError('Example outside data window')
    known=frame.label_known.astype(bool)
    if not frame.loc[known,'target'].isin([0.,1.]).all():
        raise ValueError('Known occurrence target must be binary')
    if not np.isfinite(frame.loc[known,'value_target']).all():
        raise ValueError('Known value must be finite')
    if frame.loc[~known,['target','value_target']].notna().any().any():
        raise ValueError('Unknown outcomes must remain missing')
    if not frame.value_target.gt(0).equals(frame.target.eq(1)):
        raise ValueError('Occurrence and value disagree')
    forbidden={'pnl_target','known_pnl_usd','activity_target','execution_profit'}
    if forbidden&set(frame):
        raise ValueError('Execution outcomes cannot enter opportunity training')
    return frame


def manifest(output=ROOT):
    output=Path(output)
    paths=[p for p in output.rglob('*') if p.is_file() and p.name!='complete.json']
    write_json(output/'complete.json',dict(
        files_sha256={p.relative_to(output).as_posix():sha256(p) for p in paths},
        input_sha256={p.relative_to(PROJECT_ROOT).as_posix():sha256(p)
            for p in (PROJECT_ROOT/'inputs').glob('*') if p.is_file()},
        source_sha256={p.relative_to(PROJECT_ROOT).as_posix():sha256(p)
            for p in [*(p for folder in ('data','rules','simulator') for p in (PROJECT_ROOT/'FYP'/folder).glob('*.py')),
                PROJECT_ROOT/'FYP/config.py',PROJECT_ROOT/'FYP/common.py',PROJECT_ROOT/'FYP/paths.py']},
        coverage_partition=str(PARTITION),execution_outcomes_used=False))


def load(output=ROOT):
    output=Path(output)
    complete=json.loads((output/'complete.json').read_text())
    for name,digest in complete['files_sha256'].items():
        if sha256(output/name)!=digest:raise ValueError(f'Prepared artifact changed: {name}')
    for section in ('input_sha256','source_sha256'):
        for name,digest in complete[section].items():
            if sha256(PROJECT_ROOT/name)!=digest:raise ValueError(f'Preparation dependency changed: {name}')
    return validate(pd.read_parquet(output/'examples.parquet'))


def rebuild(output, event_slugs=None):
    """Build from raw parquet; never read saved features, labels or predictions.

    An optional event subset supports bounded pipeline checks. The complete
    study uses all frozen plans. Outputs must go to a new directory.
    """
    from FYP.data.books import RawGroup
    from FYP.data.preparation import BoundedGroup, sample_features, quote_labels
    from FYP.data.features import add_hourly_history
    from FYP.data.labels import event_labels
    from FYP.data.value import peak_quotes
    from FYP.data.metadata import add_metadata
    output=Path(output)
    output.mkdir(parents=True,exist_ok=True)
    signature=dict(source={p.relative_to(PROJECT_ROOT).as_posix():sha256(p)
        for p in [*(p for folder in ('data','rules','simulator') for p in (PROJECT_ROOT/'FYP'/folder).glob('*.py')),
            PROJECT_ROOT/'FYP/config.py',PROJECT_ROOT/'FYP/common.py',PROJECT_ROOT/'FYP/paths.py']},
        inputs={p.name:sha256(p) for p in (PROJECT_ROOT/'inputs').glob('*') if p.is_file()},
        events=sorted(event_slugs) if event_slugs is not None else None)
    signature_path=output/'reconstruction.json'
    if signature_path.exists() and json.loads(signature_path.read_text())!=signature:
        raise ValueError('Reconstruction dependencies changed; use a new directory')
    write_json(signature_path,signature)
    inventory=pd.read_csv(PROJECT_ROOT/'inputs/raw_inventory.csv',dtype={'token_id':str}).set_index('token_id')
    extents={t:{k:pd.Timestamp(r[k]) for k in ('first','last')} for t,r in inventory.iterrows()}
    pieces=[]
    for plan in plans():
        if event_slugs is not None and plan['event_slug'] not in event_slugs:continue
        checkpoint=output/'events'/plan['event_slug']
        if (checkpoint/'complete.json').exists():
            for name,digest in json.loads((checkpoint/'complete.json').read_text()).items():
                if sha256(checkpoint/name)!=digest:raise ValueError('Event reconstruction checkpoint changed')
            pieces.append(pd.read_parquet(checkpoint/'examples.parquet'))
            continue
        print('Raw reconstruction:',plan['event_slug'],flush=True)
        signals_for_event=[];event_pieces=[]
        for t,path in plan['paths'].items():
            if sha256(path)!=inventory.loc[t,'input_sha256']:raise ValueError('Raw input digest changed')
        for lo,hi,terminal in ((START,PARTITION,False),(PARTITION,END,True)):
            local=dict(plan,packages=[(ids,max(a,lo),min(b,hi)) for ids,a,b in plan['packages'] if max(a,lo)<min(b,hi)])
            if not local['packages']:continue
            spec=ScreeningSpec(data_start=str(lo),data_end=str(hi))
            context=hi+pd.Timedelta(seconds=1.1) if terminal else hi
            frames={}
            for token,path in plan['paths'].items():
                seed=lo
                if terminal:
                    before=pd.read_parquet(path,columns=['snapshot_time'],filters=[('snapshot_time','<',lo-HORIZON)])
                    seed=before.snapshot_time.max() if len(before) else lo-HORIZON
                f=pd.read_parquet(path,columns=RawGroup.COLUMNS,filters=[('snapshot_time','>=',seed),('snapshot_time','<=' if terminal else '<',context)])
                f.attrs['recording_end']=extents[token]['last']
                frames[token]=f
            if not terminal and any(f.empty for f in frames.values()):continue
            if terminal:
                selected=[]
                for when in pd.date_range(lo,hi,freq=spec.frequency,inclusive='left'):
                    active=[ids for ids,a,b in local['packages'] if a<=when<b]
                    ids={t for members in active for t in members}
                    if ids and all(extents[t]['first']<=when for t in ids) and when<=max(extents[t]['last'] for t in ids):selected.append(when)
                windows=pd.DatetimeIndex(selected)
                if not len(windows):continue
                times=windows.union(windows-HORIZON)
                # Preserve pre-activation histories for lagged scan features.
                feature_plan=plan
                first_grid=min(extents[t]['first'] for t in frames).ceil(spec.frequency)
                times=times[times>=first_grid]
            else:
                start=min(f.snapshot_time.min() for f in frames.values()).ceil(spec.frequency)
                stop=min(max(f.snapshot_time.max() for f in frames.values()),hi)
                times=pd.date_range(start,stop,freq=spec.frequency)
                feature_plan=local
            feature=sample_features(feature_plan,frames,times,context)
            if feature.empty:continue
            feature=add_hourly_history(feature,spec)
            feature=feature.loc[feature.eligible_snapshot & feature.time.ge(lo) & (feature.time+spec.horizon).le(hi)]
            if terminal:feature=feature.loc[feature.time.isin(windows)]
            if feature.empty:continue
            windows=pd.DatetimeIndex(feature.time)
            if terminal:
                labels,signals,peaks=quote_labels(local,frames,windows,spec,context)
            else:
                labels,signals=event_labels(local,frames,windows,spec)
                peaks=peak_quotes(local,frames,signals)
                values=peaks.groupby('time').peak_quoted_usd.max() if len(peaks) else pd.Series(dtype=float)
                labels['witnessed_peak_quoted_usd']=labels.time.map(values).fillna(0.)
                labels['value_target']=labels.witnessed_peak_quoted_usd.where(labels.label_known)
            event_pieces.append(feature.merge(labels,on='time',validate='one_to_one'))
            if len(signals):signals_for_event.append(signals.assign(window='study'))
        if event_pieces:
            folder=checkpoint;folder.mkdir(parents=True,exist_ok=True)
            event_frame=pd.concat(event_pieces,ignore_index=True)
            event_frame['time']=pd.to_datetime(event_frame.time,utc=True)
            event_frame['first_opportunity_time']=pd.to_datetime(event_frame.first_opportunity_time,utc=True)
            event_frame.to_parquet(folder/'examples.parquet',index=False)
            pieces.append(event_frame)
        if signals_for_event:
            pd.concat(signals_for_event,ignore_index=True).to_parquet(folder/'signals.parquet',index=False)
        if event_pieces:
            write_json(folder/'complete.json',{p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name!='complete.json'})
    if not pieces:raise ValueError('No reconstructible event examples')
    frame=pd.concat(pieces,ignore_index=True)
    frame['time']=pd.to_datetime(frame.time,utc=True)
    frame=add_metadata(frame).sort_values(['time','event_slug']).reset_index(drop=True)
    schema=json.loads((PROJECT_ROOT/'inputs/example_schema.json').read_text())
    frame=frame[schema['columns']]
    validate(frame).to_parquet(output/'examples.parquet',index=False)
    manifest(output)
    return frame
