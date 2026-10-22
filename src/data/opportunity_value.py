"""Add peak displayed basket margin as an execution-independent value target."""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.common import sha256
from src.data.books import RawGroup, ObservationGroup
from src.data.io import load_frames
from src.data.opportunities import load_dataset
from src.data.universe import build_plan
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.simulator.sizing import choose_order


def peak_quotes(plan, frames, signals):
    """Maximum one-basket margin per hour, never a sum of repeated snapshots.

    Re-evaluate every member update inside each previously qualified spell.
    Different ladder pairs may share liquidity: their maxima are not added.
    """
    columns=['time','pair_id','peak_quoted_usd','peak_time','quantity','states_checked']
    if signals.empty:
        return pd.DataFrame(columns=columns)
    frames={token:RawGroup([frame]).frames[0] for token,frame in frames.items()}
    rows=[]
    for pair,part in signals.groupby('pair_id'):
        number=int(pair.rsplit(':',1)[1])
        ids=plan['packages'][number][0]
        group=ObservationGroup([frames[token] for token in ids])
        updates=pd.DatetimeIndex([])
        for token in ids:
            updates=updates.union(pd.DatetimeIndex(frames[token].snapshot_time))
        for when,hour in part.groupby('time'):
            best,peak,quantity,checked=0.,pd.NaT,0,0
            for spell in hour.itertuples():
                grid=pd.DatetimeIndex([spell.start_time]).union(
                    updates[(updates>spell.start_time)&(updates<spell.end_time)]).sort_values()
                for time in grid:
                    books=group.at(time)
                    order=None if books is None else choose_order(books,min_quantity=5,
                        max_quantity=None,cash_cap=None,min_limit_margin=0.)
                    checked+=1
                    if order is None:
                        raise ValueError('Qualified spell contains an unqualified quote')
                    if order['quoted_gross_usd']>best:
                        best,peak,quantity=order['quoted_gross_usd'],time,order['quantity']
            rows.append(dict(time=when,pair_id=pair,peak_quoted_usd=best,
                peak_time=peak,quantity=quantity,states_checked=checked))
    return pd.DataFrame(rows,columns=columns)


def build_dataset(output, parent):
    output,parent=Path(output),Path(parent)
    frame=load_dataset(parent)
    design=json.loads((parent/'design.json').read_text())
    design.update(version='observation_opportunity_value_v1',
        observation_dataset_completion_sha256=sha256(parent/'complete.json'),
        value_target='Maximum qualifying displayed dollar margin of one basket at one observed state in the next window; zero when known and absent',
        value_interpretation='Opportunity magnitude proxy, not total hourly profit or achievable execution P&L',
        value_aggregation='Maximum over times and packages; never add repeated liquidity or overlapping ladder pairs')
    design['source_sha256']['src/data/opportunity_value.py']=sha256(PROJECT_ROOT/'src/data/opportunity_value.py')
    output.mkdir(parents=True,exist_ok=False)
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    expected=pd.read_csv(parent/'input_inventory.csv',dtype={'token_id':str})
    expected={(r.event_slug,r.token_id):r.input_sha256 for r in expected.itertuples()}
    plans,_=build_plan('all')
    pieces=[]
    for plan in plans:
        slug=plan['event_slug']
        path=parent/'events'/slug/'signals.parquet'
        if not path.exists():
            continue
        signals=pd.read_parquet(path)
        if signals.empty:
            continue
        print(f'Quoted opportunity magnitude: {slug}',flush=True)
        for token,path in plan['paths'].items():
            if sha256(path)!=expected[(slug,token)]:
                raise ValueError('Raw input differs from occurrence labels')
        pieces.append(peak_quotes(plan,load_frames(plan),signals).assign(event_slug=slug))
    peaks=pd.concat(pieces,ignore_index=True)
    peaks.to_parquet(output/'package_peaks.parquet',index=False)
    values=peaks.groupby(['event_slug','time']).peak_quoted_usd.max().rename('witnessed_peak_quoted_usd')
    frame=frame.merge(values,on=['event_slug','time'],how='left',validate='one_to_one')
    frame['witnessed_peak_quoted_usd']=frame.witnessed_peak_quoted_usd.fillna(0.)
    frame['value_target']=frame.witnessed_peak_quoted_usd.where(frame.label_known)
    if not frame.value_target.gt(0).equals(frame.target.eq(1)):
        raise ValueError('Occurrence and value targets disagree')
    frame.to_parquet(output/'examples.parquet',index=False)
    files={p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(examples_sha256=sha256(output/'examples.parquet'),
        files_sha256=files,execution_outcomes_used=False,final_lockbox_opened=False),indent=2)+'\n')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--parent',type=Path,default=PROCESSED_DIR/'opportunities')
    args=parser.parse_args()
    build_dataset(args.output,args.parent)
