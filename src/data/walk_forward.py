"""Explicit full-period Dome dataset for a separate chronological study.

Reuse the two already constructed label segments without changing their earlier
development/final access contracts. Later observations become training examples
only after their complete one-hour target windows have ended.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.common import sha256
from src.data.metadata import add_metadata
from src.data.opportunities import load_dataset
from src.data.final_evaluation import verify as verify_later
from src.paths import PROJECT_ROOT, PROCESSED_DIR, MARKETS_PARQUET, EVENT_UNIVERSE

ROOT=PROCESSED_DIR/'walk_forward_v1'
START=pd.Timestamp('2025-10-14T00:00:00Z')
FIRST=pd.Timestamp('2025-11-11T00:00:00Z')
BOUNDARY=pd.Timestamp('2026-01-08T00:00:00Z')
END=pd.Timestamp('2026-02-03T08:00:00Z')
HORIZON=pd.Timedelta(hours=1)


def write_json(path,value):
    Path(path).write_text(json.dumps(value,indent=2,default=str)+'\n',encoding='utf-8')


def build(output=ROOT):
    output=Path(output)
    development=PROCESSED_DIR/'opportunity_value'
    later=PROCESSED_DIR/'final_evaluation'
    prior=load_dataset(development)
    verify_later(later)
    future=pd.read_parquet(later/'examples.parquet')
    if not (prior.time.lt(BOUNDARY).all() and future.time.ge(BOUNDARY).all()):
        raise ValueError('Source segments overlap or change boundary')
    columns=prior.columns.tolist()
    frame=pd.concat([prior.assign(recording_segment='earlier'),future[columns].assign(recording_segment='later')],ignore_index=True)
    frame=add_metadata(frame).sort_values(['time','event_slug']).reset_index(drop=True)
    if frame.duplicated(['event_slug','time']).any() or not (frame.time.ge(START)&(frame.time+HORIZON).le(END)).all():
        raise ValueError('Invalid full-period event grid')
    known=frame.label_known.astype(bool)
    if not frame.loc[known,'target'].isin([0.,1.]).all() or not np.isfinite(frame.loc[known,'value_target']).all():
        raise ValueError('Invalid known opportunity targets')
    if frame.loc[~known,['target','value_target']].notna().any().any():
        raise ValueError('Unknown labels became observed targets')
    if not frame.value_target.gt(0).equals(frame.target.eq(1)):
        raise ValueError('Occurrence and quoted-value targets disagree')
    for name in ['m_event_type','m_topic','m_age_days']:
        check=future[['event_slug','time',name]].merge(frame[['event_slug','time',name]],on=['event_slug','time'],suffixes=('_old','_new'),validate='one_to_one')
        if name=='m_age_days':
            np.testing.assert_allclose(check[name+'_old'],check[name+'_new'],equal_nan=True)
        elif not check[name+'_old'].equals(check[name+'_new']):
            raise ValueError('Metadata contract changed between segments')
    output.mkdir(parents=True,exist_ok=False)
    sources=['src/data/walk_forward.py','src/data/metadata.py','src/data/opportunities.py','src/data/final_evaluation.py']
    design=dict(version=1,start=START,end_exclusive=END,first_origin=FIRST,horizon_minutes=60,
        original_segment_boundary=BOUNDARY,original_final_period_included=True,
        interpretation='Retrospective full-period model-development study; original completed experiments preserved separately.',
        targets='Original execution-independent occurrence and peak displayed one-basket value; unknowns remain missing.',
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in sources},
        inputs_sha256={str(p.relative_to(PROJECT_ROOT)):sha256(p) for p in [development/'complete.json',development/'examples.parquet',
            later/'complete.json',later/'examples.parquet',MARKETS_PARQUET,EVENT_UNIVERSE]},
        models_trained=False,labels_rebuilt=False)
    write_json(output/'design.json',design)
    frame.to_parquet(output/'examples.parquet',index=False)
    summary=frame.groupby('recording_segment').agg(rows=('time','size'),events=('event_slug','nunique'),
        known=('label_known','sum'),positive=('target','sum'),first=('time','min'),last=('time','max'))
    summary.to_csv(output/'population.csv')
    write_json(output/'complete.json',dict(files_sha256={p.name:sha256(p) for p in output.iterdir() if p.is_file()}))
    print(summary.to_string())


def load(output=ROOT):
    output=Path(output)
    design=json.loads((output/'design.json').read_text())
    complete=json.loads((output/'complete.json').read_text())
    for name,digest in complete['files_sha256'].items():
        if sha256(output/name)!=digest:
            raise ValueError(f'Walk-forward dataset changed: {name}')
    for section in ['source_sha256','inputs_sha256']:
        for name,digest in design[section].items():
            if sha256(PROJECT_ROOT/name)!=digest:
                raise ValueError(f'Walk-forward dependency changed: {name}')
    return pd.read_parquet(output/'examples.parquet')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT)
    build(parser.parse_args().output)
