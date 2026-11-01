"""Verify models against their actual chronological populations and saved scores."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
from FYP.common import sha256
from FYP.paths import PROJECT_ROOT
from FYP.data.pipeline import ROOT as DATA, FIRST, END, HORIZON, load
from FYP.models.value import value_metrics
from FYP.experiments.study import OUTPUT as MODELS, folds, train_before, score_saved, choose
from FYP.experiments.economics import OUTPUT as REPLAY, verify_models
def verify(models=MODELS,data=DATA,*,reproduce_scores=False):
    models,data=Path(models),Path(data)
    design=verify_models(models)
    if sha256(data/'complete.json')!=design['dataset_completion_sha256']:
        raise ValueError('Weekly dataset completion changed')
    frame=load(data).sort_values(['time','event_slug']).reset_index(drop=True)
    full=pd.read_parquet(models/'predictions.parquet')
    metrics=pd.read_csv(models/'metrics.csv')
    audit_count=0
    for origin,start,end in folds():
        folder=models/'folds'/origin
        complete=json.loads((folder/'complete.json').read_text())
        for name,digest in complete['files_sha256'].items():
            if sha256(folder/name)!=digest:raise ValueError('Weekly checkpoint changed')
        train=train_before(frame,start)
        test=frame.loc[frame.time.ge(start)&frame.time.lt(end)]
        bundle=json.loads((folder/'bundle.json').read_text())
        if pd.Timestamp(bundle['trained_before'])!=start:
            raise ValueError('Bundle fitting origin mismatch')
        if any(pd.Timestamp(s['tuned_before'])>start for s in bundle['selected'].values()):
            raise ValueError('Parameters selected using future observations')
        audit=pd.read_csv(folder/'fit_audit.csv')
        for row in audit.itertuples():
            target=train.loc[train.value_target.gt(0)] if row.task=='amount' else train
            if row.training_rows!=len(target) or pd.Timestamp(row.train_last)!=target.time.max() or pd.Timestamp(row.label_end_last)>=start:
                raise ValueError('Invalid weekly training population')
            expected=design['feature_views']['book' if row.model.startswith('fixed_book_') else 'combined']
            if json.loads(row.features)!=expected:raise ValueError('Fitted feature view changed')
            audit_count+=1
        saved=full.loc[full.fold.eq(origin)]
        scores=score_saved(folder,bundle,test) if reproduce_scores else None
        for name,part in saved.groupby('model'):
            pd.testing.assert_frame_equal(part[['event_slug','time']].reset_index(drop=True),test[['event_slug','time']].reset_index(drop=True))
            np.testing.assert_allclose(part.target,test.target,equal_nan=True)
            np.testing.assert_allclose(part.value_target,test.value_target,equal_nan=True)
            if scores is not None:np.testing.assert_allclose(part.score,scores[name],rtol=1e-7,atol=1e-9)
    for folder in sorted((models/'tuning').iterdir()):
        selected=json.loads((folder/'selected.json').read_text())
        search=pd.read_csv(folder/'search.csv')
        for method,selection in selected.items():
            part=search.loc[search.method.eq(method)]
            if choose(part.to_dict('records'))!=selection['candidate']:
                raise ValueError('Tuning choice does not reproduce')
            for row in part.itertuples():
                lo,hi,cutoff=pd.Timestamp(row.validation_start),pd.Timestamp(row.validation_end),pd.Timestamp(row.fitting_cutoff)
                if pd.Timestamp(row.train_last)+HORIZON>=lo or hi+HORIZON>cutoff:
                    raise ValueError('Inner chronological separation failed')
                path=folder/f'{method}_c{row.candidate}_i{row.inner}.parquet'
                if sha256(path)!=row.predictions_sha256:raise ValueError('Tuning scores changed')
                inner=pd.read_parquet(path)
                if not (inner.time.ge(lo)&inner.time.lt(hi)).all():raise ValueError('Invalid inner window')
                metric=value_metrics(inner,inner.score,3)
                for key in ('total_peak_value','captured_peak_value','positive_windows','captured_positive_windows','average_precision'):
                    if not np.isclose(getattr(row,key),metric[key],equal_nan=True):raise ValueError('Inner metric does not reproduce')
    for row in metrics.itertuples():
        part=full.loc[full.model.eq(row.model)]
        if row.scope=='exclude_december_fed':part=part.loc[part.event_slug.ne('fed-decision-in-december')]
        if row.fold!='all':part=part.loc[part.fold.eq(row.fold)]
        calculated=value_metrics(part,part.score,row.top_n)
        for key in ('recall','peak_value_recall','positive_windows','captured_positive_windows','selected_unknown'):
            if not np.isclose(getattr(row,key),calculated[key],equal_nan=True):raise ValueError('Forward metric does not reproduce')
    return dict(weekly_fits_checked=audit_count,scores_reproduced=reproduce_scores,folds=len(folds()))

def verify_replay(folder=REPLAY):
    folder=Path(folder)
    complete=json.loads((folder/'complete.json').read_text())
    design=json.loads((folder/'design.json').read_text())
    for name,digest in complete['files_sha256'].items():
        if sha256(folder/name)!=digest:raise ValueError('Replay artifact changed')
    for section in ['source_sha256','evidence_sha256']:
        for name,digest in design[section].items():
            if sha256(PROJECT_ROOT/name)!=digest:raise ValueError('Replay dependency changed')
    if design['execution']['delay_s']!=.1 or design['execution']['cash_cap_usd'] is not None or design['execution']['max_quantity'] is not None:
        raise ValueError('Primary economics contract changed')
    if complete['weekly_ledger_resets']:raise ValueError('Weekly replay reset its ledger')
    return design
