"""Verify and present the exploratory shorter-training Dome study."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.data.metadata import add_metadata
from src.data.opportunities import load_dataset
from src.models.estimators import predict
from src.models.evaluation import ranked
from src.models.opportunity_value import value_metrics
from src.models.validation_horizon import OUTPUT, ORIGINS, COMMON_START, END, HORIZON, training_rows, forward_rows, write_json
from src.paths import PROJECT_ROOT, PROCESSED_DIR

LABELS = {'quoted_gap_rule':'Quoted-gap', 'fixed_book_logistic':'Logistic',
    'fixed_book_random_forest':'Random forest', 'fixed_book_xgboost':'XGBoost',
    'forest_two_stage':'Forest two-stage', 'tabm_two_stage':'TabM two-stage',
    'rank_ensemble':'Rank ensemble', 'depth_rule':'Depth', 'freshness_rule':'Freshness'}


def verify(output=OUTPUT, *, reproduce_scores=False):
    output=Path(output)
    design=json.loads((output/'design.json').read_text())
    complete=json.loads((output/'complete.json').read_text())
    if complete['original_final_outcomes_used'] or complete['original_final_protocol_replaced']:
        raise ValueError('This is a separate development-only study')
    for name,digest in complete['files_sha256'].items():
        if sha256(output/name)!=digest:
            raise ValueError(f'Changed study artifact: {name}')
    for section in ('source_sha256','metadata_sha256'):
        for name,digest in design[section].items():
            if sha256(PROJECT_ROOT/name)!=digest:
                raise ValueError(f'Changed source/input: {name}')
    data=PROCESSED_DIR/'opportunity_value'
    for path,key in [(data/'examples.parquet','dataset_sha256'),(data/'complete.json','dataset_completion_sha256'),
            (PROJECT_ROOT/'results/final_protocol/protocol.json','original_final_protocol_sha256'),
            (PROJECT_ROOT/'results/final_preparation/complete.json','original_final_models_completion_sha256')]:
        if sha256(path)!=design[key]:
            raise ValueError(f'Changed original evidence: {path}')
    frame=add_metadata(load_dataset(data)).sort_values(['time','event_slug']).reset_index(drop=True)
    predictions=pd.read_parquet(output/'predictions.parquet')
    metrics=pd.read_csv(output/'metrics.csv')
    audit=pd.read_csv(output/'fit_audit.csv')
    epochs=pd.read_csv(output/'epoch_selection.csv')
    if not (pd.to_datetime(audit.label_end_last,utc=True)<pd.to_datetime(audit.cutoff,utc=True)).all():
        raise ValueError('Training horizon overlaps forward validation')
    if not (pd.to_datetime(epochs.inner_train_last,utc=True)+HORIZON<pd.to_datetime(epochs.validation_start,utc=True)).all():
        raise ValueError('Inner training overlap')
    if not (pd.to_datetime(epochs.validation_last,utc=True)+HORIZON<pd.to_datetime(epochs.cutoff,utc=True)).all():
        raise ValueError('Epoch selection uses forward labels')
    for origin,boundary in ORIGINS.items():
        train=training_rows(frame,pd.Timestamp(boundary))
        test=forward_rows(frame,pd.Timestamp(boundary))
        for row in audit.loc[audit.origin.eq(origin)].itertuples():
            actual=train.loc[train.target.eq(1)] if row.task=='amount' else train
            if row.training_rows!=len(actual) or pd.Timestamp(row.train_last)!=actual.time.max():
                raise ValueError('Fitted population differs from intended split')
        for name,part in predictions.loc[predictions.origin.eq(origin)].groupby('model'):
            pd.testing.assert_frame_equal(part[['event_slug','time']].reset_index(drop=True),test[['event_slug','time']].reset_index(drop=True))
            np.testing.assert_allclose(part.target,test.target,equal_nan=True)
            np.testing.assert_allclose(part.value_target,test.value_target,equal_nan=True)
            for scope,subset in [('full_forward',part),('common_forward',part.loc[part.time.ge(COMMON_START)])]:
                for n in (3,5):
                    saved=metrics.loc[metrics.origin.eq(origin)&metrics.model.eq(name)&metrics.scope.eq(scope)&metrics.top_n.eq(n)].iloc[0]
                    calculated=value_metrics(subset,subset.score,n)
                    for key,value in calculated.items():
                        if not np.isclose(saved[key],value,equal_nan=True):
                            raise ValueError(f'Cannot reproduce {origin}/{name}/{key}')
        if reproduce_scores:
            expected={}
            book,combined=design['feature_views']['book'],design['feature_views']['combined']
            for name in ('logistic','random_forest','xgboost'):
                key='fixed_book_'+name
                model=joblib.load(output/'fitted_models'/f'{origin}_{key}.joblib')
                if list(model.feature_names_in_)!=book:
                    raise ValueError('Classical feature mismatch')
                expected[key]=predict(model,test[book])
            forest=joblib.load(output/'fitted_models'/f'{origin}_forest_two_stage.joblib')
            expected['forest_two_stage']=forest.predict(test[combined])
            members=[]
            for seed in design['seeds']:
                gate=joblib.load(output/'fitted_models'/f'{origin}_tabm_classifier_{seed}.joblib')
                amount=joblib.load(output/'fitted_models'/f'{origin}_tabm_amount_{seed}.joblib')
                if list(gate.feature_names_in_)!=combined or list(amount.feature_names_in_)!=combined:
                    raise ValueError('TabM feature mismatch')
                members.append(gate.predict(test[combined])*amount.predict(test[combined]))
            expected['tabm_two_stage']=np.mean(members,axis=0)
            expected['rank_ensemble']=np.mean([test[['time']].assign(score=expected[name]).groupby('time').score.rank(pct=True).to_numpy()
                for name in ('fixed_book_logistic','fixed_book_random_forest','tabm_two_stage')],axis=0)
            expected['quoted_gap_rule']=test.x_best_quoted_net_usd.fillna(-1e12).to_numpy()
            expected['depth_rule']=test.x_ask_depth_sum.fillna(-1e12).to_numpy()
            expected['freshness_rule']=-test.x_book_age_max_s.fillna(1e12).to_numpy()
            for name,score in expected.items():
                saved=predictions.loc[predictions.origin.eq(origin)&predictions.model.eq(name)]
                np.testing.assert_allclose(saved.score,score,rtol=1e-7,atol=1e-9)
    return design


def analyze(output=OUTPUT):
    output=Path(output)
    verify(output,reproduce_scores=True)
    analysis=output/'analysis'
    analysis.mkdir(exist_ok=True)
    predictions=pd.read_parquet(output/'predictions.parquet')
    metrics=pd.read_csv(output/'metrics.csv')
    common=metrics.loc[metrics.scope.eq('common_forward')&metrics.top_n.eq(3)]
    common.to_csv(analysis/'primary_comparison.csv',index=False)
    groups=[]
    sensitivity=[]
    counts=[]
    for origin,pool in predictions.loc[predictions.time.ge(COMMON_START)].groupby('origin'):
        reference=pool.loc[pool.model.eq('quoted_gap_rule')]
        counts.append(dict(origin=origin, scheduled_hours=int((END-COMMON_START)/HORIZON),
            nonempty_hours=reference.time.nunique(), events=reference.event_slug.nunique(),
            positive_hours=int(reference.target.eq(1).sum()), known_hours=int(reference.label_known.sum()),
            unknown_hours=int((~reference.label_known).sum()),
            peak_value_sum=float(reference.value_target.sum())))
        for name,part in pool.groupby('model'):
            selected=ranked(part,part.score,3)
            for key,g in part.groupby('event_slug'):
                s=selected.loc[selected.event_slug.eq(key)]
                groups.append(dict(origin=origin,model=name,event_slug=key,
                    positives=int(g.target.eq(1).sum()),captured=int(s.target.eq(1).sum()),
                    peak_value=float(g.value_target.sum()),captured_value=float(s.value_target.sum())))
            # Same declared event exclusion used in earlier development reports.
            reduced=part.loc[part.event_slug.ne('fed-decision-in-december')]
            sensitivity.append(dict(origin=origin,model=name,scope='exclude_december_fed_reranked',**value_metrics(reduced,reduced.score,3)))
    pd.DataFrame(groups).to_csv(analysis/'event_capture.csv',index=False)
    pd.DataFrame(sensitivity).to_csv(analysis/'exclude_december_fed.csv',index=False)
    pd.DataFrame(counts).to_csv(analysis/'common_population.csv',index=False)
    fig,axes=plt.subplots(1,2,figsize=(13,6),layout='constrained')
    names=list(LABELS)
    for ax,metric,title in zip(axes,('recall','peak_value_recall'),('Known positive-hour capture','Peak quoted-value capture')):
        y=np.arange(len(names))
        for offset,origin,color,label in [(-.18,'train_14d','#4477AA','14-day training'),(.18,'train_28d','#EE7733','28-day training')]:
            values=common.loc[common.origin.eq(origin)].set_index('model').reindex(names)[metric].mul(100)
            ax.barh(y+offset,values,height=.32,color=color,label=label)
            for at,v in zip(y+offset,values):
                ax.text(v+1,at,f'{v:.1f}%',va='center',fontsize=8)
        ax.set_yticks(y,[LABELS[n] for n in names]);ax.invert_yaxis()
        ax.set_xlim(0,113);ax.set_xlabel('Percent captured');ax.set_title(title)
    handles,labels=axes[0].get_legend_handles_labels()
    fig.legend(handles,labels,loc='outside lower center',ncol=2,fontsize=9)
    fig.suptitle('Dome exploratory study: common 11 Nov–7 Jan evaluation, N=3')
    fig.savefig(analysis/'primary_capture.png',dpi=150);plt.close(fig)
    figure,axes=plt.subplots(1,2,figsize=(12,4),layout='constrained')
    for ax,origin in zip(axes,ORIGINS):
        part=predictions.loc[predictions.origin.eq(origin)&predictions.model.eq('quoted_gap_rule')]
        count=part.groupby('time').size()
        ax.plot(count.index,count,color='#4477AA',linewidth=.8)
        ax.axhline(3,color='#CC3311',linestyle='--',label='N=3')
        ax.axhline(5,color='gray',linestyle=':',label='N=5')
        ax.set_title(origin.replace('_',' '));ax.set_ylabel('Eligible events');ax.tick_params(axis='x',rotation=25);ax.legend()
    figure.savefig(analysis/'candidate_pool.png',dpi=140);plt.close(figure)
    # Check original protected files, permitting the expressly added Dome study.
    # The older PMXT exact-inventory guard intentionally also rejects additions.
    before=json.loads((PROJECT_ROOT/'data/pmxt/pilot_v1/dome_before.json').read_text())
    changed=[p for p,digest in before.items() if not Path(p).is_file() or sha256(Path(p))!=digest]
    if changed:
        raise ValueError(f'Original protected artifacts changed: {changed[:3]}')
    write_json(analysis/'preservation.json',dict(original_files_checked=len(before),original_files_unchanged=True,
        new_study_added=True,original_final_protocol_replaced=False))
    write_json(analysis/'manifest.json',dict(study_completion_sha256=sha256(output/'complete.json'),
        reporter_sha256=sha256(Path(__file__)),scores_reproduced_from_saved_models=True,
        files_sha256={p.name:sha256(p) for p in analysis.iterdir() if p.is_file() and p.name!='manifest.json'}))
    print('Verified all saved predictions, splits and metrics; original protected files unchanged:',len(before))
    print(pd.DataFrame(sensitivity)[['origin','model','positive_windows','recall','peak_value_recall']].to_string(index=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=OUTPUT)
    args=parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        analyze(args.output)
