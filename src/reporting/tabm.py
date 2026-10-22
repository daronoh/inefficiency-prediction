"""Audited TabM comparisons, uncertainty, concentration and economic sensitivity."""
import argparse
import json
from pathlib import Path
from time import perf_counter

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, mean_absolute_error, mean_squared_error
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.config import ScreeningSpec
from src.data.metadata import add_metadata
from src.data.opportunities import load_dataset
from src.models.evaluation import ranked
from src.models.opportunities import split_masks
from src.models.tabm_study import verify, write_json, REPLAY_POLICIES, SEEDS
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.reporting.opportunities import group_capture, paired_family_intervals
from src.reporting.opportunity_replay import accounted, verify as verify_old_replay

LABELS={
    'quoted_gap_rule':'Quoted-gap rule','fixed_book_logistic':'Book logistic',
    'fixed_book_random_forest':'Book forest','fixed_combined_random_forest':'Forest + metadata',
    'tuned_combined_xgboost':'Tuned XGBoost','tuned_value_two_stage_random_forest':'Tuned two-stage forest',
    'tabm_fixed_book_classifier':'TabM occurrence: book only',
    'tabm_fixed_classifier':'TabM occurrence: fixed',
    'tabm_tuned_classifier':'TabM occurrence: tuned / 3 seeds',
    'tabm_fixed_regression':'TabM value: fixed',
    'tabm_tuned_regression':'TabM value: tuned / 3 seeds',
    'tabm_fixed_two_stage':'TabM two-stage: fixed',
    'tabm_tuned_two_stage':'TabM two-stage: tuned / 3 seeds',
    'tabm_forest_rank_ensemble':'TabM + forest rank ensemble',
    'monitor_all':'Monitor all','random_mean':'Random mean'}
COMPARATORS=('quoted_gap_rule','fixed_book_logistic','fixed_book_random_forest',
             'fixed_combined_random_forest','tuned_combined_xgboost','tuned_value_two_stage_random_forest')


def audit_fits(root,design,prediction):
    """Check actual saved objective, fitting population and reproduced predictions."""
    frame=add_metadata(load_dataset(PROCESSED_DIR/'opportunity_value'))
    spec=ScreeningSpec(**design['window'])
    fits=pd.read_csv(root/'fit_audit.csv').dropna(subset=['artifact'])
    selected=pd.read_csv(root/'selected_parameters.csv')
    rows=[]
    for fold,start,end in design['folds']:
        start,end=pd.Timestamp(start,tz='UTC'),pd.Timestamp(end,tz='UTC')
        a,b=split_masks(frame,start,end,spec.horizon)
        training=frame.loc[a]
        sample=frame.loc[b].sample(n=min(32,int(b.sum())),random_state=2026)
        cache={}
        for row in fits.loc[fits.fold.eq(fold)].itertuples():
            model=joblib.load(root/'fitted_models'/row.artifact)
            expected=training.loc[training.value_target.gt(0)] if row.task=='amount' else training
            if model.training_rows_!=len(expected) or model.task!=('classifier' if row.task=='classifier' else 'regression'):
                raise ValueError('Fitted objective or training population differs')
            if (model.k!=32 or model.seed!=row.seed or model.params!=design['candidates'][int(row.candidate)]
                or model.history_ or (model.constant_ is None and model.best_epoch_!=row.epochs)):
                raise ValueError('Saved architecture, seed or refit protocol differs')
            if row.task!='classifier' and not np.isclose(model.target_mean_,np.asarray(expected.value_target,dtype=np.float32).mean()):
                raise ValueError('Model target scaling does not match opportunity values')
            cols=design['feature_views'][row.view]
            cache[(row.task,int(row.candidate),int(row.seed),row.view)]=model.predict(sample[cols])
        def component(task,candidate,seed,view='combined'):
            return cache[(task,candidate,seed,view)]
        scores={'tabm_fixed_book_classifier':component('classifier',0,SEEDS[0],'book')}
        for method in ('classifier','regression','two_stage'):
            best=int(selected.loc[selected.fold.eq(fold)&selected.method.eq(method),'candidate'].iloc[0])
            for prefix,candidate,seeds in [('fixed',0,[SEEDS[0]]),('tuned',best,SEEDS)]:
                arrays=[]
                for seed in seeds:
                    score=(component('classifier',candidate,seed)*component('amount',candidate,seed)
                           if method=='two_stage' else component(method,candidate,seed))
                    arrays.append(score)
                    if prefix=='tuned':
                        scores[f'tabm_{method}_seed{seed-SEEDS[0]}']=score
                scores[f'tabm_{prefix}_{method}']=np.mean(arrays,axis=0)
        for name,score in scores.items():
            saved=prediction.loc[prediction.fold.eq(fold)&prediction.model.eq(name)]
            saved=saved.set_index(['event_slug','time']).reindex(pd.MultiIndex.from_frame(sample[['event_slug','time']])).score.to_numpy()
            if not np.allclose(score,saved,rtol=1e-5,atol=1e-6):
                raise ValueError(f'Saved TabM predictions do not reproduce: {fold}, {name}')
            rows.append(dict(fold=fold,model=name,samples=len(sample),maximum_error=float(np.abs(score-saved).max())))
    return pd.DataFrame(rows)


def verify_replay(root,models,baseline):
    design=json.loads((root/'design.json').read_text())
    complete=json.loads((root/'complete.json').read_text())
    for flag in ('models_retrained','labels_rebuilt','final_lockbox_opened'):
        if complete[flag] or design[flag]:
            raise ValueError('Expected fixed development replay')
    for p,digest in complete['files_sha256'].items():
        if sha256(root/p)!=digest:
            raise ValueError(f'Replay artifact changed: {p}')
    for p,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/p)!=digest:
            raise ValueError(f'Replay source changed: {p}')
    for path,key in [(models/'complete.json','model_completion_sha256'),
                     (models/'predictions.parquet','predictions_sha256'),
                     (baseline/'complete.json','baseline_completion_sha256'),
                     (PROCESSED_DIR/'opportunities/complete.json','observation_completion_sha256')]:
        if sha256(path)!=design[key]:
            raise ValueError(f'Replay input changed: {path}')
    from src.simulator.tabm import selections
    actual=pd.read_parquet(root/'selections.parquet')
    expected=selections(pd.read_parquet(models/'predictions.parquet'))
    pd.testing.assert_frame_equal(actual,expected)
    return design


def build_report(models,replay,baseline=PROJECT_ROOT/'results/opportunity_replay'):
    models,replay,baseline=map(Path,(models,replay,baseline))
    design=verify(models)
    old_design=verify_old_replay(baseline,PROJECT_ROOT/'results/opportunity_value')
    replay_design=verify_replay(replay,models,baseline)
    if replay_design['execution']!=old_design['execution']:
        raise ValueError('Different execution assumptions')
    folder=models/'analysis'
    folder.mkdir(exist_ok=True)
    prediction=pd.read_parquet(models/'predictions.parquet')
    metrics=pd.read_csv(models/'metrics.csv')
    audit_fits(models,design,prediction).to_csv(folder/'prediction_verification.csv',index=False)
    display_models=list(COMPARATORS)+list(REPLAY_POLICIES)
    headline=metrics.loc[metrics.fold.eq('all')&metrics.model.isin(display_models)]
    headline.to_csv(folder/'comparison.csv',index=False)
    # Repeated seeds are all retained; the primary tuned score is their mean.
    seeds=metrics.loc[metrics.model.str.contains('_seed',regex=False)].copy()
    seeds['method']=seeds.model.str.replace(r'_seed\d+$','',regex=True)
    seeds.groupby(['scope','fold','top_n','method'])[['recall','peak_value_recall','average_precision']].agg(
        ['mean','std','min','max']).to_csv(folder/'seed_spread.csv')
    chosen_predictions=prediction.loc[prediction.model.isin(display_models)]
    groups=group_capture(chosen_predictions)
    groups.to_csv(folder/'group_capture.csv',index=False)
    paired_family_intervals(groups).to_csv(folder/'paired_occurrence_intervals.csv',index=False)
    contributions=[]
    for model,part in chosen_predictions.groupby('model'):
        for n in (5,3):
            chosen=ranked(part,part.score,n)
            for kind in ('family','stratum','event_slug'):
                captured=chosen.groupby(kind).value_target.sum()
                for group,subset in part.groupby(kind):
                    contributions.append(dict(model=model,top_n=n,group_type=kind,group=group,
                        total=float(subset.value_target.sum()),captured=float(captured.get(group,0))))
    contributions=pd.DataFrame(contributions)
    contributions.to_csv(folder/'value_contributions.csv',index=False)
    intervals=[]
    for n,part in contributions.loc[contributions.group_type.eq('family')].groupby('top_n'):
        base=part.loc[part.model.eq('quoted_gap_rule')].set_index('group').sort_index()
        draws=np.random.default_rng(2026).integers(len(base),size=(10000,len(base)))
        totals=base.total.to_numpy()[draws].sum(axis=1)
        for model,subset in part.groupby('model'):
            delta=subset.set_index('group').reindex(base.index).captured.to_numpy()-base.captured.to_numpy()
            samples=delta[draws].sum(axis=1)[totals>0]/totals[totals>0]
            intervals.append(dict(model=model,top_n=n,families=len(base),difference=delta.sum()/base.total.sum(),
                lower95=np.quantile(samples,.025),upper95=np.quantile(samples,.975)))
    pd.DataFrame(intervals).to_csv(folder/'paired_value_intervals.csv',index=False)
    errors=[]
    for model,part in prediction.loc[prediction.model.str.startswith('tabm_')&prediction.label_known].groupby('model'):
        if model=='tabm_forest_rank_ensemble':
            continue
        for fold,subset in [('all',part)]+list(part.groupby('fold')):
            row=dict(model=model,fold=fold,rows=len(subset))
            if 'classifier' in model:
                row['brier']=brier_score_loss(subset.target,subset.score)
            else:
                row['mae_usd']=mean_absolute_error(subset.value_target,subset.score)
                row['rmse_usd']=np.sqrt(mean_squared_error(subset.value_target,subset.score))
            errors.append(row)
    pd.DataFrame(errors).to_csv(folder/'prediction_errors.csv',index=False)
    # Join saved economics; the classical strategies are not rerun or modified.
    summary=pd.concat([pd.read_csv(baseline/'summary.csv'),pd.read_csv(replay/'summary.csv')],ignore_index=True)
    random=summary.loc[summary.model.str.startswith('random_')]
    random_mean=random.groupby(['scope','fold','top_n','arrival_delay_s'],as_index=False).known_pnl_usd.mean().assign(model='random_mean')
    economic=pd.concat([summary.loc[~summary.model.str.startswith('random_')],random_mean],ignore_index=True)
    economic.to_csv(folder/'economic_comparison.csv',index=False)
    attempts=pd.concat([pd.read_parquet(baseline/'attempts.parquet'),pd.read_parquet(replay/'attempts.parquet')],ignore_index=True)
    taken=accounted(attempts)
    totals=taken.groupby(['scope','top_n','arrival_delay_s','model','event_slug'],as_index=False).agg(
        known_pnl_usd=('pnl_usd','sum'),attempts=('pnl_usd','size'))
    totals.to_csv(folder/'event_pnl.csv',index=False)
    curves=taken.loc[taken.pnl_usd.notna()].groupby(['scope','top_n','arrival_delay_s','model','accounting_time'],as_index=False).pnl_usd.sum().sort_values('accounting_time')
    curves['cumulative_pnl_usd']=curves.groupby(['scope','top_n','arrival_delay_s','model']).pnl_usd.cumsum()
    ending=curves.groupby(['scope','top_n','arrival_delay_s','model']).cumulative_pnl_usd.last()
    expected=summary.loc[summary.fold.eq('all')].set_index(['scope','top_n','arrival_delay_s','model']).known_pnl_usd
    if not np.allclose(ending.reindex(expected.index,fill_value=0),expected):
        raise ValueError('P&L curves do not reconcile')
    curves.to_parquet(folder/'pnl_curves.parquet',index=False)
    econ_ci=[]
    universe=prediction.loc[prediction.model.eq('quoted_gap_rule')].drop_duplicates('event_slug').set_index('event_slug').family
    for scope,excluded in [('all_events',None),('exclude_december_fed','fed-decision-in-december')]:
        mapping=universe if excluded is None else universe.drop(excluded,errors='ignore')
        families=sorted(mapping.unique())
        draws=np.random.default_rng(2026).integers(len(families),size=(10000,len(families)))
        for n in (5,3):
            for delay in replay_design['arrival_delays_s']:
                part=totals.loc[totals.scope.eq(scope)&totals.top_n.eq(n)&totals.arrival_delay_s.eq(delay)].copy()
                part['family']=part.event_slug.map(mapping)
                table=part.groupby(['model','family']).known_pnl_usd.sum().unstack('family',fill_value=0).reindex(columns=families,fill_value=0)
                for model in REPLAY_POLICIES:
                    values=table.loc[model].to_numpy() if model in table.index else np.zeros(len(families))
                    diff=values-table.loc['quoted_gap_rule'].to_numpy()
                    samples=diff[draws].sum(axis=1)
                    econ_ci.append(dict(scope=scope,top_n=n,arrival_delay_s=delay,model=model,families=len(families),
                        difference_usd=diff.sum(),lower95=np.quantile(samples,.025),upper95=np.quantile(samples,.975)))
    pd.DataFrame(econ_ci).to_csv(folder/'paired_pnl_intervals.csv',index=False)
    economic.merge(metrics,on=['scope','fold','top_n','model'],how='left',validate='many_to_one',suffixes=('','_prediction')).to_csv(folder/'opportunity_vs_economics.csv',index=False)
    # Compact annotated panels rather than many overlapping curves.
    plot_models=['quoted_gap_rule','fixed_book_logistic','fixed_book_random_forest']+list(REPLAY_POLICIES)
    for scope in ('all_events','exclude_december_fed'):
        fig,axes=plt.subplots(1,2,figsize=(15,7),layout='constrained')
        for ax,n in zip(axes,(5,3)):
            part=headline.loc[headline.scope.eq(scope)&headline.top_n.eq(n)].set_index('model').reindex(plot_models)
            y=np.arange(len(part))
            ax.barh(y-.17,100*part.recall,height=.32,label='Positive-hour capture')
            ax.barh(y+.17,100*part.peak_value_recall,height=.32,label='Peak-value capture')
            ax.set_yticks(y,[LABELS[m] for m in part.index]);ax.invert_yaxis()
            ax.set_xlim(0,105);ax.set_xlabel('Capture (%)');ax.set_title(f'Top {n}')
            ax.grid(axis='x',alpha=.2);ax.legend(loc='lower left')
        fig.suptitle('Development opportunity prediction' if scope=='all_events' else 'December Fed excluded and reranked')
        fig.savefig(folder/f'{scope}_capture.png',dpi=140);plt.close(fig)
        fig,axes=plt.subplots(1,2,figsize=(15,7),layout='constrained')
        part=economic.loc[economic.scope.eq(scope)&economic.fold.eq('all')]
        low,high=part.known_pnl_usd.min(),part.known_pnl_usd.max()
        for ax,n in zip(axes,(5,3)):
            table=part.loc[part.top_n.eq(n)].pivot(index='model',columns='arrival_delay_s',values='known_pnl_usd').reindex(plot_models).sort_index(axis=1)
            ax.imshow(table,aspect='auto',cmap='YlGnBu',vmin=low,vmax=high)
            ax.set_xticks(range(len(table.columns)),[int(v*1000) for v in table.columns])
            ax.set_yticks(range(len(table)),[LABELS[m] for m in table.index])
            for i in range(len(table)):
                for j in range(len(table.columns)):
                    value=table.iloc[i,j]
                    ax.text(j,i,f'${value:,.0f}',ha='center',va='center',fontsize=8,
                            color='white' if (value-low)/max(high-low,1)>.6 else 'black')
            ax.set_xlabel('Arrival latency (ms)');ax.set_title(f'Top {n}: simulated P&L')
        fig.suptitle('All events' if scope=='all_events' else 'December Fed excluded and reranked')
        fig.savefig(folder/f'{scope}_latency.png',dpi=140);plt.close(fig)
    for n in (5,3):
        part=curves.loc[curves.scope.eq('all_events')&curves.top_n.eq(n)&curves.arrival_delay_s.eq(.1)]
        base=part.loc[part.model.eq('quoted_gap_rule')]
        fig,axes=plt.subplots(2,2,figsize=(13,8),sharex=True,sharey=True,layout='constrained')
        for ax,model in zip(axes.flat,('tabm_tuned_classifier','tabm_tuned_regression','tabm_tuned_two_stage','tabm_forest_rank_ensemble')):
            values=part.loc[part.model.eq(model)]
            beginning=pd.Timestamp(design['folds'][0][1],tz='UTC')
            ax.step([beginning]+base.accounting_time.tolist(),[0.]+base.cumulative_pnl_usd.tolist(),where='post',color='gray',label='Quoted-gap')
            ax.step([beginning]+values.accounting_time.tolist(),[0.]+values.cumulative_pnl_usd.tolist(),where='post',label='TabM policy')
            ax.set_title(LABELS[model]);ax.set_ylabel('Simulated P&L (USD)');ax.grid(alpha=.2);ax.legend()
            ax.tick_params(axis='x',rotation=25)
        fig.suptitle(f'Top {n}, 100ms arrival; margins accounted at completion')
        fig.savefig(folder/f'top{n}_pnl_panels.png',dpi=140);plt.close(fig)
    files={p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name!='manifest.json'}
    write_json(folder/'manifest.json',dict(training_completion_sha256=sha256(models/'complete.json'),
        replay_completion_sha256=sha256(replay/'complete.json'),baseline_completion_sha256=sha256(baseline/'complete.json'),
        source_sha256=sha256(Path(__file__)),files_sha256=files,final_lockbox_opened=False))
    return headline,economic


def verify_analysis(models,replay,baseline=PROJECT_ROOT/'results/opportunity_replay'):
    models,replay,baseline=map(Path,(models,replay,baseline))
    manifest=json.loads((models/'analysis/manifest.json').read_text())
    for path,key in [(models/'complete.json','training_completion_sha256'),(replay/'complete.json','replay_completion_sha256'),
                     (baseline/'complete.json','baseline_completion_sha256'),(Path(__file__),'source_sha256')]:
        if sha256(path)!=manifest[key]:
            raise ValueError(f'Analysis provenance differs: {path}')
    if manifest['final_lockbox_opened']:
        raise ValueError('Unexpected test access')
    for p,digest in manifest['files_sha256'].items():
        if sha256(models/'analysis'/p)!=digest:
            raise ValueError(f'Analysis changed: {p}')
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--models',type=Path,default=PROJECT_ROOT/'results/tabm')
    parser.add_argument('--replay',type=Path,default=PROJECT_ROOT/'results/tabm_replay')
    args=parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        build_report(args.models,args.replay)
