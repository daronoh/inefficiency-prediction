"""N=3 retuning diagnostics and final-fit audits, without final-test evaluation."""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.config import ScreeningSpec
from src.data.metadata import add_metadata
from src.data.opportunities import load_dataset
from src.models.evaluation import ranked, safe_ap
from src.models.final_preparation import verify, score_bundle, NEW_POLICIES, REFERENCES
from src.models.opportunities import choose_candidate
from src.models.opportunity_value import choose_value_candidate
from src.models.tabm_study import write_json
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.reporting.opportunities import group_capture, paired_family_intervals
from src.reporting.opportunity_replay import verify as verify_old_replay
from src.reporting.tabm import verify_replay as verify_tabm_replay

LABELS={'quoted_gap_rule':'Quoted-gap rule','fixed_book_logistic':'Fixed book logistic',
        'fixed_book_random_forest':'Fixed book forest','tabm_tuned_two_stage':'TabM tuned at N=5',
        'n3_book_logistic':'Logistic tuned at N=3','n3_book_random_forest':'Forest tuned at N=3',
        'n3_tabm_two_stage':'TabM tuned at N=3'}


def verify_replay(root,models):
    root,models=map(Path,(root,models))
    complete=json.loads((root/'complete.json').read_text())
    design=json.loads((root/'design.json').read_text())
    for flag in ('models_retrained','labels_rebuilt','final_lockbox_opened'):
        if complete[flag] or design[flag]:
            raise ValueError('Unexpected fitting or test access during replay')
    for p,digest in complete['files_sha256'].items():
        if sha256(root/p)!=digest:
            raise ValueError(f'Replay artifact changed: {p}')
    for p,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/p)!=digest:
            raise ValueError(f'Replay source changed: {p}')
    if sha256(models/'predictions.parquet')!=design['predictions_sha256'] or sha256(models/'complete.json')!=design['model_completion_sha256']:
        raise ValueError('Replay prediction provenance differs')
    if (sha256(PROJECT_ROOT/'results/opportunity_replay/complete.json')!=design['baseline_completion_sha256'] or
        sha256(PROCESSED_DIR/'opportunities/complete.json')!=design['observation_completion_sha256']):
        raise ValueError('Replay baseline or observation inputs changed')
    from src.simulator.final_preparation import selections
    pd.testing.assert_frame_equal(pd.read_parquet(root/'selections.parquet'),
                                 selections(pd.read_parquet(models/'predictions.parquet')))
    return design


def build_report(root,replay):
    root,replay=map(Path,(root,replay))
    design=verify(root)
    execution=verify_replay(replay,root)
    baseline=PROJECT_ROOT/'results/opportunity_replay'
    neural=PROJECT_ROOT/'results/tabm_replay'
    reference_design=verify_old_replay(baseline,PROJECT_ROOT/'results/opportunity_value')
    neural_design=verify_tabm_replay(neural,PROJECT_ROOT/'results/tabm',baseline)
    if execution['execution']!=reference_design['execution'] or execution['execution']!=neural_design['execution']:
        raise ValueError('Economic assumptions differ')
    folder=root/'analysis';folder.mkdir(exist_ok=True)
    frame=add_metadata(load_dataset(PROCESSED_DIR/'opportunity_value'))
    prediction=pd.read_parquet(root/'predictions.parquet')
    metrics=pd.read_csv(root/'metrics.csv')
    metrics.loc[metrics.fold.eq('all')].to_csv(folder/'comparison.csv',index=False)
    # Reproduce entire outer prediction arrays from the saved bundle and features.
    audits=[]
    for fold,start,end in design['folds']:
        test=frame.loc[frame.time.ge(pd.Timestamp(start,tz='UTC'))&frame.time.lt(pd.Timestamp(end,tz='UTC'))]
        bundle=json.loads((root/f'{fold}_bundle.json').read_text())
        actual=score_bundle(root,bundle,test)
        for model,part in actual.groupby('model'):
            saved=prediction.loc[prediction.fold.eq(fold)&prediction.model.eq(model)].sort_values(['event_slug','time'])
            part=part.sort_values(['event_slug','time'])
            if not np.allclose(part.score,saved.score,rtol=1e-5,atol=1e-6):
                raise ValueError(f'Outer predictions do not reproduce: {model}, {fold}')
            audits.append(dict(fold=fold,model=model,rows=len(part),maximum_error=float(np.abs(part.score.to_numpy()-saved.score.to_numpy()).max())))
    pd.DataFrame(audits).to_csv(folder/'prediction_verification.csv',index=False)
    # Determine how changing the ranking budget changes inner choices, without
    # refitting or choosing models using outer results or execution returns.
    inner=pd.read_parquet(root/'inner_predictions.parquet')
    rows=[]
    for (origin,method,candidate,block),part in inner.groupby(['origin','method','candidate','inner']):
        for n in (3,5):
            chosen=ranked(part,part.score,n)
            rows.append(dict(origin=origin,method=method,candidate=candidate,inner=block,top_n=n,
                positive_windows=int(part.target.eq(1).sum()),captured_positive_windows=int(chosen.target.eq(1).sum()),
                average_precision=safe_ap(part.loc[part.label_known,'target'],part.loc[part.label_known,'score']),
                total_peak_value=float(part.value_target.sum()),captured_peak_value=float(chosen.value_target.sum())))
    rescored=pd.DataFrame(rows)
    choices=[]
    for (origin,method),part in rescored.groupby(['origin','method']):
        chooser=choose_value_candidate if method=='tabm_two_stage' else choose_candidate
        a=chooser(part.loc[part.top_n.eq(3)]);b=chooser(part.loc[part.top_n.eq(5)])
        choices.append(dict(origin=origin,method=method,n3_candidate=a,n5_candidate=b,changed=a!=b))
    rescored.to_csv(folder/'inner_budget_metrics.csv',index=False)
    pd.DataFrame(choices).to_csv(folder/'inner_budget_choices.csv',index=False)
    groups=group_capture(prediction,budgets=(3,5))
    groups.to_csv(folder/'group_capture.csv',index=False)
    paired_family_intervals(groups).to_csv(folder/'paired_occurrence_intervals.csv',index=False)
    value=[]
    for model,part in prediction.groupby('model'):
        for n in (3,5):
            chosen=ranked(part,part.score,n)
            captured=chosen.groupby('family').value_target.sum()
            for family,subset in part.groupby('family'):
                value.append(dict(model=model,top_n=n,family=family,total=float(subset.value_target.sum()),
                                  captured=float(captured.get(family,0.))))
    value=pd.DataFrame(value);value.to_csv(folder/'family_value_capture.csv',index=False)
    intervals=[]
    for n,part in value.groupby('top_n'):
        base=part.loc[part.model.eq('quoted_gap_rule')].set_index('family').sort_index()
        draws=np.random.default_rng(2026).integers(len(base),size=(10000,len(base)))
        totals=base.total.to_numpy()[draws].sum(axis=1)
        for model,subset in part.groupby('model'):
            diff=subset.set_index('family').reindex(base.index).captured.to_numpy()-base.captured.to_numpy()
            samples=diff[draws].sum(axis=1)[totals>0]/totals[totals>0]
            intervals.append(dict(model=model,top_n=n,difference=diff.sum()/base.total.sum(),
                lower95=np.quantile(samples,.025),upper95=np.quantile(samples,.975),families=len(base)))
    pd.DataFrame(intervals).to_csv(folder/'paired_value_intervals.csv',index=False)
    prior=pd.read_csv(baseline/'summary.csv')
    prior=prior.loc[prior.model.isin(REFERENCES)]
    prior_neural=pd.read_csv(neural/'summary.csv')
    prior_neural=prior_neural.loc[prior_neural.model.eq('tabm_tuned_two_stage')]
    economics=pd.concat([prior,prior_neural,pd.read_csv(replay/'summary.csv')],ignore_index=True)
    economics.to_csv(folder/'economic_comparison.csv',index=False)
    # Account by arrival for completed baskets and by unwind for incomplete ones.
    attempts=[]
    for directory,models in [(baseline,REFERENCES),(neural,('tabm_tuned_two_stage',)),(replay,NEW_POLICIES)]:
        trades=pd.read_parquet(directory/'attempts.parquet')
        attempts.append(trades.loc[trades.model.isin(models)&trades.attempted])
    attempts=pd.concat(attempts,ignore_index=True)
    totals=attempts.groupby(['scope','top_n','arrival_delay_s','model','event_slug'],as_index=False).pnl_usd.sum()
    totals.to_csv(folder/'event_pnl.csv',index=False)
    family_map=prediction.loc[prediction.model.eq('quoted_gap_rule')].drop_duplicates('event_slug').set_index('event_slug').family
    paired=[]
    for scope,excluded in [('all_events',None),('exclude_december_fed','fed-decision-in-december')]:
        families=sorted((family_map if excluded is None else family_map.drop(excluded)).unique())
        draws=np.random.default_rng(2026).integers(len(families),size=(10000,len(families)))
        for n in (3,5):
            for delay in execution['arrival_delays_s']:
                part=totals.loc[totals.scope.eq(scope)&totals.top_n.eq(n)&totals.arrival_delay_s.eq(delay)].copy()
                part['family']=part.event_slug.map(family_map)
                table=part.groupby(['model','family']).pnl_usd.sum().unstack('family',fill_value=0).reindex(columns=families,fill_value=0)
                for model in NEW_POLICIES:
                    values=table.loc[model].to_numpy() if model in table.index else np.zeros(len(families))
                    diff=values-table.loc['quoted_gap_rule'].to_numpy()
                    samples=diff[draws].sum(axis=1)
                    paired.append(dict(scope=scope,top_n=n,arrival_delay_s=delay,model=model,difference_usd=diff.sum(),
                        lower95=np.quantile(samples,.025),upper95=np.quantile(samples,.975),families=len(families)))
    pd.DataFrame(paired).to_csv(folder/'paired_pnl_intervals.csv',index=False)
    attempts['accounting_time']=attempts.arrival_time.where(attempts.status.eq('complete'),attempts.unwind_time)
    curves=attempts.loc[attempts.pnl_usd.notna()].groupby(['scope','top_n','arrival_delay_s','model','accounting_time'],as_index=False).pnl_usd.sum().sort_values('accounting_time')
    curves['cumulative_pnl_usd']=curves.groupby(['scope','top_n','arrival_delay_s','model']).pnl_usd.cumsum()
    ending=curves.groupby(['scope','top_n','arrival_delay_s','model']).cumulative_pnl_usd.last()
    expected=economics.loc[economics.fold.eq('all')].set_index(['scope','top_n','arrival_delay_s','model']).known_pnl_usd
    if not np.allclose(ending.reindex(expected.index,fill_value=0),expected):
        raise ValueError('Cumulative P&L fails reconciliation')
    curves.to_parquet(folder/'pnl_curves.parquet',index=False)
    for scope in ('all_events','exclude_december_fed'):
        fig,axes=plt.subplots(1,2,figsize=(14,5),layout='constrained')
        headline=economics.loc[economics.scope.eq(scope)&economics.fold.eq('all')]
        for ax,n in zip(axes,(3,5)):
            table=headline.loc[headline.top_n.eq(n)].pivot(index='model',columns='arrival_delay_s',values='known_pnl_usd').reindex(LABELS).sort_index(axis=1)
            lo,hi=table.min().min(),table.max().max()
            ax.imshow(table,aspect='auto',cmap='YlGnBu',vmin=lo,vmax=hi)
            ax.set_yticks(range(len(table)),[LABELS[m] for m in table.index])
            ax.set_xticks(range(len(table.columns)),[int(v*1000) for v in table.columns])
            for i in range(len(table)):
                for j in range(len(table.columns)):
                    v=table.iloc[i,j]
                    ax.text(j,i,f'${v:,.0f}',ha='center',va='center',fontsize=9,color='white' if (v-lo)/max(hi-lo,1)>.6 else 'black')
            ax.set_title(f'N={n}: {"primary" if n==3 else "secondary"}');ax.set_xlabel('Arrival latency (ms)')
        fig.suptitle('Development simulated P&L: '+('all events' if scope=='all_events' else 'December Fed excluded'))
        fig.savefig(folder/f'{scope}_latency.png',dpi=140);plt.close(fig)
    audit=pd.read_csv(root/'fit_audit.csv')
    audit.loc[audit.origin.eq('final_refit')].to_csv(folder/'final_fit_inventory.csv',index=False)
    files={p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name!='manifest.json'}
    write_json(folder/'manifest.json',dict(source_sha256=sha256(Path(__file__)),
        training_completion_sha256=sha256(root/'complete.json'),replay_completion_sha256=sha256(replay/'complete.json'),
        baseline_completion_sha256=sha256(baseline/'complete.json'),tabm_replay_completion_sha256=sha256(neural/'complete.json'),
        files_sha256=files,final_lockbox_opened=False))
    return metrics,economics


def verify_analysis(root,replay):
    root,replay=map(Path,(root,replay))
    manifest=json.loads((root/'analysis/manifest.json').read_text())
    for p,key in [(root/'complete.json','training_completion_sha256'),(replay/'complete.json','replay_completion_sha256'),
        (PROJECT_ROOT/'results/opportunity_replay/complete.json','baseline_completion_sha256'),
        (PROJECT_ROOT/'results/tabm_replay/complete.json','tabm_replay_completion_sha256'),(Path(__file__),'source_sha256')]:
        if sha256(p)!=manifest[key]:
            raise ValueError('Analysis provenance differs')
    if manifest['final_lockbox_opened']:
        raise ValueError('Final test was opened')
    for p,digest in manifest['files_sha256'].items():
        if sha256(root/'analysis'/p)!=digest:
            raise ValueError(f'Analysis artifact changed: {p}')
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--models',type=Path,default=PROJECT_ROOT/'results/final_preparation')
    parser.add_argument('--replay',type=Path,default=PROJECT_ROOT/'results/final_preparation_replay')
    args=parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        build_report(args.models,args.replay)
