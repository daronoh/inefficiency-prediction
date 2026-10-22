"""Economic sensitivity and concentration checks for fixed opportunity rankings."""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.reporting.workflow import profitability_run, execution_path
from src.simulator.opportunities import POLICIES

LABELS={'quoted_gap_rule':'Quoted-gap rule','fixed_book_logistic':'Book logistic',
    'fixed_book_random_forest':'Book forest','fixed_combined_random_forest':'Forest + metadata',
    'tuned_value_two_stage_random_forest':'Tuned two-stage forest','tuned_value_ensemble':'Tuned value ensemble',
    'monitor_all':'Monitor all','random_mean':'Random (20-seed mean)'}


def verify(root,models):
    root,models=Path(root),Path(models)
    complete=json.loads((root/'complete.json').read_text())
    design=json.loads((root/'design.json').read_text())
    if complete['final_lockbox_opened'] or complete['models_retrained'] or complete['labels_rebuilt']:
        raise ValueError('Expected fixed development rankings')
    for path,digest in complete['files_sha256'].items():
        if sha256(root/path)!=digest:
            raise ValueError(f'Replay artifact changed: {path}')
    for path,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/path)!=digest:
            raise ValueError(f'Replay source changed: {path}')
    if (sha256(models/'predictions.parquet')!=design['predictions_sha256'] or
        sha256(models/'complete.json')!=design['model_completion_sha256']):
        raise ValueError('Fixed model inputs changed')
    selections=pd.read_parquet(root/'selections.parquet')
    if selections.duplicated(['scope','top_n','model','event_slug','time']).any():
        raise ValueError('Duplicate selections')
    end=pd.Timestamp(design['window']['lockbox_start'])
    if not (selections.time+pd.Timedelta(minutes=design['window']['horizon_minutes'])).le(end).all():
        raise ValueError('Selection extends into final test')
    summary=pd.read_csv(root/'summary.csv')
    if not np.allclose(summary.unwind_delay_s,1.):
        raise ValueError('Unwind changed in latency-only comparison')
    # Monitoring all must be invariant to the otherwise-unused top-N panel.
    all_rows=summary.loc[summary.model.eq('monitor_all')]
    a=all_rows.loc[all_rows.top_n.eq(5)].sort_values(['scope','fold','arrival_delay_s']).reset_index(drop=True)
    b=all_rows.loc[all_rows.top_n.eq(3)].sort_values(['scope','fold','arrival_delay_s']).reset_index(drop=True)
    pd.testing.assert_frame_equal(a.drop(columns='top_n'),b.drop(columns='top_n'))
    return design


def accounted(attempts):
    taken=attempts.loc[attempts.attempted].copy()
    taken['accounting_time']=taken.arrival_time.where(taken.status.eq('complete'),taken.unwind_time)
    return taken


def verify_analysis(root):
    root=Path(root)
    manifest=json.loads((root/'analysis/manifest.json').read_text())
    if (manifest['final_lockbox_opened'] or manifest['replay_completion_sha256']!=sha256(root/'complete.json')
        or manifest['source_sha256']!=sha256(Path(__file__))):
        raise ValueError('Economic analysis provenance differs')
    for path,digest in manifest['files_sha256'].items():
        if sha256(root/'analysis'/path)!=digest:
            raise ValueError(f'Economic analysis artifact changed: {path}')
    return manifest


def build_report(root,models):
    root,models=Path(root),Path(models)
    design=verify(root,models)
    summary=pd.read_csv(root/'summary.csv')
    attempts=pd.read_parquet(root/'attempts.parquet')
    taken=accounted(attempts)
    folder=root/'analysis'
    folder.mkdir(exist_ok=True)
    keys=['scope','top_n','arrival_delay_s','fold']
    random=summary.loc[summary.model.str.startswith('random_')]
    numeric=list(summary.select_dtypes('number').columns.difference(['top_n','arrival_delay_s']))
    mean=random.groupby(keys,as_index=False)[numeric].mean().assign(model='random_mean')
    compare=pd.concat([summary.loc[~summary.model.str.startswith('random_')],mean],ignore_index=True)
    compare.to_csv(folder/'comparison.csv',index=False)
    random.groupby(keys).known_pnl_usd.agg(['mean','std','min','max',
        lambda x:x.quantile(.025),lambda x:x.quantile(.975)]).rename(columns={
            '<lambda_0>':'seed_p025','<lambda_1>':'seed_p975'}).reset_index().to_csv(folder/'random_seed_spread.csv',index=False)
    totals=taken.groupby(['scope','top_n','arrival_delay_s','model','event_slug'],as_index=False).agg(
        known_pnl_usd=('pnl_usd','sum'),attempts=('pnl_usd','size'),unknown_attempts=('pnl_usd',lambda x:int(x.isna().sum())))
    totals.to_csv(folder/'event_contributions.csv',index=False)
    curves=taken.loc[taken.pnl_usd.notna()].groupby(['scope','top_n','arrival_delay_s','model','accounting_time'],as_index=False).pnl_usd.sum()
    curves=curves.sort_values('accounting_time')
    curves['cumulative_pnl_usd']=curves.groupby(['scope','top_n','arrival_delay_s','model']).pnl_usd.cumsum()
    endings=curves.groupby(['scope','top_n','arrival_delay_s','model']).cumulative_pnl_usd.last()
    totals_check=summary.loc[summary.fold.eq('all')].set_index(['scope','top_n','arrival_delay_s','model']).known_pnl_usd
    if not np.allclose(endings.reindex(totals_check.index,fill_value=0.),totals_check):
        raise ValueError('Cumulative curves and summary P&L disagree')
    curves.to_parquet(folder/'pnl_curves.parquet',index=False)
    # Opportunity metrics and economic returns remain different columns.
    prediction_metrics=pd.read_csv(models/'metrics.csv')
    matched=compare.merge(prediction_metrics[['scope','top_n','fold','model','recall','peak_value_recall']],
        on=['scope','top_n','fold','model'],how='left',validate='many_to_one')
    matched.to_csv(folder/'opportunity_vs_economics.csv',index=False)
    # The rules and monitor-all do not depend on the learning objective. Their
    # 100ms returns should reproduce the earlier persistent execution benchmark.
    reference=profitability_run()
    agreement=[]
    for n in (5,3):
        source=execution_path(reference) if n==5 else reference/'budget_sensitivity/top3'
        if not (source/'summary.csv').exists():
            continue
        previous=pd.read_csv(source/'summary.csv')
        fields=['attempts','completed','partial_attempts','unknown_attempts','known_pnl_usd','max_drawdown_usd']
        for model in ('quoted_gap_rule','monitor_all'):
            old=previous.loc[previous.model.eq(model)].set_index(['scope','fold']).sort_index()
            new=summary.loc[summary.top_n.eq(n)&summary.arrival_delay_s.eq(.1)&summary.model.eq(model)].set_index(['scope','fold']).sort_index()
            if not old.index.equals(new.index) or not np.allclose(old[fields],new[fields],rtol=1e-10,atol=1e-8,equal_nan=True):
                raise ValueError(f'Existing 100ms reference differs: {model}, N={n}')
            agreement.append(dict(top_n=n,model=model,matched=True,reference_sha256=sha256(source/'summary.csv')))
    pd.DataFrame(agreement).to_csv(folder/'baseline_equivalence.csv',index=False)
    selection=pd.read_parquet(root/'selections.parquet')
    population=selection.loc[selection.model.eq('monitor_all')&selection.top_n.eq(5)]
    paired=[]
    for scope,group in population.groupby('scope'):
        event_family=group.drop_duplicates('event_slug').set_index('event_slug').family
        families=sorted(event_family.unique())
        draws=np.random.default_rng(2026).integers(len(families),size=(10000,len(families)))
        for n in (5,3):
            for delay in design['arrival_delays_s']:
                part=totals.loc[totals.scope.eq(scope)&totals.top_n.eq(n)&totals.arrival_delay_s.eq(delay)].copy()
                part['family']=part.event_slug.map(event_family)
                grouped=part.groupby(['model','family']).known_pnl_usd.sum().unstack('family',fill_value=0.).reindex(columns=families,fill_value=0.)
                base=grouped.loc['quoted_gap_rule'].to_numpy()
                for model in POLICIES+('monitor_all',):
                    values=grouped.loc[model].to_numpy() if model in grouped.index else np.zeros(len(families))
                    diff=values-base
                    samples=diff[draws].sum(axis=1)
                    paired.append(dict(scope=scope,top_n=n,arrival_delay_s=delay,model=model,families=len(families),
                        difference_usd=diff.sum(),lower95=np.quantile(samples,.025),upper95=np.quantile(samples,.975)))
    pd.DataFrame(paired).to_csv(folder/'paired_family_intervals.csv',index=False)
    # Heatmaps make near-identical latency curves readable without overlaid lines.
    policies=list(POLICIES)+['monitor_all','random_mean']
    for scope in ('all_events','exclude_december_fed'):
        fig,axes=plt.subplots(1,2,figsize=(14,6),layout='constrained')
        full=compare.loc[compare.scope.eq(scope)&compare.fold.eq('all')]
        lo,hi=full.known_pnl_usd.min(),full.known_pnl_usd.max()
        for ax,n in zip(axes,(5,3)):
            table=full.loc[full.top_n.eq(n)].pivot(index='model',columns='arrival_delay_s',values='known_pnl_usd').reindex(policies).sort_index(axis=1)
            ax.imshow(table.to_numpy(),aspect='auto',cmap='YlGnBu',vmin=lo,vmax=hi)
            ax.set_xticks(range(len(table.columns)),[str(int(v*1000)) for v in table.columns])
            ax.set_yticks(range(len(table)),[LABELS[k] for k in table.index])
            for i in range(len(table)):
                for j in range(len(table.columns)):
                    value=table.iloc[i,j]
                    ax.text(j,i,f'${value:,.0f}',ha='center',va='center',fontsize=9,
                        color='white' if (value-lo)/max(hi-lo,1)>.6 else 'black')
            ax.set_title(f'Top {n}: known simulated P&L')
            ax.set_xlabel('Assumed arrival latency (ms)')
        fig.suptitle('All events' if scope=='all_events' else 'December Fed excluded; selections reranked')
        fig.savefig(folder/f'{scope}_latency.png',dpi=150)
        plt.close(fig)
    # Separate panels for each policy, with the quoted-gap curve as reference.
    for n in (5,3):
        part=curves.loc[curves.scope.eq('all_events')&curves.top_n.eq(n)&curves.arrival_delay_s.eq(.1)]
        base=part.loc[part.model.eq('quoted_gap_rule')]
        fig,axes=plt.subplots(4,2,figsize=(14,11),sharex=True,sharey=True,layout='constrained')
        for ax,model in zip(axes.flat,list(POLICIES)+['monitor_all']):
            subset=part.loc[part.model.eq(model)]
            beginning=pd.Timestamp(design['folds'][0][1],tz='UTC')
            ax.step([beginning]+base.accounting_time.tolist(),[0.]+base.cumulative_pnl_usd.tolist(),where='post',color='#aaaaaa',label='Quoted-gap')
            ax.step([beginning]+subset.accounting_time.tolist(),[0.]+subset.cumulative_pnl_usd.tolist(),where='post',color='#286b9c',label=LABELS[model])
            ax.set_title(LABELS[model])
            ax.set_ylabel('Known P&L (USD)')
            ax.grid(alpha=.2)
            ax.tick_params(axis='x',rotation=30)
        axes.flat[-1].set_visible(False)
        fig.suptitle(f'100ms arrival, top {n}; grey = quoted-gap reference; margins booked at completion')
        fig.savefig(folder/f'top{n}_pnl_panels.png',dpi=150)
        plt.close(fig)
    files={p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name!='manifest.json'}
    (folder/'manifest.json').write_text(json.dumps(dict(replay_completion_sha256=sha256(root/'complete.json'),
        source_sha256=sha256(Path(__file__)),files_sha256=files,final_lockbox_opened=False),indent=2)+'\n')
    verify_analysis(root)
    return compare


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,required=True)
    parser.add_argument('--models',type=Path,default=PROJECT_ROOT/'results/opportunity_value')
    args=parser.parse_args()
    build_report(args.run,args.models)
