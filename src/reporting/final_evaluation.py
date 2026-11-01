"""Audit final outputs, compare frozen policies and create readable figures."""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.data.final_evaluation import verify as verify_data
from src.models.final_preparation import score_bundle
from src.paths import PROJECT_ROOT
from src.reporting.final_protocol import verify as verify_protocol
from src.simulator.final_evaluation import verify_files


LABELS = {'quoted_gap_rule':'Quoted-gap','fixed_book_logistic':'Fixed logistic',
    'fixed_book_random_forest':'Fixed forest','n3_book_logistic':'Retuned logistic',
    'n3_book_random_forest':'Retuned forest (same final fit)','n3_tabm_two_stage':'Two-stage TabM',
    'random_mean':'Random (20-seed mean)','monitor_all':'Monitor all'}
PRIMARY = 'frozen_supported_universe'


def paired_intervals(groups, protocol):
    rows = []
    for (scope,n),part in groups.loc[groups.group_type.eq('family')].groupby(['scope','top_n']):
        base = part.loc[part.model.eq('quoted_gap_rule')].set_index('group').sort_index()
        count = len(base)
        draws = np.random.default_rng(2026).integers(count,size=(10000,count)) if count >= 2 else None
        for model in protocol['policies']:
            candidate = part.loc[part.model.eq(model)].set_index('group').reindex(base.index)
            for name,denominator,numerator in [('recall','positive_windows','captured_positive_windows'),
                                              ('value','total_peak_value','captured_peak_value')]:
                total = base[denominator].sum()
                diff = candidate[numerator].to_numpy()-base[numerator].to_numpy()
                low,high = np.nan,np.nan
                if draws is not None and total > 0:
                    totals = base[denominator].to_numpy()[draws].sum(axis=1)
                    samples = diff[draws].sum(axis=1)[totals>0]/totals[totals>0]
                    if len(samples):
                        low,high = np.quantile(samples,[.025,.975])
                rows.append(dict(scope=scope,top_n=n,model=model,metric=name,families=count,
                    difference=diff.sum()/total if total else np.nan,lower95=low,upper95=high))
    return pd.DataFrame(rows)


def build_report(data, models, replay):
    data,models,replay = map(Path,(data,models,replay))
    verify_data(data); verify_files(models); verify_files(replay)
    protocol = verify_protocol(PROJECT_ROOT/'results/final_protocol',PROJECT_ROOT/'results/final_coverage')
    for folder,key,parent in [(models,'dataset_completion_sha256',data),
                              (replay,'dataset_completion_sha256',data),(replay,'models_completion_sha256',models)]:
        design = json.loads((folder/'design.json').read_text())
        if design[key] != sha256(parent/'complete.json'):
            raise ValueError('Final stage provenance differs')
    folder = replay/'analysis'
    folder.mkdir(exist_ok=True)
    features = pd.read_parquet(data/'features.parquet')
    predictions = pd.read_parquet(models/'predictions.parquet')
    bundle = json.loads((PROJECT_ROOT/'results/final_preparation/final_refit_bundle.json').read_text())
    reproduced = score_bundle(PROJECT_ROOT/'results/final_preparation',bundle,features)
    errors = []
    for name,part in reproduced.groupby('model'):
        p = part.sort_values(['event_slug','time'])
        saved = predictions.loc[predictions.model.eq(name)].sort_values(['event_slug','time'])
        np.testing.assert_allclose(p.score,saved.score,rtol=1e-6,atol=1e-6)
        errors.append(dict(model=name,rows=len(p),maximum_error=float(np.max(np.abs(p.score.to_numpy()-saved.score.to_numpy())))))
    pd.DataFrame(errors).to_csv(folder/'prediction_verification.csv',index=False)
    metrics = pd.read_csv(models/'metrics.csv')
    economics = pd.read_csv(replay/'summary.csv')
    main = metrics.loc[metrics.scope.eq(PRIMARY)&metrics.subset.eq('full')&metrics.top_n.eq(3)]
    main.to_csv(folder/'primary_prediction_comparison.csv',index=False)
    groups = pd.read_csv(models/'group_capture.csv')
    paired_intervals(groups,protocol).to_csv(folder/'paired_capture_intervals.csv',index=False)
    summary = json.loads((data/'summary.json').read_text())
    frame = pd.read_parquet(data/'examples.parquet')
    event_counts = frame.groupby('event_slug').agg(rows=('time','size'),known=('label_known','sum'),
        positives=('target',lambda x:int(x.eq(1).sum())),peak_quote_value=('value_target','sum'),
        witnessed_in_unknown=('witnessed_opportunity','sum'))
    event_counts['unknown'] = event_counts.rows-event_counts.known
    event_counts['witnessed_in_unknown'] = frame.loc[~frame.label_known].groupby('event_slug').witnessed_opportunity.sum().reindex(event_counts.index,fill_value=0)
    event_counts.to_csv(folder/'event_label_counts.csv')
    attempts = pd.read_parquet(replay/'attempts.parquet')
    taken = attempts.loc[attempts.attempted].copy() if len(attempts) else attempts.copy()
    if len(taken):
        taken['accounting_time'] = taken.arrival_time.where(taken.status.eq('complete'),taken.unwind_time)
        curves = taken.loc[taken.pnl_usd.notna()].groupby(['scope','top_n','arrival_delay_s','model','accounting_time'],as_index=False).pnl_usd.sum().sort_values('accounting_time')
        curves['cumulative_pnl_usd'] = curves.groupby(['scope','top_n','arrival_delay_s','model']).pnl_usd.cumsum()
        ending = curves.groupby(['scope','top_n','arrival_delay_s','model']).cumulative_pnl_usd.last()
        expected = economics.set_index(['scope','top_n','arrival_delay_s','model']).known_pnl_usd
        if not np.allclose(ending.reindex(expected.index,fill_value=0),expected):
            raise ValueError('Final PnL curves do not reconcile')
    else:
        curves = pd.DataFrame(columns=['scope','top_n','arrival_delay_s','model','accounting_time','pnl_usd','cumulative_pnl_usd'])
    curves.to_parquet(folder/'pnl_curves.parquet',index=False)
    event_pnl = taken.groupby(['scope','top_n','arrival_delay_s','model','event_slug'],as_index=False).pnl_usd.sum() if len(taken) else pd.DataFrame(columns=['scope','top_n','arrival_delay_s','model','event_slug','pnl_usd'])
    event_pnl.to_csv(folder/'event_pnl.csv',index=False)
    family_map = predictions.drop_duplicates('event_slug').set_index('event_slug').family
    intervals = []
    for scope in (PRIMARY,'development_events_only'):
        events = sorted(frame.event_slug.unique() if scope == PRIMARY else frame.loc[frame.development_event,'event_slug'].unique())
        families = sorted(set(family_map.reindex(events)))
        count = len(families)
        draws = np.random.default_rng(2026).integers(count,size=(10000,count)) if count >= 2 else None
        for n in (3,5):
            part = event_pnl.loc[event_pnl.scope.eq(scope)&event_pnl.top_n.eq(n)&event_pnl.arrival_delay_s.eq(.1)].copy()
            part['family'] = part.event_slug.map(family_map)
            table = part.groupby(['model','family']).pnl_usd.sum().unstack('family',fill_value=0).reindex(index=protocol['policies'],columns=families,fill_value=0).fillna(0)
            for model in protocol['policies']:
                diff = table.loc[model].to_numpy()-table.loc['quoted_gap_rule'].to_numpy()
                low,high = np.quantile(diff[draws].sum(axis=1),[.025,.975]) if draws is not None else (np.nan,np.nan)
                intervals.append(dict(scope=scope,top_n=n,model=model,families=count,difference_usd=float(diff.sum()),lower95=low,upper95=high))
    pd.DataFrame(intervals).to_csv(folder/'paired_pnl_intervals.csv',index=False)
    random = economics.loc[economics.model.str.startswith('random_')].groupby(['scope','top_n','arrival_delay_s']).known_pnl_usd.agg(['mean','min','max','std']).reset_index()
    random.to_csv(folder/'random_replay_distribution.csv',index=False)
    random_mean = random.rename(columns={'mean':'known_pnl_usd'}).assign(model='random_mean')
    plotted = pd.concat([economics.loc[~economics.model.str.startswith('random_')],random_mean],ignore_index=True)
    for scope in (PRIMARY,'development_events_only'):
        fig,axes = plt.subplots(1,2,figsize=(14,5),layout='constrained')
        for ax,n in zip(axes,(3,5)):
            table = plotted.loc[plotted.scope.eq(scope)&plotted.top_n.eq(n)].pivot(index='model',columns='arrival_delay_s',values='known_pnl_usd').reindex(LABELS).sort_index(axis=1)
            lo,hi = table.min().min(),table.max().max()
            ax.imshow(table,aspect='auto',cmap='YlGnBu',vmin=lo,vmax=hi)
            ax.set_yticks(range(len(table)),[LABELS[m] for m in table.index])
            ax.set_xticks(range(len(table.columns)),[int(x*1000) for x in table.columns])
            for i in range(len(table)):
                for j in range(len(table.columns)):
                    value = table.iloc[i,j]
                    ax.text(j,i,f'${value:,.2f}',ha='center',va='center',fontsize=8,color='white' if (value-lo)/max(hi-lo,1)>.6 else 'black')
            ax.set_xlabel('Arrival latency (ms)');ax.set_title(f'N={n}')
        fig.suptitle('Final simulated known P&L: '+('original supported universe' if scope == PRIMARY else 'development events only'))
        fig.savefig(folder/f'{scope}_latency.png',dpi=140);plt.close(fig)
    # Separate axes keep clustered strategy paths readable; gray is quoted-gap.
    fig,axes = plt.subplots(4,2,figsize=(13,11),layout='constrained')
    names = [m for m in LABELS if m != 'random_mean']
    part = curves.loc[curves.scope.eq(PRIMARY)&curves.top_n.eq(3)&curves.arrival_delay_s.eq(.1)]
    zero = pd.Timestamp(protocol['period']['start'])
    terminal = pd.Timestamp(protocol['period']['end_exclusive'])
    for ax,name in zip(axes.flat,names):
        for model,color,label in [('quoted_gap_rule','gray','Quoted-gap'),(name,'tab:blue',LABELS[name])]:
            p = part.loc[part.model.eq(model)]
            final = float(p.cumulative_pnl_usd.iloc[-1]) if len(p) else 0.
            times = [zero]+p.accounting_time.tolist()+[terminal]
            values = [0.]+p.cumulative_pnl_usd.tolist()+[final]
            ax.step(times,values,where='post',label=label,color=color,linewidth=3 if color == 'gray' else 1.3)
        ax.set_xlim(zero,terminal)
        endpoint = economics.loc[economics.scope.eq(PRIMARY)&economics.top_n.eq(3)&economics.arrival_delay_s.eq(.1)&economics.model.eq(name),'known_pnl_usd'].iloc[0]
        ax.text(.03,.92,f'Final: ${endpoint:,.2f}',transform=ax.transAxes,fontsize=9)
        ax.set_title(LABELS[name]);ax.set_ylabel('Known cumulative P&L ($)');ax.tick_params(axis='x',rotation=20)
    axes.flat[-1].set_visible(False)
    fig.suptitle('Final N=3, 100 ms: each policy compared with quoted-gap')
    fig.savefig(folder/'primary_pnl_small_multiples.png',dpi=130);plt.close(fig)
    metrics.loc[metrics.scope.str.startswith('leave_out:')].to_csv(folder/'leave_event_out_capture.csv',index=False)
    economics.loc[economics.scope.str.startswith('leave_out:')].to_csv(folder/'leave_event_out_economics.csv',index=False)
    files = {p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name != 'manifest.json'}
    manifest = dict(files_sha256=files,source_sha256=sha256(Path(__file__)),
        data_completion_sha256=sha256(data/'complete.json'),models_completion_sha256=sha256(models/'complete.json'),
        replay_completion_sha256=sha256(replay/'complete.json'),protocol_sha256=sha256(PROJECT_ROOT/'results/final_protocol/protocol.json'),
        models_retrained=False,final_outcomes_opened=True,summary=summary)
    (folder/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return main,economics


def verify_analysis(data,models,replay):
    data,models,replay = map(Path,(data,models,replay))
    verify_data(data);verify_files(models);verify_files(replay)
    manifest = json.loads((replay/'analysis/manifest.json').read_text())
    for path,key in [(data/'complete.json','data_completion_sha256'),(models/'complete.json','models_completion_sha256'),
                     (replay/'complete.json','replay_completion_sha256'),(Path(__file__),'source_sha256')]:
        if sha256(path) != manifest[key]:
            raise ValueError('Final report provenance changed')
    for name,digest in manifest['files_sha256'].items():
        if sha256(replay/'analysis'/name) != digest:
            raise ValueError('Final report artifact changed')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data',type=Path,required=True)
    parser.add_argument('--models',type=Path,required=True)
    parser.add_argument('--replay',type=Path,required=True)
    args = parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        build_report(args.data,args.models,args.replay)
