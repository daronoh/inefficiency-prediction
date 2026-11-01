"""Integrity checks, comparative metrics and figures for weekly Dome research."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.data.walk_forward import ROOT as DATA, FIRST, BOUNDARY, END, HORIZON, load, write_json
from src.models.evaluation import ranked
from src.models.opportunity_value import value_metrics
from src.models.validation_horizon import paired_block_intervals
from src.models.walk_forward import OUTPUT as MODELS, folds, train_before, score_saved, choose
from src.simulator.walk_forward import OUTPUT as REPLAY, verify_models
from src.paths import PROJECT_ROOT

LABELS={'quoted_gap_rule':'Quoted-gap','depth_rule':'Depth','freshness_rule':'Freshness',
    'fixed_book_logistic':'Logistic · book','fixed_book_random_forest':'Forest · book','fixed_book_xgboost':'XGBoost · book',
    'fixed_combined_logistic':'Logistic + metadata','fixed_combined_random_forest':'Forest + metadata',
    'fixed_combined_xgboost':'XGBoost + metadata','tuned_combined_logistic':'Tuned logistic',
    'tuned_combined_random_forest':'Tuned forest','tuned_combined_xgboost':'Tuned XGBoost',
    'forest_two_stage':'Forest two-stage','tabm_two_stage':'TabM two-stage',
    'classifier_rank_ensemble':'Classifier ensemble','rank_ensemble':'Logistic / forest / TabM ensemble',
    'value_rank_ensemble':'Value ensemble','monitor_all':'Monitor all','random_mean':'Random mean'}


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
        if row.fold=='earlier_segment':part=part.loc[part.time.lt(BOUNDARY)]
        elif row.fold=='later_segment':part=part.loc[part.time.ge(BOUNDARY)]
        elif row.fold!='all':part=part.loc[part.fold.eq(row.fold)]
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


def table(frame,columns):
    lines=['| '+' | '.join(label for _,label in columns)+' |','| '+' | '.join('---' for _ in columns)+' |']
    for row in frame.to_dict('records'):
        values=[]
        for key,_ in columns:
            value=row[key]
            if key=='model':value=LABELS.get(value,value)
            elif isinstance(value,(float,np.floating)):value=f'{value:.2f}' if pd.notna(value) else '—'
            values.append(str(value))
        lines.append('| '+' | '.join(values)+' |')
    return '\n'.join(lines)


def analyze(models=MODELS,replay=REPLAY,data=DATA):
    models,replay,data=map(Path,(models,replay,data))
    verified=verify(models,data,reproduce_scores=True)
    analysis=models/'analysis';analysis.mkdir(exist_ok=True)
    prediction=pd.read_parquet(models/'predictions.parquet')
    metrics=pd.read_csv(models/'metrics.csv')
    primary=metrics.loc[metrics.scope.eq('all_events')&metrics.fold.eq('all')&metrics.top_n.eq(3)].copy()
    primary=primary.sort_values(['captured_peak_value','captured_positive_windows','average_precision'],ascending=False,kind='stable')
    primary.to_csv(analysis/'primary_prediction.csv',index=False)
    populations=pd.read_csv(models/'populations.csv')
    initial_training=int(populations.iloc[0].training_rows)
    latest_training=int(populations.iloc[-1].training_rows)
    latest_positive=int(populations.iloc[-1].training_positive)
    best=str(primary.iloc[0].model)
    pool=prediction.loc[prediction.model.eq('quoted_gap_rule')]
    counts=pool.groupby('time').size()
    population=dict(scheduled_hours=int((END-FIRST)/HORIZON),event_hours=len(pool),events=pool.event_slug.nunique(),
        known=int(pool.label_known.sum()),positive=int(pool.target.eq(1).sum()),unknown=int((~pool.label_known).sum()),
        positive_events=pool.loc[pool.target.eq(1),'event_slug'].nunique(),competitive_hours_n3=int(counts.gt(3).sum()),
        median_candidates=float(counts.median()),maximum_candidates=int(counts.max()),
        total_peak_value=float(pool.value_target.sum()))
    write_json(analysis/'population.json',population)
    contributions,paired=[],[]
    baseline=ranked(pool,pool.score,3)
    for name,part in prediction.groupby('model'):
        chosen=ranked(part,part.score,3)
        paired.append(dict(model=name,**paired_block_intervals(part,chosen,baseline)))
        for slug,g in part.groupby('event_slug'):
            s=chosen.loc[chosen.event_slug.eq(slug)]
            contributions.append(dict(model=name,event_slug=slug,positive=int(g.target.eq(1).sum()),captured=int(s.target.eq(1).sum()),
                peak_value=float(g.value_target.sum()),captured_value=float(s.value_target.sum())))
    pd.DataFrame(paired).to_csv(analysis/'paired_capture_intervals.csv',index=False)
    contribution=pd.DataFrame(contributions)
    contribution.to_csv(analysis/'event_capture.csv',index=False)
    fixed=[]
    for method in ('logistic','random_forest','xgboost'):
        a=primary.loc[primary.model.eq('fixed_book_'+method)].iloc[0]
        b=primary.loc[primary.model.eq('fixed_combined_'+method)].iloc[0]
        fixed.append(dict(method=method,book_recall=a.recall,combined_recall=b.recall,recall_change=b.recall-a.recall,
            book_value_capture=a.peak_value_recall,combined_value_capture=b.peak_value_recall,value_change=b.peak_value_recall-a.peak_value_recall))
    pd.DataFrame(fixed).to_csv(analysis/'metadata_comparison.csv',index=False)
    weekly=metrics.loc[metrics.scope.eq('all_events')&metrics.fold.str.startswith('week_')&metrics.top_n.eq(3)]
    weekly.to_csv(analysis/'weekly_metrics.csv',index=False)
    fig,axes=plt.subplots(1,2,figsize=(14,9),layout='constrained')
    for ax,key,title in zip(axes,('recall','peak_value_recall'),('Known positive-hour capture','Peak quoted-value capture')):
        values=primary[key].to_numpy()*100
        ax.barh(range(len(primary)),values,color=['#CC3311' if x=='quoted_gap_rule' else '#4477AA' for x in primary.model])
        ax.set_yticks(range(len(primary)),[LABELS[x] for x in primary.model]);ax.invert_yaxis();ax.set_xlim(0,113)
        for i,v in enumerate(values):ax.text(v+1,i,f'{v:.1f}%',va='center',fontsize=8)
        ax.set_title(title);ax.set_xlabel('Percent captured')
    fig.suptitle('Weekly expanding-window screening · N=3 · full forward interval')
    fig.savefig(analysis/'primary_capture.png',dpi=140);plt.close(fig)
    shortlist=list(dict.fromkeys(['quoted_gap_rule','tuned_combined_logistic','tuned_combined_random_forest',
        'tuned_combined_xgboost','tabm_two_stage','rank_ensemble',best]))
    fig,axes=plt.subplots(2,1,figsize=(14,9),layout='constrained')
    for ax,key,title in zip(axes,('recall','peak_value_recall'),('Positive-hour capture by week','Quoted-value capture by week')):
        grid=weekly.loc[weekly.model.isin(shortlist)].pivot(index='model',columns='fold',values=key).reindex(shortlist)
        ax.imshow(grid,aspect='auto',vmin=0,vmax=1,cmap='YlGnBu')
        ax.set_yticks(range(len(grid)),[LABELS[x] for x in grid.index])
        ax.set_xticks(range(len(grid.columns)),[x.replace('week_','W') for x in grid.columns])
        for i in range(len(grid)):
            for j in range(len(grid.columns)):
                v=grid.iloc[i,j];ax.text(j,i,f'{100*v:.0f}%' if pd.notna(v) else '—',ha='center',va='center',fontsize=8,color='white' if pd.notna(v) and v>.7 else 'black')
        ax.set_title(title+'; — means no known positive target')
    fig.savefig(analysis/'weekly_capture.png',dpi=140);plt.close(fig)
    latest=models/'folds'/folds()[-1][0]
    write_json(analysis/'research_selection.json',dict(policy=best,objective='Pooled peak quoted-value capture@3, ties positive-hour capture then AP.',
        selection_basis='Retrospective comparison of saved chronological predictions; no PnL-based fitting or tuning.',
        latest_bundle=str((latest/'bundle.json').relative_to(PROJECT_ROOT)),latest_bundle_sha256=sha256(latest/'bundle.json'),
        fitted_before=str(folds()[-1][1]),inference='src.models.walk_forward.score_saved(folder,bundle,as_of_features)[policy]'))
    economics_ready=(replay/'complete.json').exists()
    if economics_ready:
        verify_replay(replay)
        summary=pd.read_csv(replay/'summary.csv')
        econ=summary.loc[summary.scope.eq('all_events')&summary.fold.eq('all')&summary.top_n.eq(3)&summary.arrival_delay_s.eq(.1)]
        comparison=primary.merge(econ[['model','known_pnl_usd','attempts','completed','unknown_attempts','partial_pnl_usd','max_drawdown_usd']],on='model',validate='one_to_one')
        comparison.to_csv(analysis/'prediction_and_economics.csv',index=False)
        weekly_econ=summary.loc[summary.scope.eq('all_events')&summary.top_n.eq(3)&summary.arrival_delay_s.eq(.1)&summary.fold.str.startswith('week_')]
        order=[x[0] for x in folds()]
        values=weekly_econ.pivot(index='model',columns='fold',values='known_pnl_usd').reindex(columns=order,fill_value=0)
        draws=np.random.default_rng(20261031).integers(len(order),size=(10000,len(order)))
        economic_intervals=[]
        for name in primary.model:
            difference=values.loc[name].to_numpy()-values.loc['quoted_gap_rule'].to_numpy()
            low,high=np.quantile(difference[draws].sum(axis=1),[.025,.975])
            economic_intervals.append(dict(model=name,difference_usd=float(difference.sum()),lower95=float(low),upper95=float(high),blocks=len(order)))
        pd.DataFrame(economic_intervals).to_csv(analysis/'paired_pnl_intervals.csv',index=False)
        attempts=pd.read_parquet(replay/'attempts.parquet')
        taken=attempts.loc[attempts.attempted].copy()
        taken['accounting_time']=taken.arrival_time.where(taken.status.eq('complete'),taken.unwind_time)
        curves=taken.loc[taken.pnl_usd.notna()].groupby(['scope','top_n','model','arrival_delay_s','accounting_time'],as_index=False).pnl_usd.sum().sort_values('accounting_time')
        curves['cumulative_pnl_usd']=curves.groupby(['scope','top_n','model','arrival_delay_s']).pnl_usd.cumsum()
        totals=curves.groupby(['scope','top_n','model','arrival_delay_s']).pnl_usd.sum()
        expected=summary.loc[summary.fold.eq('all')].set_index(['scope','top_n','model','arrival_delay_s']).known_pnl_usd
        if not np.allclose(totals.reindex(expected.index,fill_value=0),expected):raise ValueError('PnL curves fail to reconcile')
        curves.to_parquet(analysis/'pnl_curves.parquet',index=False)
        event_pnl=taken.groupby(['scope','top_n','model','arrival_delay_s','event_slug'],as_index=False).pnl_usd.sum()
        event_pnl.to_csv(analysis/'event_pnl.csv',index=False)
        random=econ.loc[econ.model.str.startswith('random_')].known_pnl_usd
        write_json(analysis/'random_economics.json',dict(seeds=len(random),mean=float(random.mean()),minimum=float(random.min()),maximum=float(random.max()),std=float(random.std())))
        fig,axes=plt.subplots(1,2,figsize=(14,9),layout='constrained')
        for ax,n in zip(axes,(3,5)):
            grid=summary.loc[summary.scope.eq('all_events')&summary.fold.eq('all')&summary.top_n.eq(n)&~summary.model.str.startswith('random_')].pivot(index='model',columns='arrival_delay_s',values='known_pnl_usd').reindex(list(primary.model)+['monitor_all'])
            ax.imshow(grid,aspect='auto',cmap='RdYlGn',vmin=min(0,grid.min().min()),vmax=grid.max().max())
            ax.set_yticks(range(len(grid)),[LABELS[x] for x in grid.index]);ax.set_xticks(range(len(grid.columns)),[int(x*1000) for x in grid.columns])
            for i in range(len(grid)):
                for j in range(len(grid.columns)):ax.text(j,i,f'${grid.iloc[i,j]:,.2f}',ha='center',va='center',fontsize=8)
            ax.set_title(f'Simulated known P&L · N={n}');ax.set_xlabel('Arrival delay (ms)')
        fig.savefig(analysis/'latency_economics.png',dpi=140);plt.close(fig)
        fig,axes=plt.subplots(3,3,figsize=(14,10),layout='constrained')
        for ax,name in zip(axes.flat,shortlist):
            for model,color in [('quoted_gap_rule','gray'),(name,'#4477AA')]:
                p=curves.loc[curves.scope.eq('all_events')&curves.top_n.eq(3)&curves.arrival_delay_s.eq(.1)&curves.model.eq(model)]
                value=float(p.cumulative_pnl_usd.iloc[-1]) if len(p) else 0.
                ax.step([FIRST]+p.accounting_time.tolist()+[END],[0.]+p.cumulative_pnl_usd.tolist()+[value],where='post',color=color,linewidth=2 if model=='quoted_gap_rule' else 1.2)
            ax.set_title(LABELS[name],fontsize=10);ax.set_ylabel('Cumulative known P&L ($)');ax.tick_params(axis='x',rotation=25,labelsize=7)
            total=float(econ.loc[econ.model.eq(name),'known_pnl_usd'].iloc[0]);ax.text(.03,.92,f'${total:,.2f}',transform=ax.transAxes)
        for ax in list(axes.flat)[len(shortlist):]:ax.set_visible(False)
        fig.suptitle('100 ms, N=3 · separate panels; gray = quoted-gap')
        fig.savefig(analysis/'pnl_small_multiples.png',dpi=140);plt.close(fig)
    original=json.loads((PROJECT_ROOT/'data/pmxt/pilot_v1/dome_before.json').read_text())
    changed=[p for p,d in original.items() if not Path(p).is_file() or sha256(Path(p))!=d]
    if changed:raise ValueError(f'Original protected files changed: {changed[:3]}')
    write_json(analysis/'verification.json',dict(**verified,original_files_checked=len(original),original_files_unchanged=True,economic_replay_verified=economics_ready))
    reference=primary.loc[primary.model.eq('quoted_gap_rule')].iloc[0]
    winner=primary.iloc[0]
    display=primary[['model','captured_positive_windows','recall','peak_value_recall']].copy()
    display[['recall','peak_value_recall']]*=100
    result_table=table(display,[('model','Policy'),('captured_positive_windows','Captured positive hours'),('recall','Recall %'),('peak_value_recall','Value capture %')])
    concentration=contribution.loc[contribution.model.eq('quoted_gap_rule')].sort_values('peak_value',ascending=False)
    largest=concentration.iloc[0]
    economic_text='Economic replay has not yet completed.'
    if economics_ready:
        economic_table=table(comparison[['model','known_pnl_usd','attempts','completed','unknown_attempts']],
            [('model','Policy'),('known_pnl_usd','Known P&L $'),('attempts','Attempts'),('completed','Completed'),('unknown_attempts','Unknown attempts')])
        economic_text='At 100 ms and N=3, the persistent replay gives:\n\n'+economic_table+f'\n\nThe mean random-policy P&L is ${random.mean():,.2f} over 20 fixed seeds. These are simulated known returns, excluding and counting unknown outcomes, rather than exchange-confirmed profits.'
        selected_econ=econ.loc[econ.model.eq(best)].iloc[0]
        economic_text+=f'''\n\nFor the opportunity-selected policy, completed baskets contribute
${selected_econ.completed_profit_usd:,.2f} and partial baskets contribute
${selected_econ.partial_pnl_usd:,.2f}. Maximum drawdown is
${selected_econ.max_drawdown_usd:,.2f}; the largest order is
{int(selected_econ.largest_quantity):,} shares. Its no-settlement funding
scenario requires ${selected_econ.no_settlement_funding_usd:,.2f}, assuming no
completed-basket settlement cash is released during the study. This is a
funding scenario, not measured deployed capital or an ROI denominator.
Monitor-all produces ${float(econ.loc[econ.model.eq('monitor_all'),'known_pnl_usd'].iloc[0]):,.2f}.
Paired weekly P&L differences are reported separately; execution outcomes did
not choose or train the opportunity model.'''
        highest=comparison.sort_values('known_pnl_usd',ascending=False).iloc[0]
        excluded=summary.loc[summary.scope.eq('exclude_december_fed')&summary.fold.eq('all')&summary.top_n.eq(3)&summary.arrival_delay_s.eq(.1)]
        excluded_best=float(excluded.loc[excluded.model.eq(best),'known_pnl_usd'].iloc[0])
        excluded_quote=float(excluded.loc[excluded.model.eq('quoted_gap_rule'),'known_pnl_usd'].iloc[0])
        economic_text+=f'''\n\n**Prediction and economic winners differ.** {LABELS[highest.model]} has
the highest model P&L (${highest.known_pnl_usd:,.2f}) in this scenario. That
does not replace the opportunity-selected policy or make execution P&L a
learning target. Simple depth and freshness rules also achieve returns close
to the learned policies; most complex-policy differences are small compared
with their improvement over quoted-gap.

**The economic advantage over quoted-gap is concentrated in December Fed.**
After removing it and reranking, the opportunity-selected policy produces
${excluded_best:,.2f}, versus quoted-gap's ${excluded_quote:,.2f}. Thus this
study demonstrates improved opportunity capture within the cohort, but does
not establish a consistent profit advantage across different events.'''
    report=f'''# Weekly expanding-window Dome study

Completed 31 October 2026. This retrospective chronological model-development
study compares 17 policies, weekly refits, earlier-only periodic tuning, and a
separate persistent economic replay. It preserves the earlier completed studies.

## Period, population and target

The dataset spans 14 October 2025–3 February 2026 at 08:00 UTC. The initial
28 calendar days supply training; evaluation runs from **11 November 2025 to
3 February 2026 at 08:00 UTC**, with the endpoint excluded: **84 days 8 hours**.
There are 13 weekly fits, including a final eight-hour evaluation block.
Known training examples grow from {initial_training:,} to {latest_training:,},
with {latest_positive} positive examples in the last fitted history. The model
does not remain restricted to its initial 28 days.

The evaluated pool contains {population['event_hours']:,} event-hours from
{population['events']} events, {population['known']:,} known labels,
{population['positive']} positive hours from {population['positive_events']}
events, and {population['unknown']:,} unknown outcomes. Median candidate count
is {population['median_candidates']:.1f}, maximum {population['maximum_candidates']};
{population['competitive_hours_n3']:,} hours have more than three candidates.
Calendar span is not complete continuous coverage for every event.

Occurrence means a qualifying basket appears during the next hour. Value is
the maximum qualifying displayed one-basket dollar margin at one state in that
hour. It is not additive hourly profit. Known training windows must finish
strictly before a fitting origin; missing labels never become negatives.
All eligible rows compete before scoring consults their labels.

The new data adapter explicitly joins the already constructed development and
later segments. Their metadata and label contracts are verified, not rebuilt.
Later observations can enter a subsequent training history only after their
target windows have ended. Original final-study artifacts remain unchanged.

## Models, tuning and features

Logistic regression, random forest and XGBoost have fixed book-only and fixed
combined-feature controls, plus tuned combined-feature fits. The fixed views
isolate adding metadata while keeping model settings unchanged. There are 33
book features and 36 combined features. Added metadata is event type, broad
topic and age; event identifiers and realised outcomes are not predictors.
See [the dataset guide](../dataset.md) for exact feature meanings.

Each classical family searches the existing eight settings. Two-stage TabM
searches four declared configurations (original indices 0, 1, 2, 4), using a
probability gate and positive-only value stage. A fixed two-stage forest is a
value reference. TabM uses 32 implicit members per stage, batch size 1,024,
at most 60 epochs and patience 10, a bounded adaptation for this dataset.

Tuning occurs on 11 November, 9 December and 6 January. Every candidate uses
two purged chronological inner blocks ending before its tuning origin. All
families select by captured peak quoted value at N=3, then captured positives,
mean inner average precision and declaration order. TabM uses the first fixed
seed during tuning. Weekly refits average three seed-specific gate × amount
products. Refit epochs are the ceiling of the median selected inner epochs;
settings and epochs carry forward between tuning dates. No weekly evaluation
outcomes choose hyperparameters, early stopping or ensemble weights.
There are 168 inner candidate evaluations. If a tuned setting equals a fixed
control, the weekly bundle shares its fitted artifact rather than treating
identical fits as independent supporting evidence.

Three fixed ensembles average within-scan percentile ranks: the three tuned
classifiers; tuned logistic, tuned forest and TabM; or the two value models.
Quoted-gap, depth and freshness are rule references. Random capture is an exact
candidate-pool expectation. N=3 is primary and N=5 secondary.

## Opportunity prediction

{result_table}

Expected random selection captures
{100*reference.expected_random_captured/reference.positive_windows:.2f}% of
known positive hours and
{100*reference.expected_random_peak_value/reference.total_peak_value:.2f}% of
quoted value at the same N=3 budget. Monitor-all captures all known targets
using the full monitoring pool.

By the primary pooled value-capture criterion, **{LABELS[best]}** ranks first
({100*winner.peak_value_recall:.2f}% value capture,
{100*winner.recall:.2f}% positive-hour recall). Quoted-gap captures
{100*reference.peak_value_recall:.2f}% and {100*reference.recall:.2f}%, respectively.
This policy choice is a retrospective comparison of chronological predictions;
it does not change those predictions or establish a universal market winner.

Tuning and ensembles do not consistently improve the fixed baselines. TabM
does not lead this comparison. Adding metadata improves fixed logistic value
capture slightly and fixed XGBoost value capture substantially, while slightly
reducing fixed forest value capture. The matched fixed-feature comparisons are
saved in `analysis/metadata_comparison.csv`.

![Primary capture](../../results/walk_forward/analysis/primary_capture.png)

Weekly metrics, the original earlier/later recording segments, N=5, and a
reranked exclusion of December Fed are reported separately. Weeks with no
known positive target have undefined recall, not zero model performance.
Pooled capture and stability are more informative than a count of weekly wins.

![Weekly capture](../../results/walk_forward/analysis/weekly_capture.png)

## Economic evaluation

{economic_text}

The primary scenario has **100 ms total source-update-to-order-arrival delay**,
uncapped capital and shares, available-depth sizing, zero imposed failure
probability, immediate submission, frozen decision-time limits and independent
per-leg FOK support. Partial baskets unwind after one second. There is no
quote-age or persistence cutoff. The frozen cohort zero-exchange-fee scenario
and zero additional per-attempt costs are retained.

Every policy and scenario starts empty at the first evaluation hour and retains
its event-level reservations and in-flight state through all weekly refits and
the original January segment boundary. Identical unchanged liquidity is not
repeatedly spent. Required token histories use backward as-of reads bounded by
each token's recording endpoint, with terminal context for outstanding exits.
No cross-event tokens are shared in the replayed universe.

Deterministic policies are evaluated at 0, 50, 100, 250 and 500 ms for N=3/5.
Twenty fixed random seeds run at 100 ms. The December-Fed exclusion reranks
the remaining universe and runs at N=3, 100 ms. Segment and weekly P&L describe
one continuous replay, not independently restarted accounts. Completed basket
margins are booked at completion rather than settlement cash receipt; unlimited
funding is assumed. Monitor-all is a resource reference, not a guaranteed
profit upper bound when trading paths differ.

## Concentration and interpretation

The largest quoted-value contribution is `{largest.event_slug}`:
{100*largest.peak_value/population['total_peak_value']:.2f}% of the weighting
denominator. Event contributions, the December-Fed exclusion and paired
seven-day block intervals are saved. There are only 13 blocks, with a short
last block; intervals condition on these fitted policies and this cohort,
not refitting, universe selection or missing-outcome uncertainty. Multiple
policy comparisons are not adjusted.

Late-period candidate competition is weaker than in November–December. Pooled
results therefore need their segment breakdown. Comparisons share the same
hourly candidate pool but known-outcome metrics cannot measure missed
opportunities during unknown recordings. Frozen retrospective membership and
metadata, source-time alignment, recorded-depth fill support and feed coverage
remain limitations. These results concern the selected Dome cohort.

## Reproduction and artifacts

```powershell
.venv/Scripts/python.exe -m src.data.walk_forward --output data/processed/walk_forward_new
.venv/Scripts/python.exe -m src.models.walk_forward --data data/processed/walk_forward_new --output results/runs/walk_forward_new
.venv/Scripts/python.exe -m src.simulator.walk_forward --models results/runs/walk_forward_new --output results/runs/walk_forward_replay_new
.venv/Scripts/python.exe -m src.reporting.walk_forward --data data/processed/walk_forward_new --models results/runs/walk_forward_new --replay results/runs/walk_forward_replay_new
```

Models default to CUDA when available; `--device cpu` is supported. The run
records its device and implementation hashes; deterministic settings do not
guarantee bitwise equality between hardware backends. Tuning and weekly model
checkpoints resume only under the same frozen code/data design.

[Notebook 18](../../notebooks/18_dome_walk_forward.ipynb) verifies and displays
the saved study and optionally runs a fresh reproduction. Data is under
`data/processed/walk_forward_v1/`, models and prediction analysis under
`results/walk_forward/`, and persistent economics under
`results/walk_forward_replay/`. `analysis/research_selection.json` identifies
the selected policy and latest saved weekly bundle for supplied as-of features.
The report verifier reproduces predictions from the fitted models, reconstructs
tuning/forward metrics and reconciles P&L curves with the economic summaries.
All {len(original):,} previously protected Dome files remain unchanged.
'''
    report_path=PROJECT_ROOT/'docs/reports/dome_walk_forward.md' if models.resolve()==MODELS.resolve() else analysis/'report.md'
    relative=lambda p:os.path.relpath(p,report_path.parent).replace(os.sep,'/')
    report=report.replace('../../results/walk_forward/analysis/',relative(analysis)+'/')
    report=report.replace('../dataset.md',relative(PROJECT_ROOT/'docs/dataset.md'))
    report=report.replace('../../notebooks/18_dome_walk_forward.ipynb',relative(PROJECT_ROOT/'notebooks/18_dome_walk_forward.ipynb'))
    report_path.write_text(report,encoding='utf-8')
    write_json(analysis/'manifest.json',dict(models_completion_sha256=sha256(models/'complete.json'),
        replay_completion_sha256=sha256(replay/'complete.json') if economics_ready else None,
        reporter_sha256=sha256(Path(__file__)),files_sha256={p.name:sha256(p) for p in analysis.iterdir() if p.is_file() and p.name!='manifest.json'}))
    print('Selected opportunity policy:',best)
    print(primary[['model','recall','peak_value_recall']].to_string(index=False))
    if economics_ready:print(comparison[['model','known_pnl_usd','unknown_attempts']].to_string(index=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--models',type=Path,default=MODELS)
    parser.add_argument('--replay',type=Path,default=REPLAY)
    parser.add_argument('--data',type=Path,default=DATA)
    args=parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        analyze(args.models,args.replay,args.data)
