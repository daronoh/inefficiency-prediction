"""Readable P&L comparisons, trade-level accounting and execution sensitivities."""
import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from src.models.settings import MODEL_NAMES, method_name, ensemble_name
from src.reporting.workflow import execution_path, timing_path, capacity_path

MODEL_LABELS = {'logistic':'Logistic regression', 'random_forest':'Random forest',
                'boosted_trees':'Boosted trees', 'rank_ensemble':'Rank ensemble'}
COLORS = {'book':'#1764ab', 'metadata':'#d97818', 'combined':'#20834e'}


def pnl_timeline(run_dir, execution_dir=None):
    """Known payoff-valued P&L at entry completion or partial-basket unwind time."""
    run_dir = Path(run_dir)
    frame = pd.read_parquet((Path(execution_dir) if execution_dir is not None else execution_path(run_dir))/'attempts.parquet')
    frame = frame.loc[frame.attempted & frame.pnl_usd.notna()].copy()
    frame['accounting_time'] = frame.arrival_time.where(frame.status.eq('complete'), frame.unwind_time)
    increments = frame.groupby(['accounting_time','model']).pnl_usd.sum().unstack(fill_value=0.)
    design = json.loads((run_dir/'design.json').read_text())
    endpoints = pd.to_datetime([design['folds'][0][1], design['folds'][-1][2]], utc=True)
    return increments.reindex(increments.index.union(endpoints).sort_values(), fill_value=0.).cumsum()


def _format_time(ax):
    locator = mdates.AutoDateLocator(minticks=3, maxticks=6)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    ax.grid(alpha=.2)


def plot_pnl(run_dir, execution_dir=None, scenario='Original capped 500 ms'):
    curves = pnl_timeline(run_dir, execution_dir)
    fig, axes = plt.subplots(2,2,figsize=(13,8),sharex=True,sharey=True)
    for ax, model in zip(axes.flat, MODEL_NAMES+['rank_ensemble']):
        for view in ('book','metadata','combined'):
            key = ensemble_name(view) if model == 'rank_ensemble' else method_name(model,view)
            ax.step(curves.index, curves[key], where='post', color=COLORS[view], label=view.capitalize(), lw=1.6)
        ax.step(curves.index,curves.quoted_gap_rule,where='post',color='#353535',ls=':',label='Quoted-gap rule',lw=1.2)
        ax.step(curves.index,curves.monitor_all,where='post',color='#999999',ls='--',label='Monitor all',lw=1.2)
        ax.set_title(MODEL_LABELS[model])
        ax.set_ylabel('Cumulative known P&L ($)')
        _format_time(ax)
    handles,labels=axes[0,0].get_legend_handles_labels()
    fig.legend(handles,labels,loc='upper center',ncol=5,bbox_to_anchor=(.5,.96))
    fig.suptitle(f'{scenario}: known P&L by model family',y=.98)
    fig.supxlabel('Execution accounting time (UTC); guaranteed payout value, not settled cash')
    fig.tight_layout(rect=(0,.025,1,.915))
    return fig


def plot_pnl_difference(run_dir, execution_dir=None, scenario='Original capped 500 ms'):
    curves = pnl_timeline(run_dir, execution_dir)
    fig,axes=plt.subplots(2,2,figsize=(13,8),sharex=True)
    for ax,model in zip(axes.flat,MODEL_NAMES+['rank_ensemble']):
        for view in ('book','combined'):
            key=ensemble_name(view) if model=='rank_ensemble' else method_name(model,view)
            difference=curves[key]-curves.quoted_gap_rule
            ax.step(curves.index,difference,where='post',color=COLORS[view],label=f'{view.capitalize()}: final ${difference.iloc[-1]:+.2f}',lw=1.8)
        ax.axhline(0,color='#444444',ls=':',lw=1.)
        ax.set_title(MODEL_LABELS[model])
        ax.set_ylabel('P&L minus quoted-gap rule ($)')
        ax.legend(fontsize=9,loc='best')
        _format_time(ax)
    fig.suptitle(f'{scenario}: advantage over quoted-gap rule (panel scales differ)',y=.98)
    fig.supxlabel('Execution accounting time (UTC)')
    fig.tight_layout(rect=(0,.025,1,.96))
    return fig


def trade_economics(run_dir, execution_dir=None):
    frame=pd.read_parquet((Path(execution_dir) if execution_dir is not None else execution_path(run_dir))/'attempts.parquet')
    rows=[]
    for model in ['logistic','random_forest','boosted_trees','rank_ensemble','quoted_gap_rule','monitor_all']:
        part=frame.loc[frame.model.eq(model)&frame.attempted]
        complete=part.loc[part.status.eq('complete')]
        partial=part.loc[part.status.str.startswith('partial')]
        rows.append(dict(model=model,completed=len(complete),completed_profit_usd=complete.pnl_usd.sum(),
            median_completed_usd=complete.pnl_usd.median(),mean_completed_usd=complete.pnl_usd.mean(),
            partial_attempts=len(partial),partial_pnl_usd=partial.pnl_usd.sum(),
            worst_partial_usd=partial.pnl_usd.min(),known_pnl_usd=part.pnl_usd.sum(),
            unknown_attempts=int(part.pnl_usd.isna().sum())))
    return pd.DataFrame(rows).set_index('model')


def timing_table(run_dir):
    return pd.read_csv(timing_path(run_dir)/'summary.csv').set_index(['scenario','model'])


def capacity_comparison(run_dir):
    """Separate the old latency change from uncapped sizing/discovery at 100 ms."""
    original = pd.read_csv(execution_path(run_dir)/'summary.csv').set_index('model')
    capacity = pd.read_csv(capacity_path(run_dir)/'summary.csv').set_index('model')
    names = [method_name(name, view) for view in ('book','combined') for name in MODEL_NAMES]
    names += [ensemble_name(view) for view in ('book','combined')] + ['quoted_gap_rule','monitor_all']
    shown = capacity.reindex(names)[['known_pnl_usd','completed_profit_usd','partial_pnl_usd',
        'unknown_attempts','largest_quantity','largest_reserved_cash_usd']].copy()
    shown = shown.rename(columns={'known_pnl_usd':'uncapped_100ms_pnl_usd'})
    shown.insert(0,'capped_500ms_pnl_usd',original.known_pnl_usd.reindex(names))
    if (timing_path(run_dir)/'summary.csv').exists():
        timing = timing_table(run_dir).loc['arrival_100ms']
        shown.insert(1,'capped_100ms_pnl_usd',timing.known_pnl_usd.reindex(names))
    return shown


def capacity_events(run_dir, model='logistic'):
    """Show concentration without treating unknown outcomes as zero."""
    frame = pd.read_parquet(capacity_path(run_dir)/'attempts.parquet')
    taken = frame.loc[frame.model.eq(model) & frame.attempted].copy()
    taken['unknown'] = taken.pnl_usd.isna()
    return taken.groupby('event_slug').agg(attempts=('attempted','size'),
        known_pnl_usd=('pnl_usd','sum'), unknown_attempts=('unknown','sum'),
        largest_reserved_cash_usd=('reserved_cash_usd','max')).sort_values('known_pnl_usd',ascending=False)


def plot_timing(run_dir):
    table=timing_table(run_dir).reset_index()
    rows=table.loc[table.scenario.str.startswith('arrival_')].copy()
    rows['delay_ms']=rows.arrival_delay_s*1000
    fig,axes=plt.subplots(2,2,figsize=(12,7),sharex=True)
    for ax,model in zip(axes.flat,MODEL_NAMES+['rank_ensemble']):
        for view in ('book','combined'):
            key=ensemble_name(view) if model=='rank_ensemble' else method_name(model,view)
            part=rows.loc[rows.model.eq(key)].sort_values('delay_ms')
            ax.plot(part.delay_ms,part.known_pnl_usd,marker='o',color=COLORS[view],label=view.capitalize())
        for name,color,label in [('quoted_gap_rule','#555555','Quoted-gap rule'),('monitor_all','#999999','Monitor all')]:
            part=rows.loc[rows.model.eq(name)].sort_values('delay_ms')
            ax.plot(part.delay_ms,part.known_pnl_usd,ls='--',color=color,label=label)
        ax.set_title(MODEL_LABELS[model])
        ax.set_ylabel('Known P&L ($)')
        ax.set_xscale('symlog',linthresh=10)
        ax.set_xticks([0,10,50,100,500],['0','10','50','100','500'])
        ax.grid(alpha=.2)
    handles,labels=axes[0,0].get_legend_handles_labels()
    fig.legend(handles,labels,loc='upper center',ncol=4,bbox_to_anchor=(.5,.96))
    fig.suptitle('Execution delay sensitivity: fixed rankings and one-second unwind/hold',y=.98)
    fig.supxlabel('Assumed arrival delay (ms; nonlinear spacing) — zero is an optimistic benchmark')
    fig.tight_layout(rect=(0,.035,1,.90))
    return fig
