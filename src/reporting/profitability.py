"""Profit-first research tables, readable curves and reproducibility checks."""
import json
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from src.common import sha256
from src.config import ScreeningSpec
from src.data.profitability import load_profitability
from src.models.settings import FOLDS, method_name
from src.models.evaluation import ranked
from src.paths import PROJECT_ROOT, DATA_DIR, REPORTS_DIR
from src.reporting.workflow import execution_path
from src.reporting.results import markdown_table
from src.reporting.economics import _format_time

PRIMARY = ['logistic','random_forest','boosted_trees','rank_ensemble',
    'reg_ridge','reg_random_forest','reg_boosted_trees','reg_ensemble',
    'two_stage_ridge','two_stage_random_forest','two_stage_boosted_trees','two_stage_ensemble']


def comparison(run_dir,scope='all_events',view='book'):
    root=Path(run_dir)
    names=[method_name(name,view) for name in PRIMARY]+['quoted_gap_rule','depth_rule','freshness_rule','monitor_all']
    metrics=pd.read_csv(root/'metrics.csv').query('top_n == 5').set_index('model')
    economic=pd.read_csv(execution_path(root)/'summary.csv')
    economic=economic.loc[economic.scope.eq(scope)&economic.fold.eq('all')].set_index('model')
    table=economic.reindex(names)[['known_pnl_usd','completed_profit_usd','partial_pnl_usd','attempts',
        'unknown_attempts','max_drawdown_usd','largest_reserved_cash_usd','cumulative_purchase_cash_usd']]
    if scope == 'all_events':
        table=table.join(metrics[['recall','precision_known','mae_usd','rmse_usd','selected_unknown']])
    return table


def timeline(run_dir,scope='all_events'):
    rows=pd.read_parquet(execution_path(run_dir)/'attempts.parquet')
    rows=rows.loc[rows.scope.eq(scope)&rows.attempted&rows.pnl_usd.notna()].copy()
    rows['accounting_time']=rows.arrival_time.where(rows.status.eq('complete'),rows.unwind_time)
    cost=json.loads((Path(run_dir)/'design.json').read_text())['dataset_design']['contract']['assumed_other_cost_usd']
    rows['net']=rows.pnl_usd-cost
    increments=rows.groupby(['accounting_time','model']).net.sum().unstack(fill_value=0.)
    names=pd.read_csv(execution_path(run_dir)/'summary.csv').model.unique()
    increments=increments.reindex(columns=names,fill_value=0.)
    endpoints=pd.to_datetime([FOLDS[0][1],FOLDS[-1][2]],utc=True)
    return increments.reindex(increments.index.union(endpoints).sort_values(),fill_value=0.).cumsum()


def plot_profitability(run_dir,view='book',difference=False):
    curves=timeline(run_dir)
    fig,axes=plt.subplots(2,2,figsize=(13,8),sharex=True,sharey=not difference)
    for ax,base,cls,title in zip(axes.flat,['ridge','random_forest','boosted_trees','ensemble'],
        ['logistic','random_forest','boosted_trees','rank_ensemble'],['Linear models','Random forest','Boosted trees','Ensembles']):
        for name,label,color in [(cls,'Opportunity classifier','#1764ab'),('reg_'+base,'Direct P&L','#d97818'),
                                 ('two_stage_'+base,'Two-stage P&L','#20834e')]:
            values=curves[method_name(name,view)]
            if difference:
                values=values-curves.quoted_gap_rule
            ax.step(curves.index,values,where='post',color=color,lw=1.5,label=f'{label}: ${values.iloc[-1]:,.0f}')
        if difference:
            ax.axhline(0,color='#555555',ls=':')
        else:
            ax.step(curves.index,curves.quoted_gap_rule,where='post',color='#555555',ls=':',label='Quoted-gap rule')
        ax.set_title(title)
        ax.set_ylabel('P&L minus quoted-gap rule ($)' if difference else 'Cumulative known P&L ($)')
        ax.legend(fontsize=8,loc='best')
        _format_time(ax)
    fig.suptitle(f'{view.capitalize()} features: '+('advantage over rule; panel scales differ' if difference else '100 ms, uncapped replay'),y=.99)
    fig.supxlabel('UTC execution accounting time; payoff-valued P&L, not a settled cash balance')
    fig.tight_layout(rect=(0,.03,1,.965))
    return fig


def event_returns(run_dir,model='two_stage_boosted_trees',scope='all_events'):
    rows=pd.read_parquet(execution_path(run_dir)/'attempts.parquet')
    rows=rows.loc[rows.scope.eq(scope)&rows.model.eq(model)&rows.attempted].copy()
    rows['unknown']=rows.pnl_usd.isna()
    return rows.groupby('event_slug').agg(known_pnl_usd=('pnl_usd','sum'),attempts=('attempted','size'),
        unknown_attempts=('unknown','sum')).sort_values('known_pnl_usd',ascending=False)


def audit_profitability(run_dir,dataset_dir):
    root,folder=Path(run_dir),Path(dataset_dir)
    design=json.loads((root/'design.json').read_text())
    if design['dataset_sha256'] != sha256(folder/'examples.parquet'):
        raise ValueError('Active profitability dataset differs from the model run; retrain before reporting')
    completion=json.loads((root/'complete.json').read_text())
    if completion['predictions_sha256'] != sha256(root/'predictions.parquet'):
        raise ValueError('Model prediction completion hash differs')
    frame=load_profitability(folder)
    prediction=pd.read_parquet(root/'predictions.parquet')
    assert not prediction.duplicated(['model','event_slug','time']).any()
    assert np.isfinite(prediction.score).all()
    assert prediction.loc[~prediction.label_known,['pnl_target','activity_target','target']].isna().all().all()
    assert prediction.time.max() < pd.Timestamp(design['dataset_design']['contract']['lockbox_start'])
    baseline=None
    for name,part in prediction.groupby('model'):
        keys=set(zip(part.event_slug,part.time))
        if baseline is None:
            baseline=keys
        assert keys == baseline
        selected=ranked(part,part.score,5)
        assert selected.groupby('time').size().le(5).all()
    fits=pd.read_csv(root/'diagnostics/fit_audit.csv')
    gap=pd.to_datetime(fits.test_first_time,utc=True)-pd.to_datetime(fits.train_last_time,utc=True)
    assert gap.dt.total_seconds().gt(design['embargo_seconds']).all()
    assert frame.loc[frame.label_known & frame.activity_target.eq(0),'pnl_target'].eq(0).all()
    attempts=pd.read_parquet(execution_path(root)/'attempts.parquet')
    assert ((attempts.arrival_time-attempts.confirmation_time).dt.total_seconds() == .1).all()
    summary=pd.read_csv(execution_path(root)/'summary.csv').query("fold == 'all'")
    cost=design['dataset_design']['contract']['assumed_other_cost_usd']
    for scope,part in summary.groupby('scope'):
        curve=timeline(root,scope)
        for row in part.itertuples():
            actual=attempts.loc[attempts.scope.eq(scope)&attempts.model.eq(row.model)&attempts.attempted]
            assert abs(actual.pnl_usd.sum()-len(actual)*cost-row.known_pnl_usd) < 1e-7
            assert int(actual.pnl_usd.isna().sum()) == row.unknown_attempts
            assert abs(curve[row.model].iloc[-1]-row.known_pnl_usd) < 1e-7
    assert not attempts.loc[attempts.scope.eq('exclude_december_fed'),'event_slug'].eq('fed-decision-in-december').any()
    result=dict(final_lockbox_opened=False,models=prediction.model.nunique(),
        evaluation_event_windows=len(baseline),known_training_and_evaluation_hours=int(frame.label_known.sum()),
        checks=['identical candidate populations','unknown targets missing','finite scores','top-five budgets',
                'purged chronological fits','100 ms arrival','attempt/summary/curve accounting','dominant event excluded'])
    (root/'audit.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


def build_profitability_report(run_dir,dataset_dir):
    root=Path(run_dir)
    frame=load_profitability(dataset_dir)
    known=frame.loc[frame.label_known]
    summary=pd.read_csv(execution_path(root)/'summary.csv')
    full=summary.query("scope == 'all_events' and fold == 'all'").set_index('model')
    random=full.loc[full.index.str.match(r'random_\d+$'),'known_pnl_usd']
    paired=pd.read_csv(execution_path(root)/'paired_comparisons.csv')
    fits=pd.read_csv(root/'diagnostics/fit_audit.csv').query("model == 'reg_boosted_trees'").set_index('fold')
    metrics=pd.read_csv(root/'metrics.csv').query('top_n == 5').set_index('model')
    leaders=[]
    for label,base_names in [('Occurrence classifier',PRIMARY[:4]),('Direct P&L regression',PRIMARY[4:8]),('Two-stage P&L',PRIMARY[8:])]:
        names=base_names+[method_name(name,'combined') for name in base_names]
        winner=full.loc[names,'known_pnl_usd'].idxmax()
        leaders.append(dict(approach=label,model=winner,known_pnl_usd=full.loc[winner,'known_pnl_usd']))
    for name in ['quoted_gap_rule','monitor_all']:
        leaders.append(dict(approach='Reference',model=name,known_pnl_usd=full.loc[name,'known_pnl_usd']))
    text=['# Profitability-oriented development results','',
        'The study ranks events from scheduled snapshots to allocate a five-event monitoring budget. '
        'The primary objective is expected simulated net P&L during the next monitoring window. '
        'This is a development comparison; the final test from 8 January 2026 remains unopened.','',
        f'{len(frame):,} eligible event-windows from {frame.event_slug.nunique()} events; {len(known):,} known targets '
        f'and {int((~frame.label_known).sum()):,} unknown. Known hours contain '
        f'{int(known.pnl_target.gt(0).sum()):,} positive, {int(known.pnl_target.eq(0).sum()):,} zero and '
        f'{int(known.pnl_target.lt(0).sum()):,} negative net outcomes. '
        f'{int(known.activity_target.sum()):,} hours contain attempted trades.','',
        'The best observed development result within each main model family is shown below. '
        'These winners are selected after comparing variants and do not constitute final-test model selection.', '',
        markdown_table(pd.DataFrame(leaders).set_index('approach')), '',
        '## Target and execution policy','',
        'Targets are rebuilt for every event-window independently of model selections, using 100 ms arrival, '
        'uncapped integer-depth sizing, a one-second partial-basket unwind/hold, zero historical exchange fees '
        'for the included market types and no additional imposed failure probability. '
        'Incomplete coverage or an unpriceable attempted outcome leaves the target missing. '
        'Complete no-trade hours have a zero target. Prices and quantities are fixed before arrival. '
        'The book features that depend on basket sizing, their lags and the quoted-gap rule are also rebuilt without caps.','',
        'Standalone hourly labels start with an empty reservation ledger. Evaluation below replays each policy '
        'chronologically with reservations retained across windows and folds. Summing selected labels is only '
        'a diagnostic and is not the strategy result. Unknown hours still compete for monitoring slots.','',
        '## Models and chronological evaluation','',
        'Opportunity classifiers (logistic regression, random forest and boosted trees) are refitted to the '
        'new execution-derived occurrence labels. Direct regression uses ridge regression, random forest and '
        'boosted trees to predict signed dollars. Two-stage models estimate P(attempt) and conditional net P&L, '
        'then multiply them. The conditional model includes zero and losing trading hours. Gates are unweighted '
        'classifiers; out-of-sample reliability is reported rather than assuming calibrated probabilities. '
        'Classifier ensembles average percentile ranks; economic ensembles average dollar predictions. '
        'Every family uses book-only and book-plus-metadata inputs. Hyperparameters are fixed without a search.','',
        markdown_table(fits[['training_rows','trading_rows','loss_hours','train_last_time','test_first_time']]),'',
        'Training uses earlier known labels and purges the complete monitoring horizon plus 1.1 seconds. '
        'Preprocessing is fitted only on training rows; conditional preprocessing uses only earlier trading hours.','',
        '## Primary results: five events per scan','',
        'Dollar figures below come from persistent replay. Recall is capture of hours containing at least one '
        'positive attempt; it is secondary to net P&L. MAE/RMSE measure standalone dollar predictions and apply '
        'only to regression and two-stage models. Unknown execution counts qualify every known-P&L subtotal.','',
        'Book features:', '',markdown_table(comparison(root)), '',
        'Book features and metadata:', '',markdown_table(comparison(root,view='combined')), '',
        f'Twenty seeded random policies earn mean ${random.mean():,.2f}, minimum ${random.min():,.2f}, '
        f'and maximum ${random.max():,.2f}. Monitor-all is a reference policy, not a guaranteed profit upper bound.','',
        f'An always-zero dollar prediction has MAE ${metrics.zero_prediction_mae_usd.iloc[0]:.4f} and '
        f'RMSE ${metrics.zero_prediction_rmse_usd.iloc[0]:.4f}; the many zero hours make prediction-error metrics '
        'insufficient on their own for judging monitoring choices.','',
        '## Fold stability','',
        markdown_table(summary.loc[summary.scope.eq('all_events') & summary.fold.ne('all') &
            summary.model.isin(['logistic','reg_boosted_trees','two_stage_boosted_trees','quoted_gap_rule','monitor_all'])]
            .set_index(['model','fold'])[['known_pnl_usd','partial_pnl_usd','unknown_attempts']]),'',
        '## Concentration and transfer','',
        'The following sensitivity removes the December Fed-decision event from the monitoring pool and '
        'reranks the remaining events to reallocate the freed slots. It uses the same fitted models. '
        'This is a post-hoc concentration check, not retraining without that event.','',
        markdown_table(comparison(root,scope='exclude_december_fed')[['known_pnl_usd','partial_pnl_usd','unknown_attempts']]),'',
        'The separate family-holdout models remove the evaluated broad topic from training while preserving '
        'chronology. These checks use boosted classifiers, direct regression and two-stage models in both feature views.','',
        markdown_table(full.loc[full.index.str.startswith('family_holdout_'),
            ['known_pnl_usd','partial_pnl_usd','unknown_attempts']]),'',
        'Event contributions for the book-only two-stage boosted model:', '',markdown_table(event_returns(root)), '',
        '## Uncertainty and features','',
        'Paired intervals resample broad topic families using each policy\'s actual replay returns. '
        'They are exploratory 95% percentile intervals, with few independent families and no multiple-comparison '
        'correction. An interval crossing zero does not establish an advantage over the quoted-gap rule.','',
        markdown_table(paired.loc[paired.scope.eq('all_events') & paired.model.isin(PRIMARY)]
            .set_index('model')[['pnl_difference_usd','lower95','upper95']]),'',
        'Book-only boosted feature-removal models are refitted. Their actual replay P&L is:', '',
        markdown_table(full.loc[full.index.str.contains('_without_'),['known_pnl_usd','partial_pnl_usd','unknown_attempts']]),'',
        'Grouped and individual within-scan permutation diagnostics appear in notebook 02. Their score is '
        'the change in selected standalone net-P&L labels, not a rerun of the persistent simulator and not a causal effect. '
        'Gate reliability bins and Brier scores are also retained in diagnostics/.','',
        '## Interpretation limits','',
        '100 ms is an assumed arrival delay, not a measured historical latency. Displayed-depth fills assume '
        'the recorded quotes are attainable. There is no quote-age cutoff, and carry-forward assumes complete '
        'capture of changes within recorded extents. Funding, settlement cash release, operating costs and '
        'a shared portfolio cash budget are not simulated. Maximum order cash and cumulative purchase cash '
        'are exposure and turnover diagnostics, not an investment-return denominator. '
        'Completed baskets are valued at guaranteed payout, so the P&L curve is not a settled cash balance. '
        'Frozen retrospective universe selection and concentrated returns limit generalisation.','',
        'The previous capped and fixed-ranking execution studies remain in results/baseline/ as historical '
        'references. Their targets and quoted-gap scores differ, so dollar changes between studies cannot '
        'be attributed solely to changing the learning objective. All methods in the new tables share the '
        'same rebuilt targets, snapshot features, eligibility, folds and execution policy.','',
        'See [dataset documentation](../dataset.md) and [simulator documentation](../simulator.md).','']
    REPORTS_DIR.mkdir(parents=True,exist_ok=True)
    path=REPORTS_DIR/'current_results.md'
    path.write_text('\n'.join(text),encoding='utf-8')
    return path


def write_profitability_evidence(run_dir,dataset_dir):
    root,folder=Path(run_dir),Path(dataset_dir)
    paths=list((PROJECT_ROOT/'src').rglob('*.py'))+list((PROJECT_ROOT/'tests').glob('*.py'))
    paths+=list((PROJECT_ROOT/'notebooks').glob('*.ipynb'))
    paths+=[PROJECT_ROOT/p for p in ['README.md','docs/dataset.md','docs/simulator.md','docs/reports/current_results.md','requirements.txt']]
    paths+=[p for p in root.rglob('*') if p.is_file() and p.name != 'evidence.json']
    paths+=[folder/name for name in ['examples.parquet','design.json','complete.json']]
    def key(path):
        resolved=path.resolve()
        return resolved.relative_to(PROJECT_ROOT).as_posix() if resolved.is_relative_to(PROJECT_ROOT) else 'DATA_ROOT/'+resolved.relative_to(DATA_DIR).as_posix()
    result=dict(final_lockbox_opened=False,files={key(p):sha256(p) for p in paths})
    (root/'evidence.json').write_text(json.dumps(result,indent=2)+'\n')
    return result
