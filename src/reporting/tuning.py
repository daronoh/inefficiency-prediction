"""Verified tables and uncluttered figures for the nested tuning study."""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import joblib
import numpy as np
import pandas as pd

from src.common import sha256


def label(name):
    return (name.replace('tuned_classifier_', 'Tuned occurrence: ')
        .replace('tuned_regression_', 'Tuned P&L: ').replace('tuned_two_stage_', 'Tuned two-stage: ')
        .replace('fixed_classifier_', 'Fixed occurrence: ').replace('fixed_regression_', 'Fixed P&L: ')
        .replace('fixed_two_stage_', 'Fixed two-stage: ').replace('combined_', 'Original: ')
        .replace('random_forest','forest').replace('boosted_trees','hist boosting').replace('_',' '))


def verify_training(root):
    root = Path(root)
    completion = json.loads((root/'complete.json').read_text())
    if completion.get('final_lockbox_opened'):
        raise ValueError('Final test must remain closed')
    for path, digest in completion['files_sha256'].items():
        if sha256(root/path) != digest:
            raise ValueError(f'Tuning artifact changed: {path}')
    design=json.loads((root/'design.json').read_text())
    gap=pd.Timedelta(seconds=design['embargo_seconds'])
    search=pd.read_csv(root/'search.csv')
    choices=pd.read_csv(root/'selected_parameters.csv')
    outer_start={fold:pd.Timestamp(start,tz='UTC') for fold,start,_ in design['folds']}
    for col in ('train_last','validation_start','validation_end'):
        search[col]=pd.to_datetime(search[col],format='mixed',utc=True)
    if not (search.train_last+gap < search.validation_start).all():
        raise ValueError('Inner training labels overlap validation')
    if not (search.validation_end+gap <= search.fold.map(outer_start)).all():
        raise ValueError('Inner validation labels enter outer evaluation')
    for row in choices.itertuples():
        rows=search.loc[search.fold.eq(row.fold)&search.model.eq(row.model)]
        scores=rows.groupby('candidate').agg(pnl=('selected_standalone_pnl_usd','sum'),hours=('hours','sum'))
        winner=int((scores.pnl/scores.hours).sort_values(ascending=False,kind='stable').index[0])
        if row.candidate != winner or len(rows) != 16 or scores.shape[0] != 8:
            raise ValueError('Candidate selection differs from the declared search')
        chosen=json.loads(rows.loc[rows.candidate.eq(winner),'params'].iloc[0])
        if json.loads(row.params) != chosen:
            raise ValueError('Selected settings differ from the evaluated candidate')
    return completion


def original_name(key):
    if not key.startswith('tuned_'):
        return None
    kind, name = next((k,key[len('tuned_'+k+'_'):]) for k in ('classifier','regression','two_stage')
                     if key.startswith('tuned_'+k+'_'))
    if name == 'xgboost':
        return f'fixed_{kind}_xgboost'
    return 'combined_'+{'classifier':'','regression':'reg_','two_stage':'two_stage_'}[kind]+name


def build_report(root):
    root = Path(root)
    completion = verify_training(root)
    design = json.loads((root/'design.json').read_text())
    metrics = pd.read_csv(root/'metrics.csv')
    results, paired, manifests = [], [], {}
    for budget in (5,3):
        folder = root/f'replay_top{budget}'
        manifest = json.loads((folder/'run.json').read_text())
        if (manifest['predictions_sha256'] != completion['predictions_sha256']
            or manifest['examples_sha256'] != design['dataset_sha256']
            or manifest['execution'] != design['dataset_design']['execution']
            or manifest['top_n'] != budget or manifest['final_lockbox_opened']):
            raise ValueError('Replay inputs or execution assumptions differ')
        table = pd.read_csv(folder/'summary.csv')
        table['net_pnl_usd'] = table.known_pnl_usd
        table['budget'] = budget
        results.append(table)
        paired.append(pd.read_csv(folder/'paired_comparisons.csv').assign(budget=budget))
        manifests[str(budget)] = {name:sha256(folder/name) for name in
                                 ('run.json','summary.csv','attempts.parquet','paired_comparisons.csv')}
    all_results = pd.concat(results, ignore_index=True)
    headline = all_results.loc[all_results.fold.eq('all')].copy()
    headline = headline.merge(metrics[['model','top_n','recall','future_recall','selected_unknown',
        'average_precision']], left_on=['model','budget'],right_on=['model','top_n'],how='left',suffixes=('','_metric'))
    headline.loc[headline.scope.ne('all_events'), ['recall','future_recall','selected_unknown','average_precision']] = np.nan
    changes = []
    for row in headline.itertuples():
        original = original_name(row.model)
        if original is None:
            continue
        base = headline.loc[headline.scope.eq(row.scope)&headline.budget.eq(row.budget)&headline.model.eq(original)]
        if len(base) != 1:
            raise ValueError(f'Missing matched comparator for {row.model}')
        changes.append(dict(scope=row.scope,budget=row.budget,model=row.model,reference=original,
            net_pnl_usd=row.net_pnl_usd,reference_pnl_usd=base.iloc[0].net_pnl_usd,
            difference_usd=row.net_pnl_usd-base.iloc[0].net_pnl_usd))
    folder = root/'analysis'
    folder.mkdir(exist_ok=True)
    headline.to_csv(folder/'comparison.csv',index=False)
    all_results.loc[all_results.fold.ne('all')].to_csv(folder/'fold_results.csv',index=False)
    pd.DataFrame(changes).to_csv(folder/'tuning_changes.csv',index=False)
    pd.concat(paired,ignore_index=True).to_csv(folder/'paired_comparisons.csv',index=False)
    importance = pd.read_csv(root/'feature_importance.csv')
    grouped = importance.groupby(['model','group','fold'],as_index=False).agg(
        pnl_drop=('selected_standalone_pnl_drop_usd','mean'),recall_drop=('recall_drop','mean'))
    grouped.to_csv(folder/'feature_importance_by_fold.csv',index=False)
    search = pd.read_csv(root/'search.csv')
    search.groupby(['fold','model','candidate'],as_index=False).agg(
        proxy_pnl=('selected_standalone_pnl_usd','sum'),validation_scans=('hours','sum')).to_csv(folder/'search_totals.csv',index=False)
    # Logistic coefficients are on imputed, signed-log, standardised inputs.
    coefficients=[]
    for fold, _, _ in design['folds']:
        model=joblib.load(root/'fitted_models'/f'{fold}_tuned_classifier_logistic.joblib')
        preprocessing=model[0]
        numeric=preprocessing.named_transformers_['numeric']
        numeric_columns=preprocessing.transformers_[0][2]
        names=list(numeric[0].get_feature_names_out(numeric_columns))
        category_columns=preprocessing.transformers_[1][2]
        names+=list(preprocessing.named_transformers_['categorical'].get_feature_names_out(category_columns))
        weights=model[-1].coef_[0]
        if len(names) != len(weights):
            raise ValueError('Logistic coefficient names differ from fitted columns')
        coefficients.extend(dict(fold=fold,feature=name,coefficient=float(weight))
                            for name,weight in zip(names,weights))
    pd.DataFrame(coefficients).to_csv(folder/'logistic_coefficients.csv',index=False)
    for scope in ('all_events','exclude_december_fed'):
        fig, axes = plt.subplots(1,2,figsize=(15,9),layout='constrained')
        for ax,budget in zip(axes,(5,3)):
            subset = headline.loc[headline.scope.eq(scope)&headline.budget.eq(budget)]
            subset = subset.loc[subset.model.str.startswith('tuned_')|subset.model.isin(
                ['quoted_gap_rule','combined_random_forest','monitor_all'])].sort_values('net_pnl_usd')
            colors = ['#d18326' if n == 'quoted_gap_rule' else '#777777' if n in
                      ('monitor_all','combined_random_forest') else '#286c9c' for n in subset.model]
            ax.barh([label(n) for n in subset.model],subset.net_pnl_usd,color=colors)
            ax.set_title(f'Top {budget} monitored events')
            ax.set_xlabel('Net simulated P&L (USD)')
            ax.grid(axis='x',alpha=.2)
            ax.margins(x=.18)
            for y,value in enumerate(subset.net_pnl_usd):
                ax.text(value,y,f' ${value:,.0f}',va='center',fontsize=8)
        fig.suptitle('All events' if scope == 'all_events' else 'December Fed excluded; policies reranked and replayed')
        fig.savefig(folder/f'{scope}.png',dpi=150)
        plt.close(fig)
    # Event totals expose concentration without equating them with event-exclusion replay.
    event_tables=[]
    for budget in (5,3):
        attempts=pd.read_parquet(root/f'replay_top{budget}'/'attempts.parquet')
        taken=attempts.loc[attempts.attempted & attempts.scope.eq('all_events')].copy()
        taken['net']=taken.pnl_usd-design['dataset_design']['contract']['assumed_other_cost_usd']
        event_tables.append(taken.groupby(['model','event_slug'],as_index=False).agg(
            known_net_pnl_usd=('net','sum'),unknown_attempts=('net',lambda x:int(x.isna().sum())),
            attempts=('net','size')).assign(budget=budget))
    pd.concat(event_tables,ignore_index=True).to_csv(folder/'event_contributions.csv',index=False)
    sources = [Path(__file__),root/'complete.json']
    files = {p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name != 'manifest.json'}
    manifest = dict(final_lockbox_opened=False,training_completion_sha256=sha256(root/'complete.json'),
        report_source_sha256=sha256(sources[0]),replays=manifests,files_sha256=files)
    (folder/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return headline, pd.DataFrame(changes)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',required=True,type=Path)
    args=parser.parse_args()
    build_report(args.run)
