"""Audits, opportunity-capture tables and figures for the first research layer."""
import argparse
import json
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common import sha256
from src.data.opportunities import load_dataset
from src.models.evaluation import ranked
from src.models.opportunities import choose_candidate
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.reporting.workflow import profitability_dataset


def label(name):
    return (name.replace('tuned_combined_', 'Tuned: ').replace('fixed_combined_', 'Fixed + metadata: ')
        .replace('fixed_book_', 'Book only: ').replace('random_forest','forest')
        .replace('boosted_trees','hist boosting').replace('_',' '))


def verify(root, data):
    root,data=Path(root),Path(data)
    for folder in (data,root):
        complete=json.loads((folder/'complete.json').read_text())
        if complete.get('execution_outcomes_used') is not False or complete.get('final_lockbox_opened') is not False:
            raise ValueError('Expected observation-only development artifacts')
        for path,digest in complete['files_sha256'].items():
            if sha256(folder/path)!=digest:
                raise ValueError(f'Artifact changed: {folder/path}')
        design=json.loads((folder/'design.json').read_text())
        for path,digest in design['source_sha256'].items():
            if sha256(PROJECT_ROOT/path)!=digest:
                raise ValueError(f'Source changed: {path}')
    design=json.loads((root/'design.json').read_text())
    if (design['dataset_sha256']!=sha256(data/'examples.parquet') or
        design['dataset_completion_sha256']!=sha256(data/'complete.json')):
        raise ValueError('Training dataset differs')
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
        part=search.loc[search.fold.eq(row.fold)&search.model.eq(row.model)]
        if len(part)!=16 or part.candidate.nunique()!=8 or choose_candidate(part)!=row.candidate:
            raise ValueError('Tuning differs from the declared recall search')
        if json.loads(row.params)!=json.loads(part.loc[part.candidate.eq(row.candidate),'params'].iloc[0]):
            raise ValueError('Selected parameters differ')
    audit=pd.read_csv(root/'fit_audit.csv')
    if not (pd.to_datetime(audit.train_last,utc=True)+gap < pd.to_datetime(audit.evaluation_start,utc=True)).all():
        raise ValueError('Outer training labels overlap evaluation')
    for row in audit.itertuples():
        fitted=joblib.load(root/'fitted_models'/f'{row.fold}_{row.model}.joblib')
        columns=design['views']['book' if row.model.startswith('fixed_book_') else 'combined']
        if list(fitted.feature_names_in_)!=columns or not np.isin(fitted.classes_,[0,1]).all():
            raise ValueError('Fitted artifact feature schema or objective differs')
    return design


def group_capture(predictions, budgets=(5,3)):
    """Select across the full candidate pool, then attribute capture to groups."""
    rows=[]
    for model,part in predictions.groupby('model'):
        for n in budgets:
            selected=ranked(part,part.score,n)
            for kind in ('family','stratum','event_slug'):
                captured=selected.groupby(kind).target.apply(lambda x:int(x.eq(1).sum()))
                for group,subset in part.groupby(kind):
                    total=int(subset.target.eq(1).sum())
                    hit=int(captured.get(group,0))
                    rows.append(dict(model=model,top_n=n,group_type=kind,group=group,
                        positive_windows=total,captured_positive_windows=hit,
                        recall=hit/total if total else np.nan))
    return pd.DataFrame(rows)


def paired_family_intervals(groups):
    """Exploratory paired family resampling of fixed-policy capture contributions."""
    frame=groups.loc[groups.group_type.eq('family')]
    rows=[]
    for n,part in frame.groupby('top_n'):
        baseline=part.loc[part.model.eq('quoted_gap_rule')].set_index('group').sort_index()
        rng=np.random.default_rng(2026)
        draws=rng.integers(len(baseline),size=(10000,len(baseline)))
        denominator=baseline.positive_windows.to_numpy()
        totals=denominator[draws].sum(axis=1)
        for model,subset in part.groupby('model'):
            subset=subset.set_index('group').reindex(baseline.index)
            delta=subset.captured_positive_windows.to_numpy()-baseline.captured_positive_windows.to_numpy()
            samples=delta[draws].sum(axis=1)[totals>0]/totals[totals>0]
            lo,hi=np.quantile(samples,[.025,.975])
            rows.append(dict(model=model,top_n=n,families=len(baseline),
                recall_difference=delta.sum()/denominator.sum(),lower=float(lo),upper=float(hi),
                draws=len(samples)))
    return pd.DataFrame(rows)


def build_report(root, data):
    root,data=Path(root),Path(data)
    design=verify(root,data)
    frame=load_dataset(data)
    predictions=pd.read_parquet(root/'predictions.parquet')
    metrics=pd.read_csv(root/'metrics.csv')
    comparison=metrics.loc[metrics.fold.eq('all')].copy()
    folder=root/'analysis'
    folder.mkdir(exist_ok=True)
    comparison.to_csv(folder/'comparison.csv',index=False)
    groups=group_capture(predictions)
    groups.to_csv(folder/'group_capture.csv',index=False)
    paired_family_intervals(groups).to_csv(folder/'paired_family_intervals.csv',index=False)
    # Audit only: old execution labels are never passed into opportunity fitting.
    parent=profitability_dataset()
    parent_design=json.loads((data/'design.json').read_text())
    if sha256(parent/'examples.parquet')!=parent_design['parent_examples_sha256']:
        raise ValueError('Original snapshot dataset changed')
    feature_columns=json.loads((data/'design.json').read_text())['feature_columns']
    old=pd.read_parquet(parent/'examples.parquet',columns=['event_slug','time','eligible_snapshot',
        'target','label_known','pnl_target']+feature_columns)
    old=old.loc[old.eligible_snapshot].set_index(['event_slug','time']).sort_index()
    new=frame.set_index(['event_slug','time']).sort_index()
    pd.testing.assert_frame_equal(old[feature_columns],new[feature_columns])
    transition=pd.DataFrame(dict(old_occurrence=old.target,new_opportunity=new.target,
        old_net_pnl=old.pnl_target,old_known=old.label_known,new_known=new.label_known))
    transitions=transition.assign(old_label=transition.old_occurrence.map({0.:'negative',1.:'positive'}).fillna('unknown'),
        new_label=transition.new_opportunity.map({0.:'negative',1.:'positive'}).fillna('unknown'))
    transitions.groupby(['old_label','new_label']).size().rename('rows').reset_index().to_csv(folder/'label_transition.csv',index=False)
    counts=[]
    out=predictions.loc[predictions.model.eq('quoted_gap_rule')]
    for population,subset in [('development',frame),('outer_evaluation',out)]:
        counts.append(dict(population=population,events=subset.event_slug.nunique(),rows=len(subset),
            known=int(subset.label_known.sum()),positive=int(subset.target.eq(1).sum()),
            negative=int(subset.target.eq(0).sum()),unknown=int((~subset.label_known).sum()),
            positive_at_scan=int((subset.target.eq(1)&subset.opportunity_at_scan).sum()),
            observed_new_onset=int(subset.future_positive.sum()),
            new_onset_without_scan=int((subset.future_positive&~subset.opportunity_at_scan).sum()),
            witnessed_unknown=int((subset.witnessed_opportunity&~subset.label_known).sum())))
    pd.DataFrame(counts).to_csv(folder/'label_counts.csv',index=False)
    audit=dict(snapshot_features_identical=True,execution_columns_used_in_fitting=False,
        old_negative_return_hours=int(old.pnl_target.lt(0).sum()),
        old_negative_return_hours_now_opportunity=int((old.pnl_target.lt(0)&new.target.eq(1)).sum()),
        new_opportunity_old_known_no_success=int((new.target.eq(1)&old.target.eq(0)).sum()),
        old_positive_now_negative=int((new.target.eq(0)&old.target.eq(1)).sum()),
        final_lockbox_opened=False)
    (folder/'label_audit.json').write_text(json.dumps(audit,indent=2)+'\n')
    importance=pd.read_csv(root/'feature_importance.csv')
    importance.groupby(['model','group','fold'],as_index=False).agg(
        positive_hours_lost=('positive_hours_lost','mean'),recall_drop=('recall_drop','mean'),
        future_recall_drop=('future_recall_drop','mean')).to_csv(folder/'feature_importance_by_fold.csv',index=False)
    coefficients=[]
    for fold,_,_ in design['folds']:
        model=joblib.load(root/'fitted_models'/f'{fold}_fixed_book_logistic.joblib')
        names=model[0].get_feature_names_out(design['views']['book'])
        weights=model[-1].coef_[0]
        if len(names)!=len(weights):
            raise ValueError('Logistic coefficient names differ')
        coefficients.extend(dict(fold=fold,feature=name,coefficient=float(weight))
                            for name,weight in zip(names,weights,strict=True))
    pd.DataFrame(coefficients).to_csv(folder/'book_logistic_coefficients.csv',index=False)
    changes=[]
    for row in comparison.itertuples():
        reference=(row.model.replace('tuned_combined_','fixed_combined_') if row.model.startswith('tuned_combined_')
                   else row.model.replace('fixed_combined_','fixed_book_') if row.model.startswith('fixed_combined_') else None)
        if reference is None:
            continue
        base=comparison.loc[comparison.scope.eq(row.scope)&comparison.top_n.eq(row.top_n)&comparison.model.eq(reference)].iloc[0]
        changes.append(dict(scope=row.scope,top_n=row.top_n,model=row.model,reference=reference,
            recall_change=row.recall-base.recall,captured_change=row.captured_positive_windows-base.captured_positive_windows,
            future_recall_change=row.future_recall-base.future_recall))
    pd.DataFrame(changes).to_csv(folder/'model_changes.csv',index=False)
    for scope in ('all_events','exclude_december_fed'):
        fig,axes=plt.subplots(1,2,figsize=(14,7),layout='constrained')
        for ax,n in zip(axes,(5,3)):
            part=comparison.loc[comparison.scope.eq(scope)&comparison.top_n.eq(n)]
            part=part.loc[part.model.str.startswith('tuned_')|part.model.isin(
                ['quoted_gap_rule','depth_rule','freshness_rule','fixed_combined_random_forest',
                 'fixed_book_logistic','fixed_book_random_forest'])].sort_values('recall')
            ax.barh([label(k) for k in part.model],100*part.recall,
                color=['#ce8b2c' if k=='quoted_gap_rule' else '#3975a0' for k in part.model])
            expected=float(part.expected_random_captured.iloc[0]/part.positive_windows.iloc[0])
            ax.axvline(100*expected,color='gray',linestyle='--',label='Expected random capture')
            for y,row in enumerate(part.itertuples()):
                ax.text(row.recall*100+.5,y,f'{row.captured_positive_windows}/{row.positive_windows}',va='center',fontsize=8)
            ax.set_xlim(0,111)
            ax.set_xlabel('Known positive event-hours captured (%)')
            ax.set_title(f'Top {n} events')
            ax.legend(loc='lower right',fontsize=8)
            ax.grid(axis='x',alpha=.2)
        fig.suptitle('Opportunity prediction: all events' if scope=='all_events' else 'December Fed excluded; same models, reranked')
        fig.savefig(folder/f'{scope}.png',dpi=150)
        plt.close(fig)
    files={p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name!='manifest.json'}
    (folder/'manifest.json').write_text(json.dumps(dict(training_completion_sha256=sha256(root/'complete.json'),
        dataset_completion_sha256=sha256(data/'complete.json'),report_source_sha256=sha256(Path(__file__)),
        execution_outcomes_used_in_fitting=False,final_lockbox_opened=False,files_sha256=files),indent=2)+'\n')
    return comparison,pd.DataFrame(counts)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,required=True)
    parser.add_argument('--data',type=Path,default=PROCESSED_DIR/'opportunities')
    args=parser.parse_args()
    build_report(args.run,args.data)
