"""Compare likelihood and quoted-value rankings without treating quotes as P&L."""
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
from src.models.opportunity_value import choose_value_candidate
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.reporting.opportunities import verify as verify_occurrence, group_capture, paired_family_intervals


def label(name):
    return (name.replace('tuned_value_regression_','Value: ').replace('tuned_value_two_stage_','Two-stage: ')
        .replace('fixed_value_regression_','Fixed value: ').replace('fixed_value_two_stage_','Fixed two-stage: ')
        .replace('tuned_combined_','Occurrence: ').replace('fixed_combined_','Fixed occurrence: ')
        .replace('fixed_book_','Book occurrence: ').replace('random_forest','forest')
        .replace('boosted_trees','hist boosting').replace('_',' '))


def verify(root,data,occurrence,observation_data):
    root,data,occurrence,observation_data=map(Path,(root,data,occurrence,observation_data))
    verify_occurrence(occurrence,observation_data)
    for folder in (root,data):
        complete=json.loads((folder/'complete.json').read_text())
        if complete.get('execution_outcomes_used') is not False or complete.get('final_lockbox_opened') is not False:
            raise ValueError('Expected observation-only development results')
        for path,digest in complete['files_sha256'].items():
            if sha256(folder/path)!=digest:
                raise ValueError(f'Artifact changed: {folder/path}')
        design=json.loads((folder/'design.json').read_text())
        for path,digest in design['source_sha256'].items():
            if sha256(PROJECT_ROOT/path)!=digest:
                raise ValueError(f'Source changed: {path}')
    design=json.loads((root/'design.json').read_text())
    if (design['dataset_sha256']!=sha256(data/'examples.parquet') or
        design['dataset_completion_sha256']!=sha256(data/'complete.json') or
        design['occurrence_completion_sha256']!=sha256(occurrence/'complete.json')):
        raise ValueError('Training inputs differ')
    if json.loads((data/'design.json').read_text())['observation_dataset_completion_sha256']!=sha256(observation_data/'complete.json'):
        raise ValueError('Value labels refer to different occurrence labels')
    search=pd.read_csv(root/'search.csv')
    selected=pd.read_csv(root/'selected_parameters.csv')
    gap=pd.Timedelta(seconds=design['embargo_seconds'])
    for col in ('train_last','validation_start','validation_end'):
        search[col]=pd.to_datetime(search[col],format='mixed',utc=True)
    outer={fold:pd.Timestamp(start,tz='UTC') for fold,start,_ in design['folds']}
    if not (search.train_last+gap<search.validation_start).all() or not (search.validation_end+gap<=search.fold.map(outer)).all():
        raise ValueError('Invalid inner temporal split')
    for row in selected.itertuples():
        part=search.loc[search.fold.eq(row.fold)&search.model.eq(row.model)]
        if len(part)!=16 or choose_value_candidate(part)!=row.candidate:
            raise ValueError('Invalid value-based parameter selection')
        if json.loads(row.params)!=json.loads(part.loc[part.candidate.eq(row.candidate),'params'].iloc[0]):
            raise ValueError('Selected settings differ')
    audits=pd.read_csv(root/'fit_audit.csv')
    if not (pd.to_datetime(audits.train_last,utc=True)+gap<pd.to_datetime(audits.evaluation_start,utc=True)).all():
        raise ValueError('Invalid outer split')
    for row in audits.itertuples():
        model=joblib.load(root/'fitted_models'/f'{row.fold}_{row.model}.joblib')
        if list(model.feature_names_in_)!=design['feature_columns']:
            raise ValueError('Fitted feature schema differs')
    old=pd.read_parquet(occurrence/'predictions.parquet').sort_values(['model','event_slug','time']).reset_index(drop=True)
    new=pd.read_parquet(root/'predictions.parquet')
    matched=new.loc[new.model.isin(old.model.unique()),old.columns].sort_values(['model','event_slug','time']).reset_index(drop=True)
    pd.testing.assert_frame_equal(old,matched)
    return design


def build_report(root,data,occurrence,observation_data):
    root,data,occurrence,observation_data=map(Path,(root,data,occurrence,observation_data))
    verify(root,data,occurrence,observation_data)
    frame=load_dataset(data)
    prediction=pd.read_parquet(root/'predictions.parquet')
    metrics=pd.read_csv(root/'metrics.csv')
    headline=metrics.loc[metrics.fold.eq('all')].copy()
    folder=root/'analysis'
    folder.mkdir(exist_ok=True)
    headline.to_csv(folder/'comparison.csv',index=False)
    groups=group_capture(prediction)
    groups.to_csv(folder/'group_capture.csv',index=False)
    paired_family_intervals(groups).to_csv(folder/'paired_occurrence_intervals.csv',index=False)
    rows=[]
    novel_rows=[]
    for model,part in prediction.groupby('model'):
        for n in (5,3):
            chosen=ranked(part,part.score,n)
            novel=part.loc[part.label_known & ~part.opportunity_at_scan & part.future_positive]
            caught=chosen.loc[chosen.label_known & ~chosen.opportunity_at_scan & chosen.future_positive]
            total_new=float(novel.value_target.sum())
            captured_new=float(caught.value_target.sum())
            novel_rows.append(dict(model=model,top_n=n,positive_windows=len(novel),captured_positive_windows=len(caught),
                total_peak_value=total_new,captured_peak_value=captured_new,
                peak_value_recall=captured_new/total_new if total_new else np.nan))
            for kind in ('family','stratum','event_slug'):
                captured=chosen.groupby(kind).value_target.sum()
                for group,subset in part.groupby(kind):
                    total=float(subset.value_target.sum())
                    hit=float(captured.get(group,0))
                    rows.append(dict(model=model,top_n=n,group_type=kind,group=group,
                        total_peak_value=total,captured_peak_value=hit,value_recall=hit/total if total else np.nan))
    contributions=pd.DataFrame(rows)
    pd.DataFrame(novel_rows).to_csv(folder/'new_opportunity_value.csv',index=False)
    contributions.to_csv(folder/'value_contributions.csv',index=False)
    paired=[]
    families=contributions.loc[contributions.group_type.eq('family')]
    for n,part in families.groupby('top_n'):
        base=part.loc[part.model.eq('quoted_gap_rule')].set_index('group').sort_index()
        draws=np.random.default_rng(2026).integers(len(base),size=(10000,len(base)))
        totals=base.total_peak_value.to_numpy()[draws].sum(axis=1)
        for model,subset in part.groupby('model'):
            subset=subset.set_index('group').reindex(base.index)
            delta=subset.captured_peak_value.to_numpy()-base.captured_peak_value.to_numpy()
            samples=delta[draws].sum(axis=1)[totals>0]/totals[totals>0]
            lo,hi=np.quantile(samples,[.025,.975])
            paired.append(dict(model=model,top_n=n,families=len(base),value_recall_difference=delta.sum()/base.total_peak_value.sum(),
                lower=lo,upper=hi,draws=len(samples)))
    pd.DataFrame(paired).to_csv(folder/'paired_value_intervals.csv',index=False)
    distribution=[]
    outer=prediction.loc[prediction.model.eq('quoted_gap_rule')]
    for population,subset in [('development',frame),('outer_evaluation',outer)]:
        known=subset.loc[subset.label_known]
        positive=known.loc[known.value_target.gt(0),'value_target']
        distribution.append(dict(population=population,known=len(known),positive=len(positive),
            median_positive_peak=float(positive.median()),p90_positive_peak=float(positive.quantile(.9)),
            largest_peak=float(positive.max()),sum_hourly_peaks=float(positive.sum()),
            december_fed_share=float(known.loc[known.event_slug.eq('fed-decision-in-december'),'value_target'].sum()/positive.sum())))
    pd.DataFrame(distribution).to_csv(folder/'value_distribution.csv',index=False)
    importance=pd.read_csv(root/'feature_importance.csv')
    importance.groupby(['model','group','fold'],as_index=False).agg(peak_value_lost=('peak_value_lost','mean'),
        value_recall_drop=('value_recall_drop','mean'),recall_drop=('recall_drop','mean')).to_csv(folder/'feature_importance_by_fold.csv',index=False)
    # All model scores rank the same universe; only the fitting objective differs.
    for scope in ('all_events','exclude_december_fed'):
        fig,axes=plt.subplots(1,2,figsize=(15,9),layout='constrained')
        for ax,n in zip(axes,(5,3)):
            part=headline.loc[headline.scope.eq(scope)&headline.top_n.eq(n)]
            part=part.loc[part.model.str.startswith('tuned_')|part.model.isin(['quoted_gap_rule',
                'fixed_book_logistic','fixed_book_random_forest','fixed_combined_random_forest',
                'fixed_value_two_stage_random_forest'])].sort_values('peak_value_recall')
            ax.barh([label(k) for k in part.model],100*part.peak_value_recall,
                color=['#ce8b2c' if k=='quoted_gap_rule' else '#3975a0' for k in part.model])
            expected=part.expected_random_peak_value.iloc[0]/part.total_peak_value.iloc[0]
            ax.axvline(100*expected,color='gray',linestyle='--',label='Expected random capture')
            for y,value in enumerate(part.peak_value_recall):
                ax.text(100*value+.5,y,f'{100*value:.1f}%',va='center',fontsize=8)
            ax.set_xlim(0,111)
            ax.set_xlabel('Share of summed hourly peak quote values captured (%)')
            ax.set_title(f'Top {n} events')
            ax.legend(loc='lower right',fontsize=8)
            ax.grid(axis='x',alpha=.2)
        fig.suptitle('Quoted opportunity value; this is not P&L' if scope=='all_events' else 'December Fed excluded; same models, reranked')
        fig.savefig(folder/f'{scope}.png',dpi=150)
        plt.close(fig)
    files={p.name:sha256(p) for p in folder.iterdir() if p.is_file() and p.name!='manifest.json'}
    (folder/'manifest.json').write_text(json.dumps(dict(training_completion_sha256=sha256(root/'complete.json'),
        report_source_sha256=sha256(Path(__file__)),final_lockbox_opened=False,files_sha256=files),indent=2)+'\n')
    return headline,pd.DataFrame(distribution)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,required=True)
    parser.add_argument('--data',type=Path,default=PROCESSED_DIR/'opportunity_value')
    parser.add_argument('--occurrence',type=Path,default=PROJECT_ROOT/'results'/'opportunities')
    parser.add_argument('--observation-data',type=Path,default=PROCESSED_DIR/'opportunities')
    args=parser.parse_args()
    build_report(args.run,args.data,args.occurrence,args.observation_data)
