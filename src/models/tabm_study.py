"""Bounded, nested chronological TabM extension of the frozen Dome study."""
import argparse
import json
from pathlib import Path
import platform
from time import perf_counter

import joblib
import numpy as np
import pandas as pd
import sklearn
import tabm
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.config import ScreeningSpec, require_split_access
from src.data.metadata import add_metadata
from src.data.opportunities import load_dataset
from src.models.features import feature_groups
from src.models.opportunities import split_masks, validation_windows, choose_candidate
from src.models.opportunity_value import value_metrics, choose_value_candidate
from src.models.settings import SEED
from src.models.tabm import TabMEstimator, TabMHurdle, candidates
from src.paths import PROJECT_ROOT, PROCESSED_DIR

SEEDS=(SEED,SEED+1,SEED+2)
REPLAY_POLICIES=('tabm_fixed_book_classifier','tabm_fixed_classifier','tabm_tuned_classifier',
    'tabm_fixed_regression','tabm_tuned_regression','tabm_fixed_two_stage','tabm_tuned_two_stage',
    'tabm_forest_rank_ensemble')


def write_json(path,value):
    path.write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')


def target(frame,task):
    return frame.target if task=='classifier' else frame.value_target


def fit(train,valid,task,params,columns,device,seed=SEED,epochs=None):
    if not train.label_known.all():
        raise ValueError('Unknown labels may not enter fitting')
    if task=='amount':
        train=train.loc[train.value_target.gt(0)]
        valid=valid.loc[valid.value_target.gt(0)] if valid is not None else None
    if train.empty:
        raise ValueError('No positive training examples for conditional amount')
    model=TabMEstimator('classifier' if task=='classifier' else 'regression',params,seed=seed,device=device)
    known=valid.loc[valid.label_known] if valid is not None else None
    # A validation block with no positive amounts cannot choose an amount epoch.
    # Use the predeclared 16-epoch fallback; never consult outer labels.
    if known is not None and known.empty:
        return model.fit(train[columns],target(train,task),epochs=16)
    validation=(known[columns],target(known,task)) if known is not None else None
    return model.fit(train[columns],target(train,task),validation=validation,epochs=epochs)


def median_epochs(search,fold,view,candidate,task):
    part=pd.DataFrame(search)
    part=part.loc[part.fold.eq(fold)&part.view.eq(view)&part.candidate.eq(candidate)]
    values=part.loc[part.method.eq('classifier' if task=='classifier' else
                    'regression' if task=='regression' else 'two_stage'),f'{task}_epochs']
    return max(1,int(np.ceil(np.median(values))))


def train_study(output,device='cpu',data=PROCESSED_DIR/'opportunity_value',
                prior=PROJECT_ROOT/'results/opportunity_value'):
    from src.reporting.opportunity_value import verify as verify_prior
    require_split_access('development')
    output,data,prior=map(Path,(output,data,prior))
    occurrence=PROJECT_ROOT/'results/opportunities'
    observation=PROCESSED_DIR/'opportunities'
    original=verify_prior(prior,data,occurrence,observation)
    frame=add_metadata(load_dataset(data))
    if any(c in frame for c in ('pnl_target','activity_target','known_pnl_usd','attempts','partial')):
        raise ValueError('Execution outcomes must not enter this study')
    book=[c for c in original['feature_columns'] if c.startswith('x_')]
    columns=original['feature_columns']
    spec=ScreeningSpec(**original['window'])
    old=pd.read_parquet(prior/'predictions.parquet')
    paper=next((PROJECT_ROOT/'papers').glob('*tabm*Paper-Conference.pdf'))
    source=['src/models/tabm.py','src/models/tabm_study.py','src/models/opportunities.py',
            'src/models/opportunity_value.py','src/models/evaluation.py','src/data/metadata.py']
    design=dict(version='tabm_opportunity_v1',window=original['window'],folds=original['folds'],
        feature_views={'book':book,'combined':columns},seeds=list(SEEDS),candidates=candidates(),
        paper=paper.relative_to(PROJECT_ROOT).as_posix(),paper_sha256=sha256(paper),
        official_implementation='https://github.com/yandex-research/tabm',
        architecture='TabM (not mini), 32 independently supervised shared-weight members, no numeric embeddings',
        preprocessing='Train-only median imputation and normal quantiles for continuous columns; binary imputation without scaling; one-hot categories; unknown categories all zero',
        target_normalization='Train-only mean/std for dollar MSE; invert before clipping aggregate to zero; no log transform',
        loss='Unweighted memberwise binary cross entropy or MSE; AdamW; gradient norm clip 1; no schedule',
        early_stopping='Inner known validation ensemble log loss / standardized MSE; patience 16, maximum 100 epochs. No outer early stopping. Refit full earlier training history for ceil(median best inner epoch).',
        conditional='Positive-only amount model with separate preprocessing; unweighted probability gate; product of predictions. Empty-positive inner validation uses fixed 16 epochs.',
        tuning='Eight combined-feature settings, two purged chronological inner blocks; occurrence chooses recall@5 then AP, value chooses captured peak dollars@5 then positive-hour capture',
        fixed='Candidate 0, seed 0; epochs still chosen inside training history. Book-only candidate 0 has its own inner epoch selection.',
        ensembles='Tuned policies average three seed predictions, none selected by outer performance. Hybrid averages within-scan percentile ranks of tuned TabM classifier and frozen combined forest.',
        primary_budget=5,budgets=[5,3],replay_policies=list(REPLAY_POLICIES),
        device=device,hardware=torch.cuda.get_device_name(0) if device=='cuda' else platform.processor(),
        packages=dict(python=platform.python_version(),torch=torch.__version__,tabm=tabm.__version__,sklearn=sklearn.__version__),
        dataset_sha256=sha256(data/'examples.parquet'),dataset_completion_sha256=sha256(data/'complete.json'),
        prior_completion_sha256=sha256(prior/'complete.json'),prior_predictions_sha256=sha256(prior/'predictions.parquet'),
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in source},
        execution_outcomes_used=False,final_lockbox_opened=False,
        interpretation='Additional exploratory development comparison after prior results; not independent confirmation or an exact paper benchmark reproduction')
    output.mkdir(parents=True,exist_ok=False)
    (output/'fitted_models').mkdir()
    write_json(output/'design.json',design)
    searches,selected,audits,histories,importance=[],[],[],[],[]
    pieces=[old]
    for fold,start,end in design['folds']:
        start,end=pd.Timestamp(start,tz='UTC'),pd.Timestamp(end,tz='UTC')
        a,b=split_masks(frame,start,end,spec.horizon)
        train,test=frame.loc[a],frame.loc[b]
        template=old.loc[old.fold.eq(fold)&old.model.eq('quoted_gap_rule')].drop(columns=['model','score'])
        template=template.set_index(['event_slug','time']).reindex(pd.MultiIndex.from_frame(test[['event_slug','time']])).reset_index()
        if template.fold.isna().any():
            raise ValueError('TabM and prior evaluation populations differ')
        for view,cols,options in [('combined',columns,list(enumerate(candidates()))),('book',book,[(0,candidates()[0])])]:
            for candidate,params in options:
                for inner,(lo,hi) in enumerate(validation_windows(frame,start,spec.horizon)):
                    x,y=split_masks(frame,lo,hi,spec.horizon)
                    fitting,valid=frame.loc[x],frame.loc[y]
                    print(f'{fold} TabM {view} candidate {candidate} inner {inner}: {len(fitting)} training rows',flush=True)
                    fitted={}
                    for task in (('classifier','regression','amount') if view=='combined' else ('classifier',)):
                        fitted[task]=fit(fitting,valid,task,params,cols,device)
                        histories.extend(dict(fold=fold,view=view,candidate=candidate,inner=inner,task=task,**r)
                                         for r in fitted[task].history_)
                    scores={'classifier':fitted['classifier'].predict(valid[cols])}
                    if view=='combined':
                        scores['regression']=fitted['regression'].predict(valid[cols])
                        scores['two_stage']=scores['classifier']*fitted['amount'].predict(valid[cols])
                    for method,score in scores.items():
                        metric=value_metrics(valid,score,5)
                        searches.append(dict(fold=fold,view=view,candidate=candidate,inner=inner,method=method,
                            params=json.dumps(params,sort_keys=True),training_rows=len(fitting),training_positive=int(fitting.target.sum()),
                            train_last=str(fitting.time.max()),validation_start=str(lo),validation_end=str(hi),
                            **{f'{task}_epochs':model.best_epoch_ for task,model in fitted.items()},
                            **{f'{task}_seconds':model.fit_seconds_ for task,model in fitted.items()},
                            **{k:metric[k] for k in ('positive_windows','captured_positive_windows','recall','average_precision',
                                'captured_peak_value','total_peak_value','peak_value_recall')}))
                pd.DataFrame(searches).to_csv(output/'search.csv',index=False)
                pd.DataFrame(histories).to_csv(output/'learning_curves.csv',index=False)
        search=pd.DataFrame(searches)
        best={}
        for method in ('classifier','regression','two_stage'):
            local=search.loc[search.fold.eq(fold)&search.view.eq('combined')&search.method.eq(method)]
            best[method]=(choose_candidate(local) if method=='classifier' else choose_value_candidate(local))
            selected.append(dict(fold=fold,method=method,candidate=best[method],params=json.dumps(candidates()[best[method]],sort_keys=True)))
        pd.DataFrame(selected).to_csv(output/'selected_parameters.csv',index=False)
        scores,cache={},{}

        def outer(task,candidate,seed,view='combined'):
            key=(task,candidate,seed,view)
            if key not in cache:
                cols=book if view=='book' else columns
                epochs=median_epochs(searches,fold,view,candidate,task)
                print(f'{fold} outer TabM {key}, {epochs} epochs',flush=True)
                model=fit(train,None,task,candidates()[candidate],cols,device,seed,epochs)
                path=f'{fold}_{view}_{task}_c{candidate}_s{seed}.joblib'
                joblib.dump(model,output/'fitted_models'/path,compress=3)
                audits.append(dict(fold=fold,view=view,task=task,candidate=candidate,seed=seed,artifact=path,
                    training_rows=model.training_rows_,training_positive=int(train.target.sum()),
                    train_last=str(train.time.max()),evaluation_start=str(start),evaluation_rows=len(test),
                    epochs=epochs,fit_seconds=model.fit_seconds_,parameters=model.parameter_count_,
                    transformed_features=model.input_width_))
                cache[key]=model
            return cache[key]

        book_model=outer('classifier',0,SEED,'book')
        scores['tabm_fixed_book_classifier']=book_model.predict(test[book])
        for method in ('classifier','regression','two_stage'):
            for prefix,candidate,seeds in [('fixed',0,[SEED]),('tuned',best[method],SEEDS)]:
                predictions=[]
                for seed in seeds:
                    model=(TabMHurdle(outer('classifier',candidate,seed),outer('amount',candidate,seed))
                           if method=='two_stage' else outer(method,candidate,seed))
                    started=perf_counter()
                    score=model.predict(test[columns])
                    seconds=perf_counter()-started
                    if not np.isfinite(score).all():
                        raise ValueError('Nonfinite TabM prediction')
                    predictions.append(score)
                    if prefix=='tuned':
                        scores[f'tabm_{method}_seed{seed-SEED}']=score
                    # Warm CPU batch of one scan, including preprocessing.
                    scan=test.loc[test.time.eq(test.time.iloc[0]),columns]
                    model.predict(scan)
                    timing=[]
                    for _ in range(5):
                        t=perf_counter(); model.predict(scan); timing.append(perf_counter()-t)
                    audits.append(dict(fold=fold,view='combined',task=method,candidate=candidate,seed=seed,
                        policy=f'tabm_{prefix}_{method}',prediction_seconds=seconds,
                        scan_rows=len(scan),scan_median_ms=1000*float(np.median(timing))))
                scores[f'tabm_{prefix}_{method}']=np.mean(predictions,axis=0)
        forest=old.loc[old.fold.eq(fold)&old.model.eq('fixed_combined_random_forest')]
        forest=forest.set_index(['event_slug','time']).reindex(pd.MultiIndex.from_frame(test[['event_slug','time']])).score.to_numpy()
        scores['tabm_forest_rank_ensemble']=np.mean([
            pd.Series(s,index=test.index).groupby(test.time).rank(pct=True).to_numpy()
            for s in (scores['tabm_tuned_classifier'],forest)],axis=0)
        # Group permutation for the tuned three-seed occurrence ensemble.
        groups=feature_groups(book)
        groups['metadata']=[c for c in columns if c.startswith('m_')]
        base=value_metrics(test,scores['tabm_tuned_classifier'],5)
        positions=list(test.groupby('time',sort=False).indices.values())
        for group,members in groups.items():
            for repeat in range(3):
                rng=np.random.default_rng(SEED+repeat)
                order=np.arange(len(test))
                for pos in positions:
                    order[pos]=rng.permutation(pos)
                shuffled=test[columns].copy()
                shuffled.loc[:,members]=test[members].iloc[order].to_numpy()
                altered=np.mean([outer('classifier',best['classifier'],seed).predict(shuffled) for seed in SEEDS],axis=0)
                metric=value_metrics(test,altered,5)
                importance.append(dict(fold=fold,group=group,repeat=repeat,model='tabm_tuned_classifier',
                    recall_drop=base['recall']-metric['recall'],value_recall_drop=base['peak_value_recall']-metric['peak_value_recall']))
        pieces.extend(template.assign(model=name,score=score) for name,score in scores.items())
        pd.DataFrame(audits).to_csv(output/'fit_audit.csv',index=False)
        pd.DataFrame(importance).to_csv(output/'feature_importance.csv',index=False)
        pd.concat(pieces,ignore_index=True).to_parquet(output/'predictions.parquet',index=False)
    prediction=pd.concat(pieces,ignore_index=True)
    if prediction.duplicated(['model','event_slug','time']).any():
        raise ValueError('Duplicate predictions')
    rows=[]
    for scope,excluded in [('all_events',None),('exclude_december_fed','fed-decision-in-december')]:
        pool=prediction if excluded is None else prediction.loc[prediction.event_slug.ne(excluded)]
        for name,part in pool.groupby('model'):
            for fold,subset in [('all',part)]+list(part.groupby('fold')):
                for n in (5,3):
                    rows.append(dict(scope=scope,model=name,fold=fold,**value_metrics(subset,subset.score,n)))
    pd.DataFrame(rows).to_csv(output/'metrics.csv',index=False)
    files={p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    write_json(output/'complete.json',dict(files_sha256=files,execution_outcomes_used=False,
        final_lockbox_opened=False,models=prediction.model.nunique()))


def verify(output,data=PROCESSED_DIR/'opportunity_value',prior=PROJECT_ROOT/'results/opportunity_value'):
    output,data,prior=map(Path,(output,data,prior))
    design=json.loads((output/'design.json').read_text())
    complete=json.loads((output/'complete.json').read_text())
    if complete['execution_outcomes_used'] or complete['final_lockbox_opened']:
        raise ValueError('Expected observation-only development study')
    for p,digest in complete['files_sha256'].items():
        if sha256(output/p)!=digest:
            raise ValueError(f'Artifact changed: {p}')
    for p,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/p)!=digest:
            raise ValueError(f'Source changed: {p}')
    for path,key in [(data/'examples.parquet','dataset_sha256'),(data/'complete.json','dataset_completion_sha256'),
        (prior/'complete.json','prior_completion_sha256'),(prior/'predictions.parquet','prior_predictions_sha256'),
        (PROJECT_ROOT/design['paper'],'paper_sha256')]:
        if sha256(path)!=design[key]:
            raise ValueError(f'Input changed: {path}')
    search=pd.read_csv(output/'search.csv')
    gap=pd.Timedelta(minutes=design['window']['horizon_minutes'])
    outer={fold:pd.Timestamp(start,tz='UTC') for fold,start,_ in design['folds']}
    if not (pd.to_datetime(search.train_last,utc=True)+gap<pd.to_datetime(search.validation_start,utc=True)).all():
        raise ValueError('Training window overlaps inner validation')
    if not (pd.to_datetime(search.validation_end,utc=True)+gap<=search.fold.map(outer)).all():
        raise ValueError('Inner validation reaches outer evaluation')
    for row in pd.read_csv(output/'selected_parameters.csv').itertuples():
        part=search.loc[search.fold.eq(row.fold)&search.view.eq('combined')&search.method.eq(row.method)]
        choice=choose_candidate(part) if row.method=='classifier' else choose_value_candidate(part)
        if len(part)!=16 or row.candidate!=choice or json.loads(row.params)!=design['candidates'][choice]:
            raise ValueError('Invalid inner parameter selection')
    audit=pd.read_csv(output/'fit_audit.csv').dropna(subset=['artifact'])
    if not (pd.to_datetime(audit.train_last,utc=True)+gap<pd.to_datetime(audit.evaluation_start,utc=True)).all():
        raise ValueError('Outer training window overlap')
    for row in audit.itertuples():
        model=joblib.load(output/'fitted_models'/row.artifact)
        if list(model.feature_names_in_)!=design['feature_views'][row.view]:
            raise ValueError('Saved model feature mismatch')
        if row.epochs!=median_epochs(search,row.fold,row.view,row.candidate,row.task):
            raise ValueError('Outer epoch count did not come from inner training')
    new=pd.read_parquet(output/'predictions.parquet')
    old=pd.read_parquet(prior/'predictions.parquet')
    keys=['model','event_slug','time']
    pd.testing.assert_frame_equal(old.sort_values(keys).reset_index(drop=True),
        new.loc[new.model.isin(old.model.unique()),old.columns].sort_values(keys).reset_index(drop=True))
    reference=old.loc[old.model.eq('quoted_gap_rule')].sort_values(['event_slug','time']).reset_index(drop=True)
    for name,part in new.loc[new.model.str.startswith('tabm_')].groupby('model'):
        part=part.sort_values(['event_slug','time']).reset_index(drop=True)
        pd.testing.assert_frame_equal(part.drop(columns=['model','score']),reference.drop(columns=['model','score']))
        if not np.isfinite(part.score).all():
            raise ValueError(f'Nonfinite scores: {name}')
    end=pd.Timestamp(design['window']['lockbox_start'])
    if not (new.time+gap).le(end).all():
        raise ValueError('Final test reached')
    return design


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--device',choices=['cpu','cuda'],default='cpu')
    args=parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        train_study(args.output,args.device)
