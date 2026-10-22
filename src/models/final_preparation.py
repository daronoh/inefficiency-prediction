"""N=3 chronological retuning and full-development refits; no final-test reads."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
from time import perf_counter
import warnings

import joblib
import numpy as np
import pandas as pd
import sklearn
import tabm
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.config import ScreeningSpec, require_split_access, shortlist_size
from src.data.metadata import add_metadata
from src.data.opportunities import load_dataset
from src.models.estimators import predict as classifier_score
from src.models.evaluation import ranked
from src.models.opportunities import split_masks, validation_windows, fit_classifier, choose_candidate
from src.models.opportunity_value import value_metrics, choose_value_candidate
from src.models.tabm import candidates as tabm_candidates
from src.models.tabm_study import fit as fit_tabm, SEEDS, write_json, verify as verify_tabm
from src.models.tuning import candidates
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.simulator.opportunities import setup

METHODS=('logistic','random_forest','tabm_two_stage')
NEW_POLICIES=('n3_book_logistic','n3_book_random_forest','n3_tabm_two_stage')
REFERENCES=('quoted_gap_rule','fixed_book_logistic','fixed_book_random_forest','tabm_tuned_two_stage')


def training_mask(frame,start,horizon):
    """Labels must be known and finish strictly before the fitting cutoff."""
    return frame.label_known & (frame.time+horizon).lt(start)


def choose(rows,method):
    rows=pd.DataFrame(rows)
    if not rows.top_n.eq(3).all():
        raise ValueError('Final preparation must tune at N=3')
    return choose_value_candidate(rows) if method=='tabm_two_stage' else choose_candidate(rows)


def epoch_counts(rows,candidate):
    part=pd.DataFrame(rows)
    part=part.loc[part.candidate.eq(candidate)]
    if len(part)!=2:
        raise ValueError('Exactly two chronological epoch selections required')
    return {task:max(1,int(np.ceil(part[f'{task}_epochs'].median()))) for task in ('classifier','amount')}


def score_bundle(folder,bundle,frame):
    """Score supplied as-of features only; neither labels nor metadata are loaded.

    The caller supplies the same feature construction used in training. For
    genuine final evaluation, use the final bundle only on times at/after its
    trained-through boundary, not on its own earlier training observations.
    """
    folder=Path(folder)
    if frame.duplicated(['event_slug','time']).any():
        raise ValueError('One row per eligible event and scan is required')
    pieces=[]
    for name,entry in bundle['policies'].items():
        if entry['kind']=='rule':
            score=frame.x_best_quoted_net_usd.fillna(-1e12).to_numpy()
        else:
            x=frame[entry['features']]
            if entry['kind']=='classifier':
                score=classifier_score(joblib.load(folder/entry['artifact']),x)
            else:
                # Average products: mean(P_s * V_s), not mean(P_s) * mean(V_s).
                score=np.mean([joblib.load(folder[pair['gate']]).predict(x)*
                               joblib.load(folder[pair['amount']]).predict(x)
                               for pair in entry['members']],axis=0)
        if not np.isfinite(score).all():
            raise ValueError(f'Nonfinite policy scores: {name}')
        pieces.append(frame[['event_slug','time']].assign(model=name,score=score))
    return pd.concat(pieces,ignore_index=True)


def shortlist(scores,top_n=3):
    shortlist_size(0,top_n)
    return pd.concat([ranked(part,part.score,top_n).drop(columns='_score')
                      for _,part in scores.groupby('model')],ignore_index=True)


def prepare(output,device='cpu'):
    require_split_access('development')
    output=Path(output)
    prior=PROJECT_ROOT/'results/tabm'
    old=verify_tabm(prior)
    data=PROCESSED_DIR/'opportunity_value'
    frame=add_metadata(load_dataset(data))
    if any(c in frame for c in ('pnl_target','activity_target','known_pnl_usd','attempts','partial')):
        raise ValueError('Execution outcomes cannot enter fitting')
    spec=ScreeningSpec(**old['window'])
    end=pd.Timestamp(spec.lockbox_start)
    book,combined=old['feature_views']['book'],old['feature_views']['combined']
    prior_predictions=pd.read_parquet(prior/'predictions.parquet')
    options={name:candidates(name,'classifier') for name in METHODS[:2]}
    options['tabm_two_stage']=tabm_candidates()
    source=['src/models/final_preparation.py']+list(old['source_sha256'])+[
        'src/models/tuning.py','src/models/estimators.py','src/models/features.py',
        'src/config.py','src/data/opportunities.py','src/data/opportunity_value.py',
        'src/models/settings.py','src/simulator/opportunities.py','src/simulator/execution.py']
    execution=asdict(setup(.1,spec.horizon_minutes));execution['freshness_s']=None
    design=dict(version='n3_final_preparation_v1',window=old['window'],folds=old['folds'],
        primary_budget=3,secondary_budget=5,feature_views=old['feature_views'],candidates=options,seeds=list(SEEDS),
        objectives={'logistic':'Binary occurrence fit; pooled positive-hour recall@3, ties mean AP then candidate order',
                    'random_forest':'Binary occurrence fit; pooled positive-hour recall@3, ties mean AP then candidate order',
                    'tabm_two_stage':'Captured peak quote dollars@3, ties positive-hour capture then candidate order'},
        inner_validation='Two earlier chronological blocks per outer origin and independently before final refit; full-window strict purge',
        tabm='32 members, unweighted gate and positive-only amount; same eight configurations; inner log-loss/MSE early stopping; outer/full refits use ceil median inner epoch; mean of three seed products',
        final_refit='Retune afresh on two development-only validation blocks ending before final-test start; fit all known windows ending strictly before that boundary',
        selection='Compare all five fitted policies plus quoted-gap; retain fixed book classifiers regardless of new tuning results. No final winner selected here.',
        final_references={'random_seeds':20,'seed_base':SEEDS[0],'monitor_all':True},
        development_references=list(REFERENCES),replay_policies=list(NEW_POLICIES),
        execution=execution,latency_sensitivity_s=[0.,.05,.1,.25,.5],
        dataset_sha256=sha256(data/'examples.parquet'),dataset_completion_sha256=sha256(data/'complete.json'),
        prior_completion_sha256=sha256(prior/'complete.json'),prior_predictions_sha256=sha256(prior/'predictions.parquet'),
        metadata_sha256=json.loads((PROJECT_ROOT/'results/opportunities/design.json').read_text())['metadata_sha256'],
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in dict.fromkeys(source)},
        packages=dict(torch=torch.__version__,tabm=tabm.__version__,sklearn=sklearn.__version__),device=device,
        primary_budget_chosen_after_development=True,execution_outcomes_used=False,final_lockbox_opened=False,
        interpretation='Development retuning and final-fit preparation only. Repeatedly inspected outer folds are exploratory. No performance estimates from the full-development refits.')
    output.mkdir(parents=True,exist_ok=False)
    for directory in ('outer_models','final_models'):
        (output/directory).mkdir()
    write_json(output/'design.json',design)
    searches,choices,audits,histories,warning_rows,inner_pieces=[],[],[],[],[],[]
    predictions=[prior_predictions.loc[prior_predictions.model.isin(REFERENCES)].copy()]
    origins=[(fold,pd.Timestamp(lo,tz='UTC'),pd.Timestamp(hi,tz='UTC')) for fold,lo,hi in old['folds']]
    origins.append(('final_refit',end,None))
    for origin,start,stop in origins:
        final=stop is None
        train=frame.loc[training_mask(frame,start,spec.horizon)]
        test=None if final else frame.loc[frame.time.ge(start)&frame.time.lt(stop)]
        bundle=dict(origin=origin,trained_before=str(start),window=old['window'],primary_budget=3,secondary_budget=5,
                    final_lockbox_opened=False,policies={'quoted_gap_rule':dict(kind='rule',features=['x_best_quoted_net_usd'])})
        for method in METHODS:
            columns=combined if method=='tabm_two_stage' else book
            local=[]
            for candidate,params in enumerate(options[method]):
                for inner,(lo,hi) in enumerate(validation_windows(frame,start,spec.horizon)):
                    a,b=split_masks(frame,lo,hi,spec.horizon)
                    fitting,valid=frame.loc[a],frame.loc[b]
                    print(f'{origin}: {method} candidate {candidate}, inner {inner}',flush=True)
                    started=perf_counter()
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter('always')
                        if method=='tabm_two_stage':
                            gate=fit_tabm(fitting,valid,'classifier',params,columns,device)
                            amount=fit_tabm(fitting,valid,'amount',params,columns,device)
                            score=gate.predict(valid[columns])*amount.predict(valid[columns])
                            extra=dict(classifier_epochs=gate.best_epoch_,amount_epochs=amount.best_epoch_)
                            for task,model in [('classifier',gate),('amount',amount)]:
                                histories.extend(dict(origin=origin,candidate=candidate,inner=inner,task=task,**r) for r in model.history_)
                        else:
                            model=fit_classifier(fitting,method,params,columns)
                            score=classifier_score(model,valid[columns]);extra={}
                    warning_rows.extend(dict(origin=origin,method=method,candidate=candidate,inner=inner,warning=str(w.message)) for w in caught)
                    metric=value_metrics(valid,score,3)
                    local.append(dict(origin=origin,method=method,candidate=candidate,inner=inner,top_n=3,
                        params=json.dumps(params,sort_keys=True),training_rows=len(fitting),training_positive=int(fitting.target.sum()),
                        train_last=str(fitting.time.max()),validation_start=str(lo),validation_end=str(hi),fit_seconds=perf_counter()-started,
                        **extra,**{k:metric[k] for k in ('positive_windows','captured_positive_windows','recall','average_precision',
                            'captured_peak_value','total_peak_value','peak_value_recall','selected_unknown')}))
                    inner_pieces.append(valid[['event_slug','time','label_known','target','value_target']].assign(
                        origin=origin,method=method,candidate=candidate,inner=inner,score=score))
                pd.DataFrame(searches+local).to_csv(output/'search.csv',index=False)
            best=choose(local,method)
            searches.extend(local)
            epochs=epoch_counts(local,best) if method=='tabm_two_stage' else {}
            choices.append(dict(origin=origin,method=method,candidate=best,top_n=3,
                                params=json.dumps(options[method][best],sort_keys=True),**epochs))
            model_name=f'n3_{"tabm_two_stage" if method=="tabm_two_stage" else "book_"+method}'
            destination=output/('final_models' if final else 'outer_models')

            def save(model,task,candidate,seed,epochs=None):
                filename=f'{origin}_{task}_c{candidate}_s{seed}.joblib'
                relative=(destination/filename).relative_to(output).as_posix()
                joblib.dump(model,output/relative,compress=3)
                subset=train.loc[train.value_target.gt(0)] if task=='tabm_amount' else train
                audits.append(dict(origin=origin,method=method,task=task,candidate=candidate,seed=seed,
                    artifact=relative,training_rows=len(subset),training_positive=int(subset.target.sum()),
                    train_last=str(subset.time.max()),label_end_last=str((subset.time+spec.horizon).max()),
                    fitting_cutoff=str(start),epochs=epochs,features=json.dumps(columns)))
                return relative

            if method=='tabm_two_stage':
                members=[]
                for seed in SEEDS:
                    print(f'{origin}: refit TabM seed {seed}, epochs {epochs}',flush=True)
                    gate=fit_tabm(train,None,'classifier',options[method][best],columns,device,seed,epochs['classifier'])
                    amount=fit_tabm(train,None,'amount',options[method][best],columns,device,seed,epochs['amount'])
                    members.append(dict(seed=seed,gate=save(gate,'tabm_classifier',best,seed,epochs['classifier']),
                        amount=save(amount,'tabm_amount',best,seed,epochs['amount'])))
                bundle['policies'][model_name]=dict(kind='hurdle_ensemble',features=columns,members=members)
            else:
                cache={}
                for prefix,index in [('fixed',0),('n3',best)]:
                    if index not in cache:
                        with warnings.catch_warnings(record=True) as caught:
                            warnings.simplefilter('always')
                            model=fit_classifier(train,method,options[method][index],columns)
                        warning_rows.extend(dict(origin=origin,method=method,candidate=index,inner='refit',warning=str(w.message)) for w in caught)
                        cache[index]=save(model,method,index,SEEDS[0])
                    bundle['policies'][f'{prefix}_book_{method}']=dict(kind='classifier',features=columns,artifact=cache[index])
        write_json(output/f'{origin}_bundle.json',bundle)
        if final:
            # In-sample probe checks serialization only; no training performance is reported.
            probe=train.sample(n=min(32,len(train)),random_state=SEEDS[0])
            probe[['event_slug','time']+combined].to_parquet(output/'refit_probe_features.parquet',index=False)
            score_bundle(output,bundle,probe).to_parquet(output/'refit_probe_scores.parquet',index=False)
        else:
            scores=score_bundle(output,bundle,test)
            template=prior_predictions.loc[prior_predictions.model.eq('quoted_gap_rule')&prior_predictions.fold.eq(origin)].drop(columns=['model','score'])
            # Fixed controls are retained from the frozen run; reproduce them independently.
            for policy in ('fixed_book_logistic','fixed_book_random_forest'):
                current=scores.loc[scores.model.eq(policy)].sort_values(['event_slug','time'])
                previous=prior_predictions.loc[prior_predictions.model.eq(policy)&prior_predictions.fold.eq(origin)].sort_values(['event_slug','time'])
                if not np.allclose(current.score,previous.score,rtol=1e-10,atol=1e-12):
                    raise ValueError('Fixed control predictions changed')
            predictions.append(scores.loc[scores.model.isin(NEW_POLICIES)].merge(template,on=['event_slug','time'],validate='many_to_one'))
            pd.concat(predictions,ignore_index=True).to_parquet(output/'predictions.parquet',index=False)
        pd.DataFrame(choices).to_csv(output/'selected_parameters.csv',index=False)
        pd.DataFrame(audits).to_csv(output/'fit_audit.csv',index=False)
        pd.DataFrame(histories).to_csv(output/'learning_curves.csv',index=False)
        pd.DataFrame(warning_rows,columns=['origin','method','candidate','inner','warning']).to_csv(output/'warnings.csv',index=False)
        pd.concat(inner_pieces,ignore_index=True).to_parquet(output/'inner_predictions.parquet',index=False)
    predicted=pd.concat(predictions,ignore_index=True)
    rows=[]
    for scope,excluded in [('all_events',None),('exclude_december_fed','fed-decision-in-december')]:
        pool=predicted if excluded is None else predicted.loc[predicted.event_slug.ne(excluded)]
        for name,part in pool.groupby('model'):
            for fold,subset in [('all',part)]+list(part.groupby('fold')):
                for n in (3,5):
                    rows.append(dict(scope=scope,model=name,fold=fold,**value_metrics(subset,subset.score,n)))
    pd.DataFrame(rows).to_csv(output/'metrics.csv',index=False)
    files={p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    write_json(output/'complete.json',dict(files_sha256=files,primary_budget=3,
        execution_outcomes_used=False,final_lockbox_opened=False,final_models_fitted=True))


def verify(output):
    output=Path(output)
    design=json.loads((output/'design.json').read_text())
    complete=json.loads((output/'complete.json').read_text())
    if complete['final_lockbox_opened'] or complete['execution_outcomes_used'] or design['primary_budget']!=3:
        raise ValueError('Expected development-only N=3 preparation')
    for p,digest in complete['files_sha256'].items():
        if sha256(output/p)!=digest:
            raise ValueError(f'Artifact changed: {p}')
    for p,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/p)!=digest:
            raise ValueError(f'Source changed: {p}')
    for p,key in [(PROCESSED_DIR/'opportunity_value/examples.parquet','dataset_sha256'),
        (PROCESSED_DIR/'opportunity_value/complete.json','dataset_completion_sha256'),
        (PROJECT_ROOT/'results/tabm/complete.json','prior_completion_sha256'),
        (PROJECT_ROOT/'results/tabm/predictions.parquet','prior_predictions_sha256')]:
        if sha256(p)!=design[key]:
            raise ValueError(f'Input changed: {p}')
    from src.paths import MARKETS_PARQUET, EVENT_UNIVERSE
    if sha256(MARKETS_PARQUET)!=design['metadata_sha256']['markets'] or sha256(EVENT_UNIVERSE)!=design['metadata_sha256']['universe']:
        raise ValueError('Metadata changed')
    spec=ScreeningSpec(**design['window'])
    end=pd.Timestamp(spec.lockbox_start)
    origins={fold:pd.Timestamp(start,tz='UTC') for fold,start,_ in design['folds']}
    origins['final_refit']=end
    search=pd.read_csv(output/'search.csv')
    if not (pd.to_datetime(search.train_last,utc=True)+spec.horizon<pd.to_datetime(search.validation_start,utc=True)).all():
        raise ValueError('Inner training overlap')
    if not (pd.to_datetime(search.validation_end,utc=True)+spec.horizon<=search.origin.map(origins)).all():
        raise ValueError('Validation reaches evaluation origin')
    inner=pd.read_parquet(output/'inner_predictions.parquet')
    # Independently reconstruct selection metrics from saved eligible validation scores.
    for row in search.itertuples():
        part=inner.loc[inner.origin.eq(row.origin)&inner.method.eq(row.method)&inner.candidate.eq(row.candidate)&inner.inner.eq(row.inner)]
        selected=ranked(part,part.score,3)
        if len(part)==0 or int(selected.target.eq(1).sum())!=row.captured_positive_windows:
            raise ValueError('Inner recall does not reproduce at N=3')
        if not np.isclose(selected.value_target.sum(),row.captured_peak_value):
            raise ValueError('Inner captured value does not reproduce at N=3')
    for row in pd.read_csv(output/'selected_parameters.csv').itertuples():
        part=search.loc[search.origin.eq(row.origin)&search.method.eq(row.method)]
        if len(part)!=16 or choose(part,row.method)!=row.candidate or json.loads(row.params)!=design['candidates'][row.method][row.candidate]:
            raise ValueError('Selection differs from declared N=3 objective')
    frame=add_metadata(load_dataset(PROCESSED_DIR/'opportunity_value'))
    audit=pd.read_csv(output/'fit_audit.csv')
    for row in audit.itertuples():
        training=frame.loc[training_mask(frame,origins[row.origin],spec.horizon)]
        if row.task=='tabm_amount':
            training=training.loc[training.value_target.gt(0)]
        if row.training_rows!=len(training) or pd.Timestamp(row.label_end_last)>=origins[row.origin]:
            raise ValueError('Invalid fitted population or cutoff')
        model=joblib.load(output/row.artifact)
        if list(model.feature_names_in_)!=json.loads(row.features):
            raise ValueError('Fitted feature schema differs')
        params=design['candidates'][row.method][row.candidate]
        if row.task.startswith('tabm_'):
            task='amount' if row.task=='tabm_amount' else 'classifier'
            part=search.loc[search.origin.eq(row.origin)&search.method.eq(row.method)]
            if row.epochs!=epoch_counts(part,row.candidate)[task] or model.history_ or model.training_rows_!=len(training):
                raise ValueError('TabM final epoch/population audit failed')
            if model.params!=params or model.k!=32 or model.seed!=row.seed:
                raise ValueError('TabM configuration differs')
            if task=='amount' and not np.isclose(model.target_mean_,np.asarray(training.value_target,dtype=np.float32).mean()):
                raise ValueError('Amount model used wrong target')
        else:
            if any(model[-1].get_params()[key]!=value for key,value in params.items()):
                raise ValueError('Classical model settings differ')
    bundle=json.loads((output/'final_refit_bundle.json').read_text())
    probe=pd.read_parquet(output/'refit_probe_features.parquet')
    expected=pd.read_parquet(output/'refit_probe_scores.parquet')
    actual=score_bundle(output,bundle,probe)
    pd.testing.assert_frame_equal(actual,expected,check_exact=False,rtol=1e-6,atol=1e-6)
    predicted=pd.read_parquet(output/'predictions.parquet')
    if predicted.duplicated(['model','event_slug','time']).any() or not (predicted.time+spec.horizon).le(end).all():
        raise ValueError('Invalid development predictions')
    reference=predicted.loc[predicted.model.eq('quoted_gap_rule')].drop(columns=['model','score']).sort_values(['event_slug','time']).reset_index(drop=True)
    for name,part in predicted.groupby('model'):
        pd.testing.assert_frame_equal(part.drop(columns=['model','score']).sort_values(['event_slug','time']).reset_index(drop=True),reference)
        if not np.isfinite(part.score).all():
            raise ValueError(f'Nonfinite scores: {name}')
    return design


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--device',choices=['cpu','cuda'],default='cpu')
    args=parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        prepare(args.output,args.device)
