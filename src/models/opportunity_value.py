"""Predict displayed opportunity magnitude, never simulated trading returns."""
import argparse
import json
from pathlib import Path
import warnings

import joblib
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.dummy import DummyRegressor
from sklearn.metrics import mean_absolute_error, brier_score_loss
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.config import ScreeningSpec
from src.data.metadata import add_metadata, METADATA_CATEGORICAL
from src.data.opportunities import load_dataset
from src.models.estimators import predict
from src.models.opportunities import split_masks, validation_windows, opportunity_metrics
from src.models.evaluation import ranked
from src.models.features import feature_groups
from src.models.settings import SEED
from src.models.tuning import candidates, make_model
from src.paths import PROCESSED_DIR, PROJECT_ROOT
from src.reporting.opportunities import verify


class OpportunityValueModel(BaseEstimator):
    """Unweighted P(opportunity) times conditional nonnegative peak quote value."""
    def __init__(self,name,params,numeric,categorical):
        self.name,self.params,self.numeric,self.categorical=name,params,numeric,categorical

    def fit(self,x,y):
        y=np.asarray(y,dtype=float)
        if not np.isfinite(y).all() or (y<0).any():
            raise ValueError('Known nonnegative quote targets required')
        positive=y>0
        self.feature_names_in_=np.asarray(x.columns,dtype=object)
        self.gate_=make_model(self.name,'classifier',self.params,positive.astype(int),
            self.numeric,self.categorical,gate=True).fit(x,positive.astype(int))
        self.positive_rows_=int(positive.sum())
        self.amount_=(make_model(self.name,'regression',self.params,y[positive],self.numeric,self.categorical)
            .fit(x.loc[positive],y[positive]) if positive.any()
            else DummyRegressor(strategy='constant',constant=0.).fit(np.zeros((len(x),1)),y))
        return self

    def components(self,x):
        probability=predict(self.gate_,x)
        value=np.maximum(0.,self.amount_.predict(x if self.positive_rows_ else np.zeros((len(x),1))))
        return probability,value

    def predict(self,x):
        probability,value=self.components(x)
        return probability*value


def fit_value(train,name,kind,params,columns):
    if not train.label_known.all() or not np.isfinite(train.value_target).all() or (train.value_target<0).any():
        raise ValueError('Value fits require known nonnegative opportunity targets')
    numeric=[c for c in columns if c not in METADATA_CATEGORICAL]
    categorical=[c for c in columns if c in METADATA_CATEGORICAL]
    model=(OpportunityValueModel(name,params,numeric,categorical) if kind=='two_stage'
           else make_model(name,'regression',params,train.value_target,numeric,categorical))
    return model.fit(train[columns],train.value_target)


def value_metrics(frame,score,n):
    result=opportunity_metrics(frame,score,n)
    chosen=ranked(frame,score,n)
    total=float(frame.loc[frame.label_known,'value_target'].sum())
    captured=float(chosen.loc[chosen.label_known,'value_target'].sum())
    count=frame.groupby('time').size()
    chance=frame.time.map(count).map(lambda size:min(n,size)/size)
    random=float((frame.value_target.fillna(0)*chance).sum())
    result.update(total_peak_value=total,captured_peak_value=captured,
        peak_value_recall=captured/total if total else np.nan,
        expected_random_peak_value=random,peak_value_lift_vs_random=captured/random if random else np.nan)
    return result


def choose_value_candidate(rows):
    table=pd.DataFrame(rows).groupby('candidate',sort=True).agg(
        captured=('captured_peak_value','sum'),total=('total_peak_value','sum'),
        positive_captured=('captured_positive_windows','sum'))
    if not np.allclose(table.total,table.total.iloc[0]):
        raise ValueError('Value candidates must share the validation population')
    return int(table.sort_values(['captured','positive_captured'],ascending=False,kind='stable').index[0])


def train_study(output,data,occurrence,observation_data=None):
    output,data,occurrence=Path(output),Path(data),Path(occurrence)
    observation_data=Path(observation_data) if observation_data is not None else PROCESSED_DIR/'opportunities'
    original_design=verify(occurrence,observation_data)
    frame=add_metadata(load_dataset(data))
    value_design=json.loads((data/'design.json').read_text())
    if value_design['observation_dataset_completion_sha256']!=sha256(observation_data/'complete.json'):
        raise ValueError('Value and occurrence labels must share the same observation dataset')
    if any(c in frame for c in ('pnl_target','activity_target','known_pnl_usd')):
        raise ValueError('Execution outcomes cannot enter value prediction')
    if not frame.value_target.gt(0).equals(frame.target.eq(1)):
        raise ValueError('Value target must agree with opportunity occurrence')
    columns=original_design['views']['combined']
    spec=ScreeningSpec(**original_design['window'])
    names=('random_forest','boosted_trees','xgboost')
    methods=[(kind,name) for kind in ('regression','two_stage') for name in names]
    design=dict(version='quoted_value_prediction_v1',dataset_sha256=sha256(data/'examples.parquet'),
        dataset_completion_sha256=sha256(data/'complete.json'),
        occurrence_completion_sha256=sha256(occurrence/'complete.json'),
        window=original_design['window'],folds=original_design['folds'],feature_columns=columns,
        candidates={name:candidates(name,'regression') for name in names},primary_budget=5,budgets=[5,3],
        target='Largest qualifying quoted basket dollar margin within the next window; no executed returns',
        tuning_objective='Sum of captured known peak quote values at N=5; ties captured positive hours then first declared setting',
        embargo_seconds=spec.horizon.total_seconds(),gate='Unweighted probability of a positive opportunity; no hard gate threshold',
        conditional='Fit positive quote magnitudes only; clip negative amount predictions to zero',
        aggregation='A sum of per-hour maxima for ranking evaluation, not cumulative tradable profit',
        execution_outcomes_used=False,final_lockbox_opened=False,
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in ['src/models/opportunity_value.py',
            'src/models/opportunities.py','src/models/tuning.py','src/models/estimators.py','src/models/profitability.py']})
    output.mkdir(parents=True,exist_ok=False)
    (output/'fitted_models').mkdir()
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    prior=pd.read_parquet(occurrence/'predictions.parquet')
    values=frame[['event_slug','time','value_target']]
    prior=prior.merge(values,on=['event_slug','time'],validate='many_to_one')
    pieces=[prior]
    searches,selected,audits,warning_rows,errors,importance=[],[],[],[],[],[]
    groups=feature_groups([c for c in columns if c.startswith('x_')])
    groups['metadata']=[c for c in columns if c.startswith('m_')]
    for fold,start,end in design['folds']:
        start,end=pd.Timestamp(start,tz='UTC'),pd.Timestamp(end,tz='UTC')
        a,b=split_masks(frame,start,end,spec.horizon)
        train,test=frame.loc[a],frame.loc[b]
        template=prior.loc[prior.fold.eq(fold)&prior.model.eq('quoted_gap_rule')].drop(columns=['model','score'])
        template=template.set_index(['event_slug','time']).reindex(pd.MultiIndex.from_frame(test[['event_slug','time']])).reset_index()
        if template.fold.isna().any():
            raise ValueError('Value and occurrence evaluation populations differ')
        scores={}
        for kind,name in methods:
            print(f'{fold}: quoted value {kind} {name}',flush=True)
            local=[]
            options=candidates(name,'regression')
            for candidate,params in enumerate(options):
                for inner,(lo,hi) in enumerate(validation_windows(frame,start,spec.horizon)):
                    x,y=split_masks(frame,lo,hi,spec.horizon)
                    fitting,valid=frame.loc[x],frame.loc[y]
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter('always')
                        model=fit_value(fitting,name,kind,params,columns)
                    warning_rows.extend(dict(fold=fold,model=f'{kind}_{name}',candidate=candidate,inner=inner,warning=str(w.message)) for w in caught)
                    score=np.maximum(0.,model.predict(valid[columns]))
                    metric=value_metrics(valid,score,5)
                    local.append(dict(fold=fold,model=f'{kind}_{name}',candidate=candidate,inner=inner,
                        params=json.dumps(params,sort_keys=True),training_rows=len(fitting),training_positive=int(fitting.target.sum()),
                        train_last=str(fitting.time.max()),validation_start=str(lo),validation_end=str(hi),
                        **{k:metric[k] for k in ('captured_peak_value','total_peak_value','captured_positive_windows','peak_value_recall')}))
            best=choose_value_candidate(local)
            searches.extend(local)
            selected.append(dict(fold=fold,model=f'{kind}_{name}',candidate=best,params=json.dumps(options[best],sort_keys=True)))
            for prefix,index in [('fixed',0),('tuned',best)]:
                key=f'{prefix}_value_{kind}_{name}'
                model=fit_value(train,name,kind,options[index],columns)
                score=np.maximum(0.,model.predict(test[columns]))
                if not np.isfinite(score).all():
                    raise ValueError('Nonfinite scores')
                scores[key]=score
                joblib.dump(model,output/'fitted_models'/f'{fold}_{key}.joblib',compress=3)
                audits.append(dict(fold=fold,model=key,training_rows=len(train),training_positive=int(train.target.sum()),
                    train_last=str(train.time.max()),evaluation_start=str(start),evaluation_rows=len(test)))
                known=test.label_known.to_numpy()
                error=dict(fold=fold,model=key,mae=float(mean_absolute_error(test.loc[known,'value_target'],score[known])))
                if kind=='two_stage':
                    probability,_=model.components(test[columns])
                    error['gate_brier']=brier_score_loss(test.loc[known,'target'],probability[known])
                errors.append(error)
                if prefix=='tuned':
                    baseline=value_metrics(test,score,5)
                    positions=list(test.groupby('time',sort=False).indices.values())
                    for group,members in groups.items():
                        for repeat in range(3):
                            rng=np.random.default_rng(SEED+repeat)
                            order=np.arange(len(test))
                            for pos in positions:
                                order[pos]=rng.permutation(pos)
                            shuffled=test[columns].copy()
                            shuffled.loc[:,members]=test[members].iloc[order].to_numpy()
                            altered=value_metrics(test,np.maximum(0.,model.predict(shuffled)),5)
                            importance.append(dict(fold=fold,model=key,group=group,repeat=repeat,
                                peak_value_lost=baseline['captured_peak_value']-altered['captured_peak_value'],
                                value_recall_drop=baseline['peak_value_recall']-altered['peak_value_recall'],
                                recall_drop=baseline['recall']-altered['recall']))
            pd.DataFrame(searches).to_csv(output/'search.csv',index=False)
            pd.DataFrame(selected).to_csv(output/'selected_parameters.csv',index=False)
            pd.DataFrame(audits).to_csv(output/'fit_audit.csv',index=False)
            pd.DataFrame(errors).to_csv(output/'prediction_errors.csv',index=False)
            pd.DataFrame(importance).to_csv(output/'feature_importance.csv',index=False)
            pd.DataFrame(warning_rows,columns=['fold','model','candidate','inner','warning']).to_csv(output/'warnings.csv',index=False)
        for prefix in ('fixed','tuned'):
            scores[f'{prefix}_value_ensemble']=np.mean([v for k,v in scores.items() if k.startswith(prefix+'_value_')],axis=0)
        for key,score in scores.items():
            pieces.append(template.assign(model=key,score=score))
    predictions=pd.concat(pieces,ignore_index=True)
    if predictions.duplicated(['model','event_slug','time']).any():
        raise ValueError('Duplicate predictions')
    predictions.to_parquet(output/'predictions.parquet',index=False)
    rows=[]
    for scope,excluded in [('all_events',None),('exclude_december_fed','fed-decision-in-december')]:
        pool=predictions if excluded is None else predictions.loc[predictions.event_slug.ne(excluded)]
        for model,part in pool.groupby('model'):
            for fold,subset in [('all',part)]+list(part.groupby('fold')):
                for n in (5,3):
                    rows.append(dict(scope=scope,model=model,fold=fold,**value_metrics(subset,subset.score,n)))
    pd.DataFrame(rows).to_csv(output/'metrics.csv',index=False)
    files={p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(predictions_sha256=sha256(output/'predictions.parquet'),
        files_sha256=files,execution_outcomes_used=False,final_lockbox_opened=False),indent=2)+'\n')


def main():
    from src.models.opportunity_value import train_study as run
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--data',type=Path,default=PROCESSED_DIR/'opportunity_value')
    parser.add_argument('--occurrence',type=Path,default=PROJECT_ROOT/'results'/'opportunities')
    parser.add_argument('--observation-data',type=Path,default=PROCESSED_DIR/'opportunities')
    args=parser.parse_args()
    with threadpool_limits(limits=4):
        run(args.output,args.data,args.occurrence,args.observation_data)


if __name__=='__main__':
    main()
