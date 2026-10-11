"""Chronological dollar-value prediction, activity hurdles and matched baselines."""
from dataclasses import replace
import json
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, brier_score_loss
from src.common import sha256
from src.config import ScreeningSpec
from src.data.profitability import load_profitability
from src.data.metadata import add_metadata, METADATA_COLUMNS, METADATA_NUMERIC, METADATA_CATEGORICAL
from src.models.estimators import estimator, predict
from src.models.features import family, feature_groups
from src.models.evaluation import masks, ranked, ranking_metrics, safe_ap
from src.models.settings import MODEL_NAMES, FOLDS, SEED, BUDGETS, method_name
from src.paths import MARKETS_PARQUET, EVENT_UNIVERSE

REGRESSORS = ['ridge','random_forest','boosted_trees']
VIEWS = ('book','combined')


def regressor(name, numeric=None, categorical=None, conditional=False):
    if name == 'ridge':
        model = Ridge(alpha=10.)
    elif name == 'random_forest':
        model = RandomForestRegressor(n_estimators=200,max_depth=8,
            min_samples_leaf=5 if conditional else 20,n_jobs=4,random_state=SEED)
    elif name == 'boosted_trees':
        model = HistGradientBoostingRegressor(max_iter=120,learning_rate=.05,max_leaf_nodes=15,
            min_samples_leaf=10 if conditional else 30,l2_regularization=10.,early_stopping=False,random_state=SEED)
    else:
        raise ValueError(name)
    preprocessing = estimator('logistic',np.array([0,1]),numeric=numeric,categorical=categorical)
    return Pipeline(preprocessing.steps[:-1]+[('regression',model)])


class ActivityPnLModel(RegressorMixin,BaseEstimator):
    """P(attempt | snapshot) times E(signed hourly P&L | attempt, snapshot).

    Gates fit the observed class prevalence, with no class weighting or random
    oversampling. Conditional regressors retain zero and negative trading hours.
    No hard threshold discards an event before ranking.
    """
    def __init__(self,name='boosted_trees',numeric=None,categorical=None):
        self.name,self.numeric,self.categorical=name,numeric,categorical

    def fit(self,x,y,activity):
        activity = np.asarray(activity,dtype=int)
        y = np.asarray(y,dtype=float)
        if not np.isfinite(y).all() or not np.isin(activity,[0,1]).all():
            raise ValueError('Hurdle fitting requires known dollar and binary activity targets')
        if np.any((activity == 0) & (np.abs(y) > 1e-10)):
            raise ValueError('No attempted trade must imply zero P&L')
        name = 'logistic' if self.name == 'ridge' else self.name
        self.gate_ = estimator(name,activity,numeric=self.numeric,categorical=self.categorical)
        if len(np.unique(activity)) > 1:
            self.gate_[-1].set_params(class_weight=None)
        self.gate_.fit(x,activity)
        active = activity.astype(bool)
        self.active_rows_ = int(active.sum())
        if active.any():
            self.amount_ = regressor(self.name,self.numeric,self.categorical,conditional=True).fit(x.loc[active],y[active])
        else:
            self.amount_ = DummyRegressor(strategy='constant',constant=0.).fit(np.zeros((len(x),1)),y)
        return self

    def components(self,x):
        probability = predict(self.gate_,x)
        amount = self.amount_.predict(x if self.active_rows_ else np.zeros((len(x),1)))
        return probability,amount

    def predict(self,x):
        probability,amount = self.components(x)
        return probability*amount


def model_score(model,x,kind):
    return predict(model,x) if kind == 'classifier' else model.predict(x)


def selected_value(frame,score,n=5):
    chosen = ranked(frame,score,n)
    return float(chosen.loc[chosen.label_known,'pnl_target'].sum())


def fit_profitability(output,dataset_dir):
    output,dataset_dir = Path(output),Path(dataset_dir)
    design = json.loads((dataset_dir/'design.json').read_text())
    spec = replace(ScreeningSpec(**design['contract']),arrival_delay_s=design['execution']['delay_s'])
    frame = add_metadata(load_profitability(dataset_dir))
    frame['family'] = frame.event_slug.map(family)
    columns = [c for c in frame if c.startswith('x_')]
    views = {'book':columns,'combined':columns+METADATA_COLUMNS}
    groups = feature_groups(columns)
    output.mkdir(parents=True,exist_ok=False)
    (output/'fitted_models').mkdir()
    (output/'diagnostics').mkdir()
    contract = dict(dataset_sha256=sha256(dataset_dir/'examples.parquet'),dataset_design=design,
        metadata_sha256={'markets':sha256(MARKETS_PARQUET),'universe':sha256(EVENT_UNIVERSE)},
        folds=FOLDS,primary_budget=5,budgets=BUDGETS,seed=SEED,views=views,
        embargo_seconds=spec.horizon.total_seconds()+spec.arrival_delay_s+spec.unwind_delay_s,
        primary_metric='Persistent chronological replay net P&L; standalone selected label sums are diagnostics only',
        gate='Unweighted activity classifier; probabilities audited out of sample, not assumed calibrated',
        amount='Signed net P&L on all known attempted hours, including losses and zeros',
        ensembles='Classifiers: mean percentile rank. Regressors and hurdles: mean predicted dollars.',
        family_holdout='Classifier, direct and two-stage boosted trees in both views; evaluated family excluded from training',
        final_lockbox_opened=False,source_sha256=sha256(__file__))
    (output/'design.json').write_text(json.dumps(contract,indent=2)+'\n')
    predictions,fit_audit,importance,gate_rows = [],[],[],[]

    def fit_one(train,kind,name,view,selected):
        numeric = None if view == 'book' else [c for c in selected if c not in METADATA_CATEGORICAL]
        categorical = None if view == 'book' else METADATA_CATEGORICAL
        x = train[selected]
        if kind == 'classifier':
            model = estimator(name,train.target.astype(int),numeric,categorical).fit(x,train.target.astype(int))
        elif kind == 'regression':
            model = regressor(name,numeric,categorical).fit(x,train.pnl_target)
        else:
            model = ActivityPnLModel(name,numeric,categorical).fit(x,train.pnl_target,train.activity_target)
        return model

    for fold,start,end in FOLDS:
        training,evaluation = masks(frame,start,end,spec)
        train,test = frame.loc[training].copy(),frame.loc[evaluation].copy()
        if train.empty or test.empty:
            raise ValueError('Empty profitability fold')
        print(f'Profit models {fold}: {len(train)} known training hours, {int(train.activity_target.sum())} trading hours',flush=True)
        scores,kinds,models = {},{},{}
        def remember(key,score,kind):
            if not np.isfinite(score).all():
                raise ValueError(f'Nonfinite prediction: {key}')
            scores[key],kinds[key] = np.asarray(score),kind
        for view,selected in views.items():
            for kind,names,prefix in [('classifier',MODEL_NAMES,''),('regression',REGRESSORS,'reg_'),('two_stage',REGRESSORS,'two_stage_')]:
                for name in names:
                    key = method_name(prefix+name,view)
                    model = fit_one(train,kind,name,view,selected)
                    models[key]=(model,kind,selected)
                    remember(key,model_score(model,test[selected],kind),kind)
                    joblib.dump(model,output/'fitted_models'/f'{fold}_{key}.joblib',compress=3)
                    fit_audit.append(dict(fold=fold,model=key,training_rows=len(train),
                        trading_rows=int(train.activity_target.sum()),loss_hours=int(train.pnl_target.lt(0).sum()),
                        train_last_time=str(train.time.max()),test_first_time=str(test.time.min())))
                    if kind == 'two_stage':
                        probability,amount = model.components(test[selected])
                        known = test.label_known.to_numpy()
                        gate_rows.append(pd.DataFrame(dict(fold=fold,model=key,event_slug=test.event_slug.to_numpy(),
                            time=test.time.to_numpy(),label_known=known,activity_target=test.activity_target.to_numpy(),
                            probability=probability,conditional_pnl_usd=amount)))
                keys=[method_name(prefix+name,view) for name in names]
                if kind == 'classifier':
                    combined=np.mean([pd.Series(scores[key],index=test.index).groupby(test.time).rank(pct=True) for key in keys],axis=0)
                    key=method_name('rank_ensemble',view)
                else:
                    combined=np.mean([scores[key] for key in keys],axis=0)
                    key=method_name(prefix+'ensemble',view)
                remember(key,combined,kind)
        # Transfer checks are specified for one representative nonlinear family.
        for view,selected in views.items():
            for kind,prefix in [('classifier',''),('regression','reg_'),('two_stage','two_stage_')]:
                values=np.full(len(test),np.nan)
                key='family_holdout_'+method_name(prefix+'boosted_trees',view)
                for topic in sorted(test.family.unique()):
                    subset=train.loc[train.family.ne(topic)]
                    where=test.family.eq(topic).to_numpy()
                    if subset.empty:
                        raise ValueError('Empty family holdout training set')
                    model=fit_one(subset,kind,'boosted_trees',view,selected)
                    values[where]=model_score(model,test.loc[where,selected],kind)
                remember(key,values,kind)
        for group in ('history','quoted_gap','freshness_quality'):
            kept=[c for c in columns if c not in groups[group]]
            for kind,prefix in [('regression','reg_'),('two_stage','two_stage_')]:
                key=prefix+'without_'+group
                model=fit_one(train,kind,'boosted_trees','book',kept)
                remember(key,model_score(model,test[kept],kind),kind)
        for key,column,sign,missing in [('quoted_gap_rule','x_best_quoted_net_usd',1,-1e12),
            ('depth_rule','x_ask_depth_sum',1,-1e12),('freshness_rule','x_book_age_max_s',-1,1e12)]:
            remember(key,sign*test[column].fillna(missing).to_numpy(),'rule')
        for seed in range(20):
            remember(f'random_{seed:02d}',np.random.default_rng(SEED+seed+FOLDS.index((fold,start,end))*100).random(len(test)),'random')
        positions=list(test.groupby('time',sort=False).indices.values())
        for view,selected in views.items():
            units={f'group:{name}':members for name,members in groups.items()}
            units.update({f'feature:{c}':[c] for c in columns if not c.startswith(('x_snapshot_lag1_','x_snapshot_change1_'))})
            if view == 'combined':
                units['group:metadata']=METADATA_COLUMNS
                units.update({f'feature:{c}':[c] for c in METADATA_COLUMNS})
            for prefix in ('reg_','two_stage_'):
                key=method_name(prefix+'boosted_trees',view)
                model,kind,_=models[key]
                base_value=selected_value(test,scores[key])
                for unit,members in units.items():
                    for repeat in range(3):
                        rng=np.random.default_rng(SEED+repeat)
                        order=np.arange(len(test))
                        for pos in positions:
                            order[pos]=rng.permutation(pos)
                        shuffled=test[selected].copy()
                        shuffled.loc[:,members]=test[members].iloc[order].to_numpy()
                        altered=model_score(model,shuffled,kind)
                        importance.append(dict(fold=fold,model=key,unit=unit,repeat=repeat,
                            selected_standalone_pnl_drop_usd=base_value-selected_value(test,altered)))
        for key,score in scores.items():
            part=test[['event_slug','family','stratum','time','label_known','target','pnl_target','activity_target',
                'future_positive','coverage_fraction','x_active_tokens','known_pnl_usd']].copy()
            part['model'],part['fold'],part['score'],part['kind']=key,fold,score,kinds[key]
            predictions.append(part)
        pd.concat(predictions,ignore_index=True).to_parquet(output/'predictions.parquet',index=False)
        pd.DataFrame(fit_audit).to_csv(output/'diagnostics/fit_audit.csv',index=False)
        pd.DataFrame(importance).to_csv(output/'diagnostics/permutation_importance.csv',index=False)
        pd.concat(gate_rows,ignore_index=True).to_parquet(output/'diagnostics/gate_predictions.parquet',index=False)
    combined=pd.concat(predictions,ignore_index=True)
    metrics=[]
    for model,part in combined.groupby('model'):
        known=part.loc[part.label_known]
        dollars=part.kind.iloc[0] in ('regression','two_stage')
        for n in BUDGETS:
            metrics.append(dict(model=model,kind=part.kind.iloc[0],**ranking_metrics(part,part.score,n),
                average_precision=safe_ap(known.target,known.score),
                selected_standalone_pnl_usd=selected_value(part,part.score,n),
                mae_usd=mean_absolute_error(known.pnl_target,known.score) if dollars else np.nan,
                rmse_usd=np.sqrt(mean_squared_error(known.pnl_target,known.score)) if dollars else np.nan,
                zero_prediction_mae_usd=float(known.pnl_target.abs().mean()),
                zero_prediction_rmse_usd=float(np.sqrt(np.mean(known.pnl_target**2)))))
    pd.DataFrame(metrics).to_csv(output/'metrics.csv',index=False)
    gates=pd.concat(gate_rows,ignore_index=True)
    reliability=[]
    for (model,fold),part in gates.loc[gates.label_known].groupby(['model','fold']):
        part=part.assign(bin=pd.cut(part.probability,[-.0001,.001,.005,.01,.02,.05,.1,.2,.5,1.0001]))
        brier=brier_score_loss(part.activity_target,part.probability)
        for interval,rows in part.groupby('bin',observed=True):
            reliability.append(dict(model=model,fold=fold,probability_bin=str(interval),rows=len(rows),
                mean_probability=rows.probability.mean(),observed_activity=rows.activity_target.mean(),fold_brier=brier))
    pd.DataFrame(reliability).to_csv(output/'diagnostics/gate_reliability.csv',index=False)
    (output/'complete.json').write_text(json.dumps(dict(predictions_sha256=sha256(output/'predictions.parquet'),
        models=combined.model.nunique(),final_lockbox_opened=False),indent=2)+'\n')
    return pd.DataFrame(metrics)
