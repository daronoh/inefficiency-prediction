"""Opportunity classification and recall-based chronological tuning, without P&L."""
import argparse
import json
from pathlib import Path
import platform
import warnings

import joblib
import numpy as np
import pandas as pd
import sklearn
import xgboost
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.config import ScreeningSpec, require_split_access
from src.data.metadata import add_metadata, METADATA_COLUMNS, METADATA_CATEGORICAL
from src.data.opportunities import load_dataset
from src.models.estimators import predict
from src.models.evaluation import ranked, ranking_metrics, safe_ap
from src.models.features import family, feature_groups
from src.models.settings import FOLDS, SEED
from src.models.tuning import candidates, make_model
from src.paths import MARKETS_PARQUET, EVENT_UNIVERSE, PROJECT_ROOT, PROCESSED_DIR

NAMES = ('logistic','random_forest','boosted_trees','xgboost')
BUDGETS = (5,3)


def split_masks(frame, start, end, horizon):
    """Window labels must finish strictly before the evaluation starts."""
    return (frame.time.add(horizon).lt(start) & frame.label_known,
            frame.time.ge(start) & frame.time.lt(end))


def validation_windows(frame, outer_start, horizon):
    end = outer_start-horizon
    times = pd.DatetimeIndex(sorted(frame.loc[frame.time.lt(end),'time'].unique()))
    if len(times) < 24:
        raise ValueError('Insufficient inner history')
    points = [times[len(times)//2],times[3*len(times)//4],end]
    return [(points[i],points[i+1]) for i in range(2)]


def opportunity_metrics(frame, score, n):
    result = ranking_metrics(frame,score,n)
    chosen = ranked(frame,score,n)
    novel = frame.label_known & ~frame.opportunity_at_scan & frame.future_positive
    captured = chosen.label_known & ~chosen.opportunity_at_scan & chosen.future_positive
    result.update(known_new_without_scan=int(novel.sum()), captured_new_without_scan=int(captured.sum()),
        new_without_scan_recall=float(captured.sum()/novel.sum()) if novel.any() else np.nan,
        selected_unknown_fraction=float((~chosen.label_known).mean()),
        average_precision=safe_ap(frame.loc[frame.label_known,'target'],np.asarray(score)[frame.label_known.to_numpy()]))
    return result


def choose_candidate(rows):
    """Pooled recall@5, then mean inner AP, then declaration order; no dollar targets."""
    table = pd.DataFrame(rows).groupby('candidate',sort=True).agg(
        captured=('captured_positive_windows','sum'),positive=('positive_windows','sum'),
        ap=('average_precision','mean'))
    if table.positive.nunique() != 1:
        raise ValueError('Candidates must share the same labelled validation population')
    table['recall'] = table.captured/table.positive.replace(0,np.nan)
    return int(table.fillna(0).sort_values(['recall','ap'],ascending=False,kind='stable').index[0])


def fit_classifier(train, name, params, columns):
    if not train.label_known.all() or not train.target.isin([0.,1.]).all():
        raise ValueError('Only known binary opportunity labels may enter fitting')
    numeric = [c for c in columns if c not in METADATA_CATEGORICAL]
    categorical = [c for c in columns if c in METADATA_CATEGORICAL]
    model = make_model(name,'classifier',params,train.target.astype(int),numeric,categorical)
    return model.fit(train[columns],train.target.astype(int))


def train_study(output, data):
    require_split_access('development')
    output, data = Path(output), Path(data)
    data_design = json.loads((data/'design.json').read_text())
    spec = ScreeningSpec(**data_design['window'])
    frame = add_metadata(load_dataset(data))
    if any(c in frame for c in ('pnl_target','activity_target','known_pnl_usd','attempts','partial')):
        raise ValueError('Execution outcomes must not enter this study')
    frame['family'] = frame.event_slug.map(family)
    book = data_design['feature_columns']
    combined = book + METADATA_COLUMNS
    sources = ['src/models/opportunities.py','src/models/tuning.py','src/models/estimators.py',
               'src/models/evaluation.py','src/models/features.py','src/data/metadata.py']
    design = dict(version='opportunity_recall_v1',dataset_sha256=sha256(data/'examples.parquet'),
        dataset_completion_sha256=sha256(data/'complete.json'),window=data_design['window'],
        qualification=data_design['qualification'],folds=FOLDS,seed=SEED,primary_budget=5,budgets=list(BUDGETS),
        views={'book':book,'combined':combined},candidates={name:candidates(name,'classifier') for name in NAMES},
        inner_validation='Two last-quarter chronological blocks within each outer training history',
        tuning_objective='Pooled known positive event-hour recall at N=5; ties mean inner AP, then first declared setting',
        embargo_seconds=spec.horizon.total_seconds(),early_stopping=False,
        ensembles='Equal mean within-scan percentile ranks; no fitted ensemble weights',
        execution_outcomes_used=False,final_lockbox_opened=False,
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in sources},
        metadata_sha256={'markets':sha256(MARKETS_PARQUET),'universe':sha256(EVENT_UNIVERSE)},
        packages=dict(python=platform.python_version(),sklearn=sklearn.__version__,xgboost=xgboost.__version__),
        interpretation='Exploratory development under a changed target; scores are not assumed calibrated probabilities')
    output.mkdir(parents=True,exist_ok=False)
    (output/'fitted_models').mkdir()
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    pieces, search, selected, audit, warning_rows, importance = [], [], [], [], [], []
    groups = feature_groups(book)
    groups['metadata'] = METADATA_COLUMNS
    prediction_columns = ['event_slug','stratum','time','family','label_known','target','coverage_fraction',
        'future_positive','opportunity_at_scan','witnessed_opportunity','x_active_tokens']
    for fold, start, end in FOLDS:
        start, end = pd.Timestamp(start,tz='UTC'),pd.Timestamp(end,tz='UTC')
        a,b = split_masks(frame,start,end,spec.horizon)
        train,test = frame.loc[a],frame.loc[b]
        if train.empty or test.empty:
            raise ValueError('Empty outer training or evaluation')
        scores = {}
        for name in NAMES:
            print(f'{fold}: opportunity {name}, {len(train)} rows / {int(train.target.sum())} positives',flush=True)
            local = []
            options = candidates(name,'classifier')
            for candidate,params in enumerate(options):
                for inner,(lo,hi) in enumerate(validation_windows(frame,start,spec.horizon)):
                    x,y = split_masks(frame,lo,hi,spec.horizon)
                    fitting,valid = frame.loc[x],frame.loc[y]
                    if fitting.empty or valid.empty:
                        raise ValueError('Empty inner training or validation')
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter('always')
                        model = fit_classifier(fitting,name,params,combined)
                    warning_rows.extend(dict(fold=fold,model=name,candidate=candidate,inner=inner,warning=str(w.message)) for w in caught)
                    prediction = predict(model,valid[combined])
                    metric = opportunity_metrics(valid,prediction,5)
                    local.append(dict(fold=fold,model=name,candidate=candidate,inner=inner,
                        params=json.dumps(params,sort_keys=True),training_rows=len(fitting),training_positive=int(fitting.target.sum()),
                        train_last=str(fitting.time.max()),validation_start=str(lo),validation_end=str(hi),
                        **{k:metric[k] for k in ('positive_windows','captured_positive_windows','recall','average_precision','selected_unknown')}))
            best = choose_candidate(local)
            search.extend(local)
            selected.append(dict(fold=fold,model=name,candidate=best,params=json.dumps(options[best],sort_keys=True)))
            for key,columns,index in [(f'fixed_book_{name}',book,0),(f'fixed_combined_{name}',combined,0),
                                      (f'tuned_combined_{name}',combined,best)]:
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always')
                    model=fit_classifier(train,name,options[index],columns)
                warning_rows.extend(dict(fold=fold,model=key,candidate=index,inner='outer',warning=str(w.message)) for w in caught)
                scores[key]=predict(model,test[columns])
                if not np.isfinite(scores[key]).all():
                    raise ValueError('Nonfinite predictions')
                joblib.dump(model,output/'fitted_models'/f'{fold}_{key}.joblib',compress=3)
                audit.append(dict(fold=fold,model=key,training_rows=len(train),training_positive=int(train.target.sum()),
                    train_last=str(train.time.max()),evaluation_start=str(start),evaluation_rows=len(test)))
                if key.startswith('tuned_'):
                    baseline=opportunity_metrics(test,scores[key],5)
                    positions=list(test.groupby('time',sort=False).indices.values())
                    for group,members in groups.items():
                        for repeat in range(3):
                            rng=np.random.default_rng(SEED+repeat)
                            order=np.arange(len(test))
                            for pos in positions:
                                order[pos]=rng.permutation(pos)
                            shuffled=test[columns].copy()
                            shuffled.loc[:,members]=test[members].iloc[order].to_numpy()
                            altered=opportunity_metrics(test,predict(model,shuffled),5)
                            importance.append(dict(fold=fold,model=key,group=group,repeat=repeat,
                                positive_hours_lost=baseline['captured_positive_windows']-altered['captured_positive_windows'],
                                recall_drop=baseline['recall']-altered['recall'],
                                future_recall_drop=baseline['future_recall']-altered['future_recall']))
            pd.DataFrame(search).to_csv(output/'search.csv',index=False)
            pd.DataFrame(selected).to_csv(output/'selected_parameters.csv',index=False)
            pd.DataFrame(audit).to_csv(output/'fit_audit.csv',index=False)
            pd.DataFrame(importance).to_csv(output/'feature_importance.csv',index=False)
            pd.DataFrame(warning_rows,columns=['fold','model','candidate','inner','warning']).to_csv(output/'warnings.csv',index=False)
        for prefix in ('fixed_book','fixed_combined','tuned_combined'):
            scores[f'{prefix}_rank_ensemble']=np.mean([
                pd.Series(scores[f'{prefix}_{name}'],index=test.index).groupby(test.time).rank(pct=True).to_numpy()
                for name in NAMES],axis=0)
        for key,column,sign,missing in [('quoted_gap_rule','x_best_quoted_net_usd',1,-1e12),
                ('depth_rule','x_ask_depth_sum',1,-1e12),('freshness_rule','x_book_age_max_s',-1,1e12)]:
            scores[key]=sign*test[column].fillna(missing).to_numpy()
        for key,score in scores.items():
            pieces.append(test[prediction_columns].assign(model=key,fold=fold,score=np.asarray(score)))
    prediction=pd.concat(pieces,ignore_index=True)
    if prediction.duplicated(['model','event_slug','time']).any():
        raise ValueError('Duplicate outer predictions')
    prediction.to_parquet(output/'predictions.parquet',index=False)
    rows=[]
    for scope,excluded in [('all_events',None),('exclude_december_fed','fed-decision-in-december')]:
        pool=prediction if excluded is None else prediction.loc[prediction.event_slug.ne(excluded)]
        for key,part in pool.groupby('model'):
            for fold,subset in [('all',part)]+list(part.groupby('fold')):
                for n in BUDGETS:
                    rows.append(dict(scope=scope,model=key,fold=fold,**opportunity_metrics(subset,subset.score,n)))
    pd.DataFrame(rows).to_csv(output/'metrics.csv',index=False)
    files={p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(predictions_sha256=sha256(output/'predictions.parquet'),
        files_sha256=files,models=prediction.model.nunique(),execution_outcomes_used=False,
        final_lockbox_opened=False),indent=2)+'\n')


def main():
    from src.models.opportunities import train_study as run_training
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--data',type=Path,default=PROCESSED_DIR/'opportunities')
    args=parser.parse_args()
    with threadpool_limits(limits=4):
        run_training(args.output,args.data)


if __name__ == '__main__':
    main()
