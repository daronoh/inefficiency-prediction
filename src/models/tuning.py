"""Bounded nested chronological tuning; no access to the final test.

Inner selection uses signed standalone top-five hourly targets, a proxy only.
Outer predictions must undergo persistent replay before economic interpretation.
"""
from dataclasses import replace
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
from sklearn.base import BaseEstimator
from sklearn.dummy import DummyRegressor
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, brier_score_loss
from threadpoolctl import threadpool_limits
from xgboost import XGBClassifier, XGBRegressor

from src.common import sha256
from src.config import ScreeningSpec, require_split_access
from src.data.metadata import add_metadata, METADATA_COLUMNS, METADATA_CATEGORICAL
from src.data.profitability import load_profitability
from src.models.estimators import estimator, predict
from src.models.profitability import regressor, selected_value
from src.models.evaluation import masks, ranking_metrics, safe_ap
from src.models.features import family, feature_groups
from src.models.settings import FOLDS, SEED
from src.reporting.workflow import profitability_dataset, profitability_run


def candidates(name, kind):
    """Eight declared configurations per family; order resolves exact ties."""
    if name == 'logistic':
        return [dict(C=c, penalty=p, class_weight=w,
                     solver='liblinear' if p == 'l1' else 'lbfgs')
                for c, p, w in [(1., 'l2', 'balanced'), (.01, 'l2', 'balanced'),
                    (.1, 'l2', 'balanced'), (10., 'l2', 'balanced'),
                    (.1, 'l1', 'balanced'), (1., 'l1', 'balanced'),
                    (.1, 'l2', None), (1., 'l2', None)]]
    if name == 'random_forest':
        base = dict(n_estimators=200, max_depth=10 if kind == 'classifier' else 8,
                    min_samples_leaf=20, max_features='sqrt' if kind == 'classifier' else 1.)
        return [base] + [dict(n_estimators=200, max_depth=d, min_samples_leaf=l, max_features=f)
            for d, l, f in [(4, 20, .5), (8, 5, .5), (8, 50, 1.),
                           (12, 20, .5), (None, 20, .5), (6, 50, .5), (12, 5, 1.)]]
    if name == 'boosted_trees':
        return [dict(max_iter=n, learning_rate=r, max_leaf_nodes=l,
                     min_samples_leaf=m, l2_regularization=a)
                for n, r, l, m, a in [(120, .05, 15, 30, 1. if kind == 'classifier' else 10.),
                    (120, .05, 7, 30, 10.), (200, .03, 7, 50, 10.),
                    (200, .05, 15, 50, 10.), (120, .05, 31, 30, 10.),
                    (200, .03, 15, 10, 10.), (120, .1, 7, 30, 1.), (200, .05, 15, 50, 100.)]]
    if name == 'xgboost':
        return [dict(n_estimators=n, learning_rate=r, max_depth=d, min_child_weight=m,
                     subsample=s, colsample_bytree=f, reg_lambda=l, reg_alpha=a)
                for n, r, d, m, s, f, l, a in [
                    (200, .05, 3, 10, 1., 1., 10., 0.),
                    (200, .05, 2, 10, .8, .8, 10., 0.),
                    (400, .03, 3, 20, .8, .8, 10., 0.),
                    (200, .05, 4, 20, .8, .8, 10., 1.),
                    (200, .05, 6, 20, .8, .8, 10., 1.),
                    (400, .03, 2, 10, 1., 1., 1., 0.),
                    (200, .1, 3, 20, .8, .8, 100., 1.),
                    (200, .05, 3, 50, 1., .8, 100., 10.)]]
    raise ValueError(name)


METHODS = [('classifier', n) for n in ('logistic', 'random_forest', 'boosted_trees', 'xgboost')]
METHODS += [(k, n) for k in ('regression', 'two_stage')
            for n in ('random_forest', 'boosted_trees', 'xgboost')]


def inner_windows(frame, outer_start, spec):
    """Two time blocks within outer training history; all events share boundaries."""
    embargo = spec.horizon + pd.Timedelta(seconds=spec.arrival_delay_s + spec.unwind_delay_s)
    end = pd.Timestamp(outer_start, tz='UTC') - embargo
    times = pd.DatetimeIndex(sorted(frame.loc[frame.time.lt(end), 'time'].unique()))
    if len(times) < 24:
        raise ValueError('Insufficient chronological history for tuning')
    points = [times[len(times)//2], times[3*len(times)//4], end]
    return [(points[i], points[i+1]) for i in range(2)]


def inner_masks(frame, start, end, spec):
    embargo = spec.horizon + pd.Timedelta(seconds=spec.arrival_delay_s + spec.unwind_delay_s)
    return (frame.time.lt(start - embargo) & frame.label_known,
            frame.time.ge(start) & frame.time.lt(end))


def make_model(name, kind, params, y, numeric, categorical, gate=False):
    """Preprocessing is fit with each model; gates always use natural prevalence."""
    if kind == 'classifier':
        pipe = estimator(name if name != 'xgboost' else 'logistic', y, numeric, categorical)
        if len(np.unique(y)) < 2:
            return pipe
        if name == 'xgboost':
            counts = np.bincount(np.asarray(y, dtype=int), minlength=2)
            model = XGBClassifier(tree_method='hist', n_jobs=4, random_state=SEED,
                objective='binary:logistic', eval_metric='logloss',
                scale_pos_weight=1. if gate else counts[0]/counts[1], **params)
        else:
            model = pipe[-1].set_params(**params)
            if gate:
                model.set_params(class_weight=None)
    else:
        pipe = regressor(name if name != 'xgboost' else 'random_forest', numeric, categorical)
        model = (XGBRegressor(tree_method='hist', n_jobs=4, random_state=SEED,
                             objective='reg:squarederror', **params)
                 if name == 'xgboost' else pipe[-1].set_params(**params))
    return Pipeline(pipe.steps[:-1] + [('model', model)])


class TunedActivityPnL(BaseEstimator):
    """Unweighted activity gate times conditional signed dollars, with shared settings."""
    def __init__(self, name, params, numeric, categorical):
        self.name, self.params, self.numeric, self.categorical = name, params, numeric, categorical

    def fit(self, x, y, activity):
        y, activity = np.asarray(y, dtype=float), np.asarray(activity, dtype=int)
        if not np.isfinite(y).all() or not np.isin(activity, [0, 1]).all():
            raise ValueError('Known finite targets required')
        if np.any((activity == 0) & (np.abs(y) > 1e-10)):
            raise ValueError('Inactive hours must have zero P&L')
        self.gate_ = make_model(self.name, 'classifier', self.params, activity,
                               self.numeric, self.categorical, gate=True).fit(x, activity)
        active = activity.astype(bool)
        self.active_rows_ = int(active.sum())
        self.amount_ = (make_model(self.name, 'regression', self.params, y[active],
                                   self.numeric, self.categorical).fit(x.loc[active], y[active])
                        if active.any() else DummyRegressor(strategy='constant', constant=0.).fit(np.zeros((len(x),1)), y))
        return self

    def components(self, x):
        return predict(self.gate_, x), self.amount_.predict(x if self.active_rows_ else np.zeros((len(x),1)))

    def predict(self, x):
        probability, amount = self.components(x)
        return probability * amount


def fit_model(train, kind, name, params, columns):
    numeric = [c for c in columns if c not in METADATA_CATEGORICAL]
    categorical = [c for c in columns if c in METADATA_CATEGORICAL]
    if kind == 'two_stage':
        return TunedActivityPnL(name, params, numeric, categorical).fit(
            train[columns], train.pnl_target, train.activity_target)
    y = train.target.astype(int) if kind == 'classifier' else train.pnl_target
    return make_model(name, kind, params, y, numeric, categorical).fit(train[columns], y)


def score_model(model, x, kind):
    result = predict(model, x) if kind == 'classifier' else model.predict(x)
    if not np.isfinite(result).all():
        raise ValueError('Nonfinite model scores')
    return result


def choose_candidate(rows):
    """Equal search budgets; deterministic ties, no outer outcomes consulted."""
    totals = pd.DataFrame(rows).groupby('candidate').agg(
        pnl=('selected_standalone_pnl_usd', 'sum'), hours=('hours', 'sum'))
    return int((totals.pnl / totals.hours).sort_values(ascending=False, kind='stable').index[0])


def fit_study(output, dataset_dir, baseline_dir):
    require_split_access('development')
    output, data, baseline = map(Path, (output, dataset_dir, baseline_dir))
    base_design = json.loads((baseline/'design.json').read_text())
    base_complete = json.loads((baseline/'complete.json').read_text())
    if base_design['dataset_sha256'] != sha256(data/'examples.parquet'):
        raise ValueError('Baseline dataset differs')
    if base_complete['predictions_sha256'] != sha256(baseline/'predictions.parquet'):
        raise ValueError('Baseline predictions differ')
    if base_design.get('final_lockbox_opened') or base_complete.get('final_lockbox_opened'):
        raise ValueError('Final test must remain closed')
    spec = replace(ScreeningSpec(**base_design['dataset_design']['contract']), arrival_delay_s=.1)
    frame = add_metadata(load_profitability(data))
    frame['family'] = frame.event_slug.map(family)
    columns = base_design['views']['combined']
    source = pd.read_parquet(baseline/'predictions.parquet')
    names = [n for n in source.model.unique() if n.startswith('combined_') or n in
             ('quoted_gap_rule', 'depth_rule', 'freshness_rule')]
    if set(source.loc[source.model.eq('quoted_gap_rule'), 'time']) != set(
            frame.loc[frame.time.ge(pd.Timestamp(FOLDS[0][1], tz='UTC')), 'time']):
        raise ValueError('Baseline and new study do not share evaluation times')
    output.mkdir(parents=True, exist_ok=False)
    (output/'fitted_models').mkdir()
    design = dict(base_design)
    design.update(study='bounded_nested_tuning_v1', source_sha256=sha256(__file__),
        baseline_predictions_sha256=sha256(baseline/'predictions.parquet'),
        baseline_design_sha256=sha256(baseline/'design.json'),
        packages=dict(python=platform.python_version(), sklearn=sklearn.__version__, xgboost=xgboost.__version__),
        tuning=dict(candidates={f'{k}_{n}':candidates(n,k) for k,n in METHODS},
            inner_blocks='Last two quarters of available outer-training scan times; expanding fits',
            objective='Mean signed standalone selected top-five P&L per inner validation scan; proxy, not persistent replay',
            candidate_tie='First declared configuration', primary_budget=5, budgets=[5,3],
            early_stopping=False, scope='Combined features only; 8 candidates per method; 2 inner time blocks',
            status='Exploratory extension after inspecting existing development results; final test untouched',
            two_stage='Shared gate/amount hyperparameters; gate unweighted; negative and zero active hours retained'))
    (output/'design.json').write_text(json.dumps(design, indent=2)+'\n')
    pieces = [source.loc[source.model.isin(names)].copy()]
    audit, search, choices, importance, gate_rows, warning_rows = [], [], [], [], [], []
    groups = feature_groups(columns)
    groups['metadata'] = METADATA_COLUMNS
    for fold, start, end in FOLDS:
        training, evaluation = masks(frame, start, end, spec)
        train, test = frame.loc[training], frame.loc[evaluation]
        windows = inner_windows(frame, start, spec)
        for kind, name in METHODS:
            key = f'tuned_{kind}_{name}'
            options = candidates(name, kind)
            print(f'{fold}: {key} ({len(train)} training rows)', flush=True)
            local = []
            for candidate, params in enumerate(options):
                for inner, (lo, hi) in enumerate(windows):
                    a, b = inner_masks(frame, lo, hi, spec)
                    inner_train, valid = frame.loc[a], frame.loc[b]
                    if inner_train.empty or valid.empty or not valid.label_known.any():
                        raise ValueError('Empty inner training/validation')
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter('always')
                        model = fit_model(inner_train, kind, name, params, columns)
                    warning_rows.extend(dict(fold=fold, model=key, candidate=candidate, inner=inner,
                                             warning=str(w.message)) for w in caught)
                    score = score_model(model, valid[columns], kind)
                    row = dict(fold=fold, model=key, candidate=candidate, inner=inner,
                        params=json.dumps(params, sort_keys=True), train_rows=len(inner_train),
                        train_last=str(inner_train.time.max()), validation_start=str(lo), validation_end=str(hi),
                        train_activity=int(inner_train.activity_target.sum()), validation_known=int(valid.label_known.sum()),
                        hours=valid.time.nunique(), selected_standalone_pnl_usd=selected_value(valid, score, 5))
                    local.append(row)
            best = choose_candidate(local)
            search.extend(local)
            choices.append(dict(fold=fold, model=key, candidate=best, params=json.dumps(options[best], sort_keys=True)))
            # Fixed XGBoost candidate zero provides a same-family untuned comparator.
            fits = [(key, best)] + ([(f'fixed_{kind}_xgboost', 0)] if name == 'xgboost' else [])
            for model_key, candidate in fits:
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always')
                    model = fit_model(train, kind, name, options[candidate], columns)
                warning_rows.extend(dict(fold=fold, model=model_key, candidate=candidate, inner='outer',
                                         warning=str(w.message)) for w in caught)
                score = score_model(model, test[columns], kind)
                joblib.dump(model, output/'fitted_models'/f'{fold}_{model_key}.joblib', compress=3)
                part = test[source.columns.intersection(test.columns)].copy()
                part['model'], part['fold'], part['score'], part['kind'] = model_key, fold, score, kind
                pieces.append(part)
                audit.append(dict(fold=fold, model=model_key, training_rows=len(train),
                    train_last_time=str(train.time.max()), test_first_time=str(test.time.min()),
                    trading_rows=int(train.activity_target.sum()), loss_hours=int(train.pnl_target.lt(0).sum())))
                if kind == 'two_stage':
                    p, amount = model.components(test[columns])
                    gate_rows.append(pd.DataFrame(dict(fold=fold, model=model_key, event_slug=test.event_slug,
                        time=test.time, label_known=test.label_known, activity_target=test.activity_target,
                        probability=p, conditional_pnl_usd=amount)))
                if model_key.startswith('tuned_'):
                    positions = list(test.groupby('time', sort=False).indices.values())
                    base_value = selected_value(test, score, 5)
                    base_recall = ranking_metrics(test, score, 5)['recall']
                    for group, members in groups.items():
                        for repeat in range(3):
                            rng = np.random.default_rng(SEED+repeat)
                            order = np.arange(len(test))
                            for pos in positions:
                                order[pos] = rng.permutation(pos)
                            shuffled = test[columns].copy()
                            shuffled.loc[:, members] = test[members].iloc[order].to_numpy()
                            changed = score_model(model, shuffled, kind)
                            importance.append(dict(fold=fold, model=model_key, group=group, repeat=repeat,
                                selected_standalone_pnl_drop_usd=base_value-selected_value(test, changed, 5),
                                recall_drop=base_recall-ranking_metrics(test, changed, 5)['recall']))
            pd.DataFrame(search).to_csv(output/'search.csv', index=False)
            pd.DataFrame(choices).to_csv(output/'selected_parameters.csv', index=False)
            pd.DataFrame(audit).to_csv(output/'fit_audit.csv', index=False)
            pd.DataFrame(importance).to_csv(output/'feature_importance.csv', index=False)
            pd.DataFrame(warning_rows, columns=['fold','model','candidate','inner','warning']).to_csv(output/'warnings.csv', index=False)
    combined = pd.concat(pieces, ignore_index=True)
    if combined.duplicated(['model','event_slug','time']).any():
        raise ValueError('Duplicate outer predictions')
    reference = source.loc[source.model.eq('quoted_gap_rule')].set_index(['event_slug','time']).index.sort_values()
    for model_key, part in combined.groupby('model'):
        if not part.set_index(['event_slug','time']).index.sort_values().equals(reference):
            raise ValueError(f'Mismatched eligible pool: {model_key}')
    combined.to_parquet(output/'predictions.parquet', index=False)
    metrics = []
    for key, part in combined.groupby('model'):
        known = part.loc[part.label_known]
        for n in (5,3):
            metrics.append(dict(model=key, kind=part.kind.iloc[0], **ranking_metrics(part,part.score,n),
                average_precision=safe_ap(known.target, known.score),
                selected_standalone_pnl_usd=selected_value(part,part.score,n),
                mae_usd=mean_absolute_error(known.pnl_target,known.score) if part.kind.iloc[0] in ('regression','two_stage') else np.nan))
    pd.DataFrame(metrics).to_csv(output/'metrics.csv', index=False)
    gates = pd.concat(gate_rows, ignore_index=True)
    gates.to_parquet(output/'gate_predictions.parquet', index=False)
    calibration = []
    for (key, fold), part in gates.loc[gates.label_known].groupby(['model','fold']):
        calibration.append(dict(model=key, fold=fold, rows=len(part), mean_probability=part.probability.mean(),
            observed_activity=part.activity_target.mean(), brier=brier_score_loss(part.activity_target,part.probability)))
    pd.DataFrame(calibration).to_csv(output/'gate_calibration.csv', index=False)
    artifacts = {p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(predictions_sha256=sha256(output/'predictions.parquet'),
        models=combined.model.nunique(), files_sha256=artifacts, final_lockbox_opened=False), indent=2)+'\n')


def main():
    # Use the importable module, not __main__, for portable joblib class paths.
    from src.models.tuning import fit_study as run_fit
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    with threadpool_limits(limits=4):
        run_fit(args.output, profitability_dataset(), profitability_run())


if __name__ == '__main__':
    main()
