from __future__ import annotations
import numpy as np
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier, XGBRegressor
from FYP.models.estimators import estimator
from FYP.models.regression import regressor
from FYP.models.settings import SEED

def candidates(name, kind):
    """Eight declared configurations per family; order resolves exact ties."""
    if name == 'logistic':
        return [dict(C=c, penalty=p, class_weight=w, solver='liblinear' if p == 'l1' else 'lbfgs') for c, p, w in [(1.0, 'l2', 'balanced'), (0.01, 'l2', 'balanced'), (0.1, 'l2', 'balanced'), (10.0, 'l2', 'balanced'), (0.1, 'l1', 'balanced'), (1.0, 'l1', 'balanced'), (0.1, 'l2', None), (1.0, 'l2', None)]]
    if name == 'random_forest':
        base = dict(n_estimators=200, max_depth=10 if kind == 'classifier' else 8, min_samples_leaf=20, max_features='sqrt' if kind == 'classifier' else 1.0)
        return [base] + [dict(n_estimators=200, max_depth=d, min_samples_leaf=l, max_features=f) for d, l, f in [(4, 20, 0.5), (8, 5, 0.5), (8, 50, 1.0), (12, 20, 0.5), (None, 20, 0.5), (6, 50, 0.5), (12, 5, 1.0)]]
    if name == 'xgboost':
        return [dict(n_estimators=n, learning_rate=r, max_depth=d, min_child_weight=m, subsample=s, colsample_bytree=f, reg_lambda=l, reg_alpha=a) for n, r, d, m, s, f, l, a in [(200, 0.05, 3, 10, 1.0, 1.0, 10.0, 0.0), (200, 0.05, 2, 10, 0.8, 0.8, 10.0, 0.0), (400, 0.03, 3, 20, 0.8, 0.8, 10.0, 0.0), (200, 0.05, 4, 20, 0.8, 0.8, 10.0, 1.0), (200, 0.05, 6, 20, 0.8, 0.8, 10.0, 1.0), (400, 0.03, 2, 10, 1.0, 1.0, 1.0, 0.0), (200, 0.1, 3, 20, 0.8, 0.8, 100.0, 1.0), (200, 0.05, 3, 50, 1.0, 0.8, 100.0, 10.0)]]
    raise ValueError(name)

def make_model(name, kind, params, y, numeric, categorical, gate=False):
    """Preprocessing is fit with each model; gates always use natural prevalence."""
    if kind == 'classifier':
        pipe = estimator(name if name != 'xgboost' else 'logistic', y, numeric, categorical)
        if len(np.unique(y)) < 2:
            return pipe
        if name == 'xgboost':
            counts = np.bincount(np.asarray(y, dtype=int), minlength=2)
            model = XGBClassifier(tree_method='hist', n_jobs=4, random_state=SEED, objective='binary:logistic', eval_metric='logloss', scale_pos_weight=1.0 if gate else counts[0] / counts[1], **params)
        else:
            model = pipe[-1].set_params(**params)
            if gate:
                model.set_params(class_weight=None)
    else:
        pipe = regressor(name if name != 'xgboost' else 'random_forest', numeric, categorical)
        model = XGBRegressor(tree_method='hist', n_jobs=4, random_state=SEED, objective='reg:squarederror', **params) if name == 'xgboost' else pipe[-1].set_params(**params)
    return Pipeline(pipe.steps[:-1] + [('model', model)])
