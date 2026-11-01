from __future__ import annotations
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from FYP.models.estimators import estimator
from FYP.models.settings import SEED

def regressor(name, numeric=None, categorical=None, conditional=False):
    if name == 'ridge':
        model = Ridge(alpha=10.0)
    elif name == 'random_forest':
        model = RandomForestRegressor(n_estimators=200, max_depth=8, min_samples_leaf=5 if conditional else 20, n_jobs=4, random_state=SEED)
    else:
        raise ValueError(name)
    preprocessing = estimator('logistic', np.array([0, 1]), numeric=numeric, categorical=categorical)
    return Pipeline(preprocessing.steps[:-1] + [('regression', model)])
