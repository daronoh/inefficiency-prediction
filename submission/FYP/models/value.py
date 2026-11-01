from __future__ import annotations
import numpy as np
from sklearn.base import BaseEstimator
from sklearn.dummy import DummyRegressor
from FYP.data.metadata import METADATA_CATEGORICAL
from FYP.models.estimators import predict
from FYP.models.classification import opportunity_metrics
from FYP.models.metrics import ranked
from FYP.models.tuning import make_model

class OpportunityValueModel(BaseEstimator):
    """Unweighted P(opportunity) times conditional nonnegative peak quote value."""

    def __init__(self, name, params, numeric, categorical):
        self.name, self.params, self.numeric, self.categorical = (name, params, numeric, categorical)

    def fit(self, x, y):
        y = np.asarray(y, dtype=float)
        if not np.isfinite(y).all() or (y < 0).any():
            raise ValueError('Known nonnegative quote targets required')
        positive = y > 0
        self.feature_names_in_ = np.asarray(x.columns, dtype=object)
        self.gate_ = make_model(self.name, 'classifier', self.params, positive.astype(int), self.numeric, self.categorical, gate=True).fit(x, positive.astype(int))
        self.positive_rows_ = int(positive.sum())
        self.amount_ = make_model(self.name, 'regression', self.params, y[positive], self.numeric, self.categorical).fit(x.loc[positive], y[positive]) if positive.any() else DummyRegressor(strategy='constant', constant=0.0).fit(np.zeros((len(x), 1)), y)
        return self

    def components(self, x):
        probability = predict(self.gate_, x)
        value = np.maximum(0.0, self.amount_.predict(x if self.positive_rows_ else np.zeros((len(x), 1))))
        return (probability, value)

    def predict(self, x):
        probability, value = self.components(x)
        return probability * value

def fit_value(train, name, kind, params, columns):
    if not train.label_known.all() or not np.isfinite(train.value_target).all() or (train.value_target < 0).any():
        raise ValueError('Value fits require known nonnegative opportunity targets')
    numeric = [c for c in columns if c not in METADATA_CATEGORICAL]
    categorical = [c for c in columns if c in METADATA_CATEGORICAL]
    model = OpportunityValueModel(name, params, numeric, categorical) if kind == 'two_stage' else make_model(name, 'regression', params, train.value_target, numeric, categorical)
    return model.fit(train[columns], train.value_target)

def value_metrics(frame, score, n):
    result = opportunity_metrics(frame, score, n)
    chosen = ranked(frame, score, n)
    total = float(frame.loc[frame.label_known, 'value_target'].sum())
    captured = float(chosen.loc[chosen.label_known, 'value_target'].sum())
    count = frame.groupby('time').size()
    chance = frame.time.map(count).map(lambda size: min(n, size) / size)
    random = float((frame.value_target.fillna(0) * chance).sum())
    result.update(total_peak_value=total, captured_peak_value=captured, peak_value_recall=captured / total if total else np.nan, expected_random_peak_value=random, peak_value_lift_vs_random=captured / random if random else np.nan)
    return result
