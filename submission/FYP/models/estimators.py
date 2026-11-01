from __future__ import annotations
import numpy as np
from sklearn.dummy import DummyClassifier
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler, OneHotEncoder
from FYP.models.settings import SEED

def signed_log(values):
    """Stable import path for serialized model preprocessing."""
    return np.sign(values) * np.log1p(np.abs(values))

def estimator(name, y, numeric=None, categorical=None):
    if len(np.unique(y)) < 2:
        classifier = DummyClassifier(strategy='prior')
    elif name == 'logistic':
        classifier = LogisticRegression(C=1.0, class_weight='balanced', max_iter=2000, random_state=SEED)
    elif name == 'random_forest':
        classifier = RandomForestClassifier(n_estimators=200, max_depth=10, min_samples_leaf=20, class_weight='balanced_subsample', n_jobs=4, random_state=SEED)
    else:
        raise ValueError(name)
    numeric_pipeline = make_pipeline(SimpleImputer(strategy='median', add_indicator=True, keep_empty_features=True), FunctionTransformer(signed_log), StandardScaler())
    if categorical:
        preprocessing = ColumnTransformer([('numeric', numeric_pipeline, numeric), ('categorical', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical)], remainder='drop')
        return make_pipeline(preprocessing, classifier)
    return make_pipeline(*(step for _, step in numeric_pipeline.steps), classifier)

def predict(model, x):
    classes = model[-1].classes_
    if 1 not in classes:
        return np.zeros(len(x))
    return model.predict_proba(x)[:, list(classes).index(1)]
