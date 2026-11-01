from __future__ import annotations
import numpy as np
import pandas as pd
from FYP.data.metadata import METADATA_CATEGORICAL
from FYP.models.metrics import ranked, ranking_metrics, safe_ap
from FYP.models.tuning import make_model

def split_masks(frame, start, end, horizon):
    """Window labels must finish strictly before the evaluation starts."""
    return (frame.time.add(horizon).lt(start) & frame.label_known, frame.time.ge(start) & frame.time.lt(end))

def validation_windows(frame, outer_start, horizon):
    end = outer_start - horizon
    times = pd.DatetimeIndex(sorted(frame.loc[frame.time.lt(end), 'time'].unique()))
    if len(times) < 24:
        raise ValueError('Insufficient inner history')
    points = [times[len(times) // 2], times[3 * len(times) // 4], end]
    return [(points[i], points[i + 1]) for i in range(2)]

def opportunity_metrics(frame, score, n):
    result = ranking_metrics(frame, score, n)
    chosen = ranked(frame, score, n)
    novel = frame.label_known & ~frame.opportunity_at_scan & frame.future_positive
    captured = chosen.label_known & ~chosen.opportunity_at_scan & chosen.future_positive
    result.update(known_new_without_scan=int(novel.sum()), captured_new_without_scan=int(captured.sum()), new_without_scan_recall=float(captured.sum() / novel.sum()) if novel.any() else np.nan, selected_unknown_fraction=float((~chosen.label_known).mean()), average_precision=safe_ap(frame.loc[frame.label_known, 'target'], np.asarray(score)[frame.label_known.to_numpy()]))
    return result

def fit_classifier(train, name, params, columns):
    if not train.label_known.all() or not train.target.isin([0.0, 1.0]).all():
        raise ValueError('Only known binary opportunity labels may enter fitting')
    numeric = [c for c in columns if c not in METADATA_CATEGORICAL]
    categorical = [c for c in columns if c in METADATA_CATEGORICAL]
    model = make_model(name, 'classifier', params, train.target.astype(int), numeric, categorical)
    return model.fit(train[columns], train.target.astype(int))
