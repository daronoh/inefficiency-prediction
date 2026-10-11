"""Purged time splits and ranking metrics over the complete eligible pool."""
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

def masks(frame, start, end, spec):
    start, end = pd.Timestamp(start, tz='UTC'), pd.Timestamp(end, tz='UTC')
    # Purge the full label horizon plus the last potential unwind, before start.
    embargo = spec.horizon + pd.Timedelta(seconds=spec.arrival_delay_s + spec.unwind_delay_s)
    train = frame.time.lt(start - embargo) & frame.label_known
    test = frame.time.ge(start) & frame.time.lt(end)
    return train, test


def ranked(frame, score, n):
    """Rank the entire snapshot-eligible universe BEFORE consulting coverage."""
    return frame.assign(_score=np.asarray(score)).sort_values(
        ['time', '_score', 'event_slug'], ascending=[True, False, True], kind='stable').groupby('time', sort=False).head(n)


def ranking_metrics(frame, score, n):
    selected = ranked(frame, score, n)
    known = frame.label_known.astype(bool)
    selected_known = selected.label_known.astype(bool)
    total_positive = int((frame.target.eq(1) & known).sum())
    captured = int(selected.target.eq(1).sum())
    future_total = int(frame.future_positive.sum())
    count = frame.groupby('time').size()
    competitive = frame.time.map(count).gt(n)
    selected_competitive = selected.time.map(count).gt(n)
    competitive_positive = int(frame.loc[competitive, 'target'].eq(1).sum())
    # Expected random capture is exact given the observed candidate pool.
    random_probability = frame.time.map(count).map(lambda size: min(n, size) / size)
    random_positive = float((frame.target.eq(1) * random_probability).sum())
    return dict(top_n=n, windows=frame.time.nunique(), competitive_windows=int(count.gt(n).sum()),
        candidates=len(frame), selected=len(selected), known_candidates=int(known.sum()),
        known_selected=int(selected_known.sum()), selected_unknown=int((~selected_known).sum()),
        positive_windows=total_positive, captured_positive_windows=captured,
        recall=captured / total_positive if total_positive else np.nan,
        precision_known=captured / selected_known.sum() if selected_known.any() else np.nan,
        competitive_recall=float(selected.loc[selected_competitive, 'target'].eq(1).sum() / competitive_positive) if competitive_positive else np.nan,
        future_positive_windows=future_total, captured_future_positive_windows=int(selected.future_positive.sum()),
        future_recall=float(selected.future_positive.sum() / future_total) if future_total else np.nan,
        expected_random_captured=random_positive,
        capture_lift_vs_random=captured / random_positive if random_positive else np.nan,
        selected_token_snapshots=float(selected.x_active_tokens.sum()),
        all_token_snapshots=float(frame.x_active_tokens.sum()))


def safe_ap(y, scores):
    return float(average_precision_score(y, scores)) if len(y) and np.sum(y) else np.nan
