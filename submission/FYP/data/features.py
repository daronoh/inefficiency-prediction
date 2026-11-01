from __future__ import annotations
import pandas as pd
from FYP.config import SPEC

def add_hourly_history(frame, spec=SPEC):
    if frame.duplicated(['event_slug', 'time']).any():
        raise ValueError('Duplicate event/decision time')
    result = frame.sort_values(['event_slug', 'time']).copy()
    consecutive = result.time.sub(result.groupby('event_slug').time.shift()).eq(pd.Timedelta(minutes=spec.cadence_minutes))
    for column in [c for c in frame if c.startswith('x_')]:
        previous = result.groupby('event_slug')[column].shift().where(consecutive)
        result[f'x_snapshot_lag1_{column[2:]}'] = previous
        result[f'x_snapshot_change1_{column[2:]}'] = result[column] - previous
    return result
