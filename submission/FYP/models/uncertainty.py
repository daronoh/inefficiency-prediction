from __future__ import annotations
import numpy as np
import pandas as pd

def paired_block_intervals(pool, selected, baseline, *, repetitions=10000):
    """Resample whole scheduled seven-day blocks jointly for both policies.

    This conditions on fitted models and this observed cohort. It does not
    estimate uncertainty from refitting, missing outcomes, or market selection.
    """
    origin = pool.time.min().floor('D')
    count = int((pool.time.max() - origin) // pd.Timedelta(days=7)) + 1

    def blocks(part):
        return ((part.time - origin) // pd.Timedelta(days=7)).astype(int)
    denominator = pool.assign(block=blocks(pool)).groupby('block').agg(positives=('target', lambda x: x.eq(1).sum()), value=('value_target', 'sum')).reindex(range(count), fill_value=0)

    def totals(part):
        return part.assign(block=blocks(part)).groupby('block').agg(positives=('target', lambda x: x.eq(1).sum()), value=('value_target', 'sum')).reindex(range(count), fill_value=0)
    difference = totals(selected) - totals(baseline)
    draws = np.random.default_rng(20261030).integers(count, size=(repetitions, count))
    result = {'seven_day_blocks': count}
    for column, name in [('positives', 'recall'), ('value', 'value_capture')]:
        num = difference[column].to_numpy()[draws].sum(axis=1)
        den = denominator[column].to_numpy()[draws].sum(axis=1)
        valid = den > 0
        low, high = np.quantile(num[valid] / den[valid], [0.025, 0.975]) if valid.any() else (np.nan, np.nan)
        total = denominator[column].sum()
        result.update({f'{name}_difference': float(difference[column].sum() / total) if total else np.nan, f'{name}_lower95': low, f'{name}_upper95': high, f'{name}_defined_draws': int(valid.sum())})
    return result
