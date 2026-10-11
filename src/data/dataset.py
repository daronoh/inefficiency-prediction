"""Reader-facing alignment views and the exact event-window modelling table."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
from src.common import sha256
from src.config import SPEC, spec_hash
from src.data.books import RawGroup


def align_books(frames, times=None):
    """Long-form backward-as-of view; no forward fill outside a token's extent.

The pipeline aligns derived depth quotes in memory. This compact view exposes
the same time direction for inspection without materialising every book level.
"""
    readers = {token: RawGroup([frame]) for token, frame in frames.items()}
    if times is None:
        times = pd.DatetimeIndex([], tz='UTC')
        for reader in readers.values():
            times = times.union(reader.times[0])
    times = pd.DatetimeIndex(times).sort_values()
    if len(times) and (times.min() < pd.Timestamp(SPEC.development_start) or times.max() >= pd.Timestamp(SPEC.lockbox_start)):
        raise ValueError('Alignment is restricted to development times')
    rows = []
    for when in times:
        for token, reader in readers.items():
            books = reader.at(when)
            book = books[0] if books else None
            rows.append(dict(time=when, token_id=token,
                source_time=book.stamp if book else pd.NaT,
                age_s=(when-book.stamp).total_seconds() if book else np.nan,
                observed=book is not None, clean=book.clean if book else False,
                best_bid=book.bids.prices[0] if book and len(book.bids) else np.nan,
                best_ask=book.asks.prices[0] if book and len(book.asks) else np.nan,
                ask_depth=book.asks.sizes.sum() if book else np.nan))
    return pd.DataFrame(rows)


def load_examples(features_path, labels_dir, spec=SPEC):
    """All snapshot-eligible event-windows, with unknown targets left missing."""
    features_path, labels_dir = Path(features_path), Path(labels_dir)
    design = json.loads((labels_dir / 'design.json').read_text())
    completion = json.loads((labels_dir / 'complete.json').read_text())
    feature_design = json.loads((features_path.parent / 'run.json').read_text())
    if design['feature_sha256'] != sha256(features_path) or completion['labels_sha256'] != sha256(labels_dir / 'labels.parquet'):
        raise ValueError('Feature/label completion hashes do not match')
    if design['spec_hash'] != spec_hash(spec) or feature_design['spec_hash'] != spec_hash(spec):
        raise ValueError('Rebuild features and labels for the requested window/settings')
    features = pd.read_parquet(features_path)
    labels = pd.read_parquet(labels_dir / 'labels.parquet')
    for frame in [features, labels]:
        if frame.time.min() < pd.Timestamp(spec.development_start) or frame.time.max() >= pd.Timestamp(spec.lockbox_start):
            raise ValueError('Final-test data cannot enter development examples')
    if labels.quote_policy.unique().tolist() != ['carry_forward']:
        raise ValueError('Expected the declared carry-forward label policy')
    frame = features.merge(labels, on=['event_slug', 'time'], validate='one_to_one')
    frame = frame.loc[frame.eligible_snapshot].sort_values(['time', 'event_slug']).reset_index(drop=True)
    frame['label_known'] = frame.coverage_fraction.ge(1 - 1e-9) & frame.unknown_attempts.eq(0)
    frame['target'] = frame.positive.astype(float).where(frame.label_known)
    frame['future_positive'] = frame.future_positive & frame.label_known
    return frame
