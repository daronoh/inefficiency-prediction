"""Load development-only histories and build backward-as-of basket readers."""
import pandas as pd
from src.config import SPEC
from src.data.books import RawGroup, ObservationGroup

def load_frames(plan):
    return {token: pd.read_parquet(path, columns=RawGroup.COLUMNS, filters=[
        ('snapshot_time', '>=', pd.Timestamp(SPEC.development_start)),
        ('snapshot_time', '<', pd.Timestamp(SPEC.lockbox_start))])
        for token, path in plan['paths'].items()}

def observation_groups(plan, frames, required=None):
    groups, mapping = {}, {}
    for i, (ids, start, end) in enumerate(plan['packages']):
        pid = f"{plan['event_slug']}:{i}"
        if required is not None and pid not in required:
            continue
        mapping[pid] = tuple(ids)
        groups[pid] = ObservationGroup([frames[t] for t in ids])
    return groups, mapping
