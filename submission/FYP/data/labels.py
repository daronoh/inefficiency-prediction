from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import pandas as pd
from FYP.config import SPEC
from FYP.data.books import RawGroup
from FYP.data.coverage import classify_book, observation_fraction
from FYP.simulator.discovery import discover_depth_signals

@dataclass(frozen=True)
class Qualification:
    """Only observation-time constraints; deliberately no latency or unwind fields."""
    sizing: str = 'adaptive'
    min_entry_gross_usd: float = 0.0
    min_limit_margin_usd: float = 0.0
    min_quantity: int = 5
    max_quantity: int | None = None
    cash_cap_usd: float | None = None

def label_windows(windows, signals, coverage, spec=SPEC):
    """Symmetric full-window coverage; witnessed positives in partial windows are diagnostics."""
    windows = pd.DatetimeIndex(windows)
    if len(coverage) != len(windows) or windows.has_duplicates:
        raise ValueError('Expected one coverage value per unique scan')
    if not np.isfinite(coverage).all() or np.any((np.asarray(coverage) < 0) | (np.asarray(coverage) > 1)):
        raise ValueError('Invalid coverage fractions')
    if len(signals):
        if (signals.end_time <= signals.start_time).any():
            raise ValueError('An opportunity episode must have positive observed duration')
        grouped = dict(tuple(signals.groupby('time')))
    else:
        grouped = {}
    rows = []
    for when, fraction in zip(windows, coverage, strict=True):
        part = grouped.get(when, pd.DataFrame())
        if len(part):
            if not (part.start_time.ge(when) & part.end_time.le(when + spec.horizon)).all():
                raise ValueError('Signal crosses its declared monitoring window')
            witnessed = True
            visible = bool(part.start_time.eq(when).any())
            new_onset = bool((part.original_spell_start.gt(when) & part.start_observed).any())
            episodes = part.episode_id.nunique()
            first = part.start_time.min()
        else:
            witnessed = visible = new_onset = False
            episodes, first = (0, pd.NaT)
        known = fraction >= 1 - 1e-09
        rows.append(dict(time=when, coverage_fraction=float(fraction), label_known=known, target=float(witnessed) if known else np.nan, witnessed_opportunity=witnessed, opportunity_at_scan=visible, future_positive=new_onset and known, witnessed_new_onset=new_onset, qualifying_package_episodes=int(episodes), first_opportunity_time=first))
    return pd.DataFrame(rows)

def event_labels(plan, frames, windows, spec=SPEC):
    """Reconstruct quote qualification and coverage; never invoke a trade scheduler."""
    frames = {token: RawGroup([frame]).frames[0] for token, frame in frames.items()}
    states = {}
    for token, frame in frames.items():
        values = [classify_book(r.asks_json, r.bids_json, r.min_order_size, r.tick_size, quantities=None) for r in frame.itertuples(index=False)]
        states[token] = pd.DataFrame(values, columns=['size_qualified', 'state', 'reason']).assign(time=frame.snapshot_time.to_numpy())
    coverage = observation_fraction(plan, states, pd.DatetimeIndex(windows), spec)
    signals = discover_depth_signals(plan, frames, Qualification(), spec)
    if len(signals):
        signals = signals.loc[signals.time.isin(windows)].copy()
    return (label_windows(windows, signals, coverage, spec), signals)
