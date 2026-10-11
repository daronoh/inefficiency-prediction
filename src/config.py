"""Periodic snapshot screening; 60 minutes is the default research window."""
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import math
import pandas as pd


@dataclass(frozen=True)
class ScreeningSpec:
    version: str = 'periodic_snapshot_screening_v5'
    development_start: str = '2025-10-14T00:00:00Z'
    lockbox_start: str = '2026-01-08T00:00:00Z'
    cadence_minutes: int = 60
    horizon_minutes: int = 60
    quantities: tuple[int, ...] = (5, 10, 25, 50, 100, 250, 500, 1000)
    assumed_other_cost_usd: float = 0.0
    cash_cap_usd: float = 1000.0
    arrival_delay_s: float = 0.5
    confirmation_s: float = 0.0
    unwind_delay_s: float = 1.0
    primary_clock: str = 'snapshot_time'

    def __post_init__(self):
        for value in (self.cadence_minutes, self.horizon_minutes):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError('Cadence and horizon must be positive integer minutes')
        if self.cadence_minutes != self.horizon_minutes:
            raise ValueError('Current replay requires equal cadence and horizon to preserve one top-N budget; overlapping/gapped policies need a separate design')
        for value in (self.assumed_other_cost_usd, self.confirmation_s, self.arrival_delay_s, self.unwind_delay_s):
            if isinstance(value, bool) or not math.isfinite(value) or value < 0:
                raise ValueError('Costs and delays must be finite and nonnegative')

    @property
    def frequency(self):
        return f'{self.cadence_minutes}min'

    @property
    def horizon(self):
        return pd.Timedelta(minutes=self.horizon_minutes)


# Preserve the original scheduled-input contract for historical reproducibility.
# Current profitability execution overrides are explicit in profit_setup()
# (src/data/profitability.py) and saved in each dataset's execution manifest.
SPEC = ScreeningSpec()


def for_window(window_minutes=60):
    return replace(SPEC, cadence_minutes=window_minutes, horizon_minutes=window_minutes)


def spec_hash(spec=SPEC):
    return hashlib.sha256(json.dumps(asdict(spec), sort_keys=True).encode()).hexdigest()


def require_split_access(split):
    if split != 'development':
        raise ValueError('Active preparation is development-only. Final evaluation is not implemented or authorised here.')


def decision_window(starts, spec=SPEC):
    """UTC-aligned half-open windows; a boundary belongs to the new window."""
    return pd.to_datetime(starts, utc=True).dt.floor(spec.frequency)


def shortlist_size(n, top_n):
    if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n < 1 or n < 0:
        raise ValueError('A positive integer top_n and nonnegative population are required')
    return min(n, top_n)
