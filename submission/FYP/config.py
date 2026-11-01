"""Explicit data window and configurable, nonoverlapping screening horizon."""
from dataclasses import dataclass
import pandas as pd
@dataclass(frozen=True)
class ScreeningSpec:
    data_start: str = '2025-10-14T00:00:00Z'
    data_end: str = '2026-02-03T08:00:00Z'
    cadence_minutes: int = 60
    horizon_minutes: int = 60
    quantities: tuple | None = None
    unwind_delay_s: float = 1.
    assumed_other_cost_usd: float = 0.
    def __post_init__(self):
        if any(isinstance(n,bool) or not isinstance(n,int) or n<1 for n in (self.cadence_minutes,self.horizon_minutes)):
            raise ValueError('Cadence and horizon must be positive integer minutes')
        if self.cadence_minutes!=self.horizon_minutes:
            raise ValueError('Overlapping or gapped monitoring windows require a separate policy')
    @property
    def frequency(self):return f'{self.cadence_minutes}min'
    @property
    def horizon(self):return pd.Timedelta(minutes=self.horizon_minutes)
SPEC=ScreeningSpec()
