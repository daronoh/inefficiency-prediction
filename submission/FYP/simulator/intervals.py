from __future__ import annotations
from FYP.config import SPEC

def split_monitoring_hours(spells, spec=SPEC):
    """An already-open gap is visible at selection; entries respect the window.

    Reservations in the scheduler persist between hours, so splitting is not a
    licence to reuse the same displayed liquidity.
    """
    rows = []
    for spell in spells:
        when = spell['start_time']
        while when < spell['end_time']:
            hour = when.floor(spec.frequency)
            end = min(spell['end_time'], hour + spec.horizon)
            rows.append({**spell, 'original_spell_start': spell['start_time'], 'start_time': when, 'end_time': end, 'time': hour, 'monitoring_end_time': min(spell['monitoring_end_time'], hour + spec.horizon), 'boundary_continuation': when != spell['start_time']})
            when = end
    return rows
