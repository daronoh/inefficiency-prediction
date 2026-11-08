"""Temporal boundary checks for the additional rolling-window experiment."""
import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault('FYP_ROOT', str(ROOT))

import pandas as pd
from FYP.experiments.sensitivity_extension import training_rows


class RollingAvailabilityTests(unittest.TestCase):
    def test_window_and_label_boundaries(self):
        origin = pd.Timestamp('2026-01-01', tz='UTC')
        times = [origin-pd.Timedelta(days=14, hours=1), origin-pd.Timedelta(days=14),
                 origin-pd.Timedelta(hours=2), origin-pd.Timedelta(hours=1), origin,
                 origin-pd.Timedelta(hours=3)]
        frame = pd.DataFrame(dict(time=times, label_known=[True]*5+[False], row=range(6)))
        self.assertEqual(training_rows(frame, origin, 14).row.tolist(), [1, 2])

    def test_longer_window_only_adds_earlier_available_rows(self):
        origin = pd.Timestamp('2026-01-01', tz='UTC')
        frame = pd.DataFrame(dict(time=pd.date_range(origin-pd.Timedelta(days=30), origin,
                                                    freq='h'), label_known=True))
        short, long = [training_rows(frame, origin, days) for days in (14,28)]
        self.assertTrue(set(short.time).issubset(set(long.time)))
        self.assertTrue((long.time+pd.Timedelta(hours=1)).lt(origin).all())
        self.assertTrue(long.time.ge(origin-pd.Timedelta(days=28)).all())


if __name__ == '__main__':
    unittest.main()
