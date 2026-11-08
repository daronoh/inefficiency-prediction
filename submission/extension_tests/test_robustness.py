"""Information-availability regressions for the independent processed extension."""
import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ['FYP_ROOT'] = str(ROOT)

import numpy as np
import pandas as pd
from FYP.experiments.robustness_extension import HISTORY, historical_scores


class HistoricalAvailabilityTests(unittest.TestCase):
    def setUp(self):
        times = pd.date_range('2025-11-01', periods=6, freq='h', tz='UTC')
        self.timeline = pd.DataFrame(dict(event_slug=['fed-decision-test'] * 6,
            time=times, label_known=[True, True, False, True, True, True],
            target=[1., 0., np.nan, 1., 0., 1.], value_target=[8., 0., np.nan, 3., 0., 9.]))
        self.pool = self.timeline[['event_slug', 'time']].copy()

    def test_window_end_is_strictly_before_scan_and_zero_is_observation(self):
        scored, _ = historical_scores(self.timeline, self.pool)
        self.assertEqual(scored.event_history_count.tolist(), [0, 0, 1, 2, 2, 3])
        self.assertEqual(scored.history_last_known_value.tolist(), [0, 0, 8, 0, 0, 3])

    def test_future_label_perturbation_cannot_change_past_scores(self):
        original, _ = historical_scores(self.timeline, self.pool)
        changed = self.timeline.copy()
        changed.loc[changed.time.ge(changed.time.iloc[3]), 'target'] = 0
        changed.loc[changed.time.ge(changed.time.iloc[3]), 'value_target'] = 500
        perturbed, _ = historical_scores(changed, self.pool)
        pd.testing.assert_frame_equal(original.loc[:4, list(HISTORY)], perturbed.loc[:4, list(HISTORY)])

    def test_unknown_label_cannot_become_a_zero_observation(self):
        scored, _ = historical_scores(self.timeline, self.pool)
        changed = self.timeline.copy()
        changed.loc[2, ['target', 'value_target']] = [1, 999]
        perturbed, _ = historical_scores(changed, self.pool)
        pd.testing.assert_frame_equal(scored, perturbed)


if __name__ == '__main__':
    unittest.main()
