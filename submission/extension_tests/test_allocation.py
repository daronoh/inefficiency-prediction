"""Budget and information-availability regressions for saved-score allocation."""
import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ['FYP_ROOT'] = str(ROOT)

import pandas as pd
from FYP.experiments.allocation_extension import resource_select, sequential_selection, choose_cases


class ResourceTests(unittest.TestCase):
    def test_skip_expensive_event_and_never_exceed_budget(self):
        pool = pd.DataFrame(dict(event_slug=['a', 'b', 'c', 'd'],
            time=[pd.Timestamp('2025-11-11', tz='UTC')] * 4,
            score=[4., 3., 2., 1.], x_active_tokens=[8, 5, 2, 2]))
        selected, audit = resource_select(pool, 7)
        self.assertEqual(selected.event_slug.tolist(), ['b', 'c'])
        self.assertEqual(audit.used_tokens.tolist(), [7])

    def test_unknown_outcome_still_consumes_resources(self):
        pool = pd.DataFrame(dict(event_slug=['a', 'b'], time=[pd.Timestamp('2025-11-11', tz='UTC')] * 2,
            score=[2., 1.], x_active_tokens=[5, 5], label_known=[False, True], target=[None, 1.]))
        selected, _ = resource_select(pool, 5)
        changed = pool.assign(label_known=[True, False], target=[1., None])
        other, _ = resource_select(changed, 5)
        self.assertEqual(selected.event_slug.tolist(), ['a'])
        self.assertEqual(selected.event_slug.tolist(), other.event_slug.tolist())


class SelectorTests(unittest.TestCase):
    def setUp(self):
        self.first = pd.Timestamp('2025-11-11', tz='UTC')
        self.origin = self.first + pd.Timedelta(days=7)
        rows = []
        for time, values in [(self.first, [10., 0.]),
                             (self.origin - pd.Timedelta(hours=1), [0., 1000.]),
                             (self.origin, [0., 5000.])]:
            for model, scores in [('a', [2., 1.]), ('b', [1., 2.])]:
                for event, value, score in zip(['x', 'y'], values, scores):
                    rows.append(dict(event_slug=event, time=time, model=model, score=score,
                                     label_known=True, target=float(value > 0), value_target=value))
        self.frame = pd.DataFrame(rows)

    def run_selector(self, frame):
        return sequential_selection(frame, policies=['a', 'b'], first=self.first,
                                    end=self.origin + pd.Timedelta(hours=1), warmup_blocks=1, n=1)

    def test_label_ending_exactly_at_origin_is_unavailable(self):
        _, audit, history = self.run_selector(self.frame)
        self.assertEqual(audit.chosen_policy.tolist(), ['a'])
        self.assertTrue((history.maximum_history_label_end < history.origin).all())

    def test_changing_evaluation_targets_cannot_change_policy_choice(self):
        _, before, _ = self.run_selector(self.frame)
        changed = self.frame.copy()
        changed.loc[changed.time.ge(self.origin), 'value_target'] = 1e12
        changed.loc[changed.time.ge(self.origin), 'target'] = 1.
        _, after, _ = self.run_selector(changed)
        self.assertEqual(before.chosen_policy.tolist(), after.chosen_policy.tolist())


class CaseTests(unittest.TestCase):
    def test_known_positives_only_and_cases_are_disjoint(self):
        frame = pd.DataFrame(dict(event_slug=['unknown', 'a', 'b', 'c'],
            time=pd.date_range('2025-11-11', periods=4, freq='h', tz='UTC'),
            label_known=[False, True, True, True], target=[1., 1., 1., 1.],
            value_target=[999., 2., 3., 1.], forest_selected=[True, True, False, False],
            history_selected=[False, False, True, False]))
        cases, support = choose_cases(frame)
        self.assertEqual(cases.event_slug.tolist(), ['a', 'b', 'c'])
        self.assertEqual(cases.case_group.tolist(), ['forest_only', 'history_only', 'neither'])
        self.assertEqual(support.positive_hours.sum(), 3)


if __name__ == '__main__':
    unittest.main()
