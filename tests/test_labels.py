"""Boundary and missing-evidence regressions for the recovered Dome hours."""
from dataclasses import replace
import unittest

import numpy as np
import pandas as pd

from src.data.coverage import classify_frame, observation_fraction
from src.simulator.discovery import discover_observed_package
from src.data.io import observation_groups
from src.simulator.execution import experiment
from src.data.labels import summarize_window
from src.simulator.scheduler import run_event
from src.config import for_window
from src.simulator.discovery import token_quotes, split_monitoring_hours
from fixtures import BASE, _frame


class ObservationLabelsTests(unittest.TestCase):
    def setUp(self):
        self.spec = replace(for_window(60), quantities=(5,))

    def plan(self, start=0, end=3600):
        return dict(event_slug='event', packages=[(['a', 'b'], BASE + pd.Timedelta(seconds=start),
                                                  BASE + pd.Timedelta(seconds=end))])

    def books(self, rows):
        return _frame([(t, [(ask, 10)], [(ask - .01, 10)]) for t, ask in rows])

    def discover(self, frames, plan):
        states = {t: classify_frame(f, self.spec) for t, f in frames.items()}
        quotes = {t: token_quotes(f, self.spec) for t, f in frames.items()}
        ids, start, end = plan['packages'][0]
        return discover_observed_package(ids, quotes, states, start, end, 'event', 'event:0', self.spec)

    def test_old_quotes_are_accepted_within_recording_extent(self):
        frames = {t: self.books([(0, .4), (7200, .4)]) for t in ['a', 'b']}
        group = observation_groups(self.plan(), frames)[0]['event:0']
        self.assertIsNotNone(group.at(BASE + pd.Timedelta(seconds=3599)))
        self.assertEqual(experiment(self.spec).freshness_s, float('inf'))
        states = {t: classify_frame(f) for t, f in frames.items()}
        self.assertEqual(observation_fraction(self.plan(), states, pd.DatetimeIndex([BASE]))[0], 1.)

    def test_explicit_blocker_can_rule_out_pair_with_unknown_other_leg(self):
        blocked = _frame([(0, [], []), (3600, [], [])])
        missing = self.books([(1800, .4), (3600, .4)])
        states = {'a': classify_frame(blocked), 'b': classify_frame(missing)}
        self.assertEqual(observation_fraction(self.plan(), states, pd.DatetimeIndex([BASE]))[0], 1.)

    def test_unknown_unblocked_pair_stays_unknown_even_with_another_good_pair(self):
        states = {'a': classify_frame(self.books([(0, .6), (3600, .6)])),
                  'b': classify_frame(self.books([(1800, .6), (3600, .6)]))}
        plan = self.plan()
        plan['packages'].append((['a', 'a'], BASE, BASE + pd.Timedelta(hours=1)))
        self.assertEqual(observation_fraction(plan, states, pd.DatetimeIndex([BASE]))[0], .5)

    def test_inactive_time_needs_no_books(self):
        states = {t: classify_frame(self.books([(600, .6), (1800, .6)])) for t in ['a', 'b']}
        self.assertEqual(observation_fraction(self.plan(600, 1800), states, pd.DatetimeIndex([BASE]))[0], 1.)

    def test_pre_activation_seed_is_detected_and_can_execute(self):
        plan = self.plan(600, 1800)
        frames = {t: self.books([(0, .4), (3600, .4)]) for t in ['a', 'b']}
        spells = self.discover(frames, plan)
        self.assertEqual(spells[0]['start_time'], BASE + pd.Timedelta(seconds=600))
        self.assertFalse(spells[0]['start_observed'])
        signals = pd.DataFrame(split_monitoring_hours(spells, self.spec))
        groups, mapping = observation_groups(plan, frames)
        details = run_event(signals, groups, mapping, experiment(self.spec))[0]
        self.assertGreater(details.iloc[0].pnl_usd, 0)

    def test_gap_after_missing_history_is_not_a_new_observed_onset(self):
        frames = {'a': self.books([(0, .4), (3600, .4)]),
                  'b': self.books([(100, .4), (3600, .4)])}
        spells = self.discover(frames, self.plan())
        self.assertFalse(spells[0]['start_observed'])
        frames['b'] = self.books([(0, .7), (100, .4), (3600, .4)])
        self.assertTrue(self.discover(frames, self.plan())[0]['start_observed'])

    def test_discovery_and_execution_stop_at_each_tokens_recording_end(self):
        frames = {'a': self.books([(0, .4), (100, .4)]),
                  'b': self.books([(0, .7), (200, .4), (3600, .4)])}
        self.assertEqual(self.discover(frames, self.plan()), [])
        group = observation_groups(self.plan(), frames)[0]['event:0']
        self.assertIsNone(group.at(BASE + pd.Timedelta(seconds=200)))

    def test_unknown_arrival_cannot_become_a_negative_training_label(self):
        frames = {t: self.books([(0, .7), (99.8, .4), (100, .4)]) for t in ['a', 'b']}
        spells = self.discover(frames, self.plan())
        signals = pd.DataFrame(split_monitoring_hours(spells, self.spec))
        groups, mapping = observation_groups(self.plan(), frames)
        details = run_event(signals, groups, mapping, experiment(self.spec))[0]
        stats = summarize_window(details, signals, self.spec)
        self.assertEqual(stats['unknown_attempts'], 1)
        self.assertTrue(np.isnan(details.iloc[0].pnl_usd))

    def test_window_length_remains_configurable(self):
        states = {t: classify_frame(self.books([(0, .6), (1800, .6)])) for t in ['a', 'b']}
        spec = for_window(15)
        windows = pd.DatetimeIndex([BASE, BASE + spec.horizon])
        np.testing.assert_equal(observation_fraction(self.plan(0, 1800), states, windows, spec), [1., 1.])

    def test_entry_arriving_after_rule_deadline_is_rejected(self):
        plan = self.plan(0, 1800)
        frames = {t: self.books([(0, .7), (1799.8, .4), (3600, .4)]) for t in ['a', 'b']}
        signals = pd.DataFrame(split_monitoring_hours(self.discover(frames, plan), self.spec))
        groups, mapping = observation_groups(plan, frames)
        details, audit = run_event(signals, groups, mapping, experiment(self.spec))
        self.assertTrue(details.empty)
        self.assertEqual(audit.reason.tolist(), ['outside_monitoring_window'])

    def test_already_submitted_partial_basket_can_unwind_after_rule_deadline(self):
        plan = self.plan(0, 1800)
        frames = {'a': self.books([(0, .4), (3600, .4)]),
                  'b': self.books([(0, .7), (1799, .4), (1799.5, .7), (3600, .7)])}
        signals = pd.DataFrame(split_monitoring_hours(self.discover(frames, plan), self.spec))
        groups, mapping = observation_groups(plan, frames)
        details, _ = run_event(signals, groups, mapping, experiment(self.spec))
        self.assertEqual(details.iloc[0].status, 'partial_unwound')
        self.assertGreater(details.iloc[0].unwind_time, plan['packages'][0][2])
        self.assertLess(details.iloc[0].pnl_usd, 0.)


if __name__ == '__main__':
    unittest.main()
