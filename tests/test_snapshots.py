import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from src.data.snapshots import add_hourly_history, sample_token, event_features
from src.config import shortlist_size, decision_window, require_split_access, for_window, spec_hash
from fixtures import _frame, BASE


class HourlyScreeningTests(unittest.TestCase):
    def test_explicit_monitoring_budget(self):
        self.assertEqual(shortlist_size(0, 5), 0)
        self.assertEqual(shortlist_size(3, 5), 3)
        self.assertEqual(shortlist_size(3000, 100), 100)
        for bad in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                shortlist_size(10, bad)

    def test_lags_do_not_cross_events_or_missing_hours(self):
        f = pd.DataFrame({'event_slug':['a','a','a','b'],
            'time':[BASE, BASE+pd.Timedelta(hours=1), BASE+pd.Timedelta(hours=3), BASE],
            'x_value':[1., 3., 8., 20.]})
        result = add_hourly_history(f)
        self.assertEqual(result.loc[1, 'x_snapshot_change1_value'], 2)
        self.assertTrue(pd.isna(result.loc[2, 'x_snapshot_lag1_value']))
        self.assertTrue(pd.isna(result.loc[3, 'x_snapshot_lag1_value']))

    def test_sampling_and_lags_follow_each_configured_window(self):
        raw = _frame([(0,[(.4,100)],[(.39,100)]),
            (10,[(.9,100)],[(.89,100)]), (8*3600,[(.4,100)],[(.39,100)])])
        for minutes in (15, 30, 60, 120):
            with self.subTest(minutes=minutes):
                spec = for_window(minutes)
                plan = dict(event_slug='test', stratum='ladder', paths={'a':Path('a'), 'b':Path('b')},
                    packages=[(['a','b'], BASE, BASE+pd.Timedelta(hours=9))])
                with patch('src.data.snapshots.pd.read_parquet', return_value=raw):
                    features = event_features(plan, spec)
                self.assertTrue(features.time.diff().dropna().eq(spec.horizon).all())
                lagged = add_hourly_history(features, spec)
                self.assertAlmostEqual(lagged.iloc[1].x_snapshot_lag1_best_quoted_net_usd, features.iloc[0].x_best_quoted_net_usd)
                missing_scan = add_hourly_history(features.drop(index=1), spec)
                self.assertTrue(pd.isna(missing_scan.iloc[1].x_snapshot_lag1_best_quoted_net_usd))

    def test_window_validation_and_provenance(self):
        self.assertNotEqual(spec_hash(for_window(15)), spec_hash(for_window(60)))
        for minutes in (0, -1, True, 2.5):
            with self.assertRaises(ValueError):
                for_window(minutes)
        with self.assertRaises(ValueError):
            replace(for_window(15), horizon_minutes=60)

    def test_small_positive_snapshot_margin_uses_configured_cost(self):
        raw = _frame([(0,[(.499,100)],[(.498,100)])])
        plan = dict(event_slug='test', stratum='ladder',
            paths={'a':Path('a'), 'b':Path('b')},
            packages=[(['a','b'], BASE, BASE+pd.Timedelta(hours=1))])
        with patch('src.data.snapshots.pd.read_parquet', return_value=raw):
            baseline = event_features(plan, for_window(60)).iloc[0]
            charged = event_features(plan, replace(for_window(60), assumed_other_cost_usd=.5)).iloc[0]
        self.assertAlmostEqual(baseline.x_best_quoted_net_usd, .2)
        self.assertAlmostEqual(charged.x_best_quoted_net_usd, -.3)
        self.assertEqual(baseline.x_positive_quoted_net_package_fraction, 1)
        self.assertEqual(charged.x_positive_quoted_net_package_fraction, 0)

    def test_hidden_intrahour_and_future_paths_cannot_change_predictors(self):
        hours = pd.date_range(BASE, periods=2, freq='h')
        a = _frame([(0,[(.4,100)],[(.39,100)]), (10,[(.9,100)],[(.89,100)]),
            (30,[(.4,100)],[(.39,100)]), (4000,[(.8,100)],[(.79,100)])])
        b = _frame([(0,[(.4,100)],[(.39,100)]), (10,[(.1,100)],[(.09,100)]),
            (30,[(.4,100)],[(.39,100)]), (4000,[(.2,100)],[(.19,100)])])
        pd.testing.assert_frame_equal(sample_token(a, hours), sample_token(b, hours))
        self.assertEqual(sample_token(a, hours).iloc[0].cost_10, 4.)

    def test_boundary_belongs_to_new_hour_and_lockbox_stays_closed(self):
        stamps = pd.Series([BASE, BASE-pd.Timedelta(nanoseconds=1)])
        self.assertEqual(decision_window(stamps).tolist(), [BASE, BASE-pd.Timedelta(hours=1)])
        with self.assertRaises(ValueError):
            require_split_access('lockbox')
