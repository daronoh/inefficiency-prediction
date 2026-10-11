import unittest
import json
import numpy as np
import pandas as pd
from src.data.coverage import classify_book, package_intervals, interval_fraction, UNKNOWN, BLOCKED, TRADABLE
from src.simulator.discovery import token_quotes
from fixtures import BASE, _frame


class ResearchTests(unittest.TestCase):
    def test_empty_is_observed_but_missing_is_unknown(self):
        self.assertEqual(classify_book('[]', '[]', 5., .01)[1:], (BLOCKED, 'empty_asks'))
        self.assertEqual(classify_book(None, '[]', 5., .01)[1:], (UNKNOWN, 'missing_or_malformed_book'))
        self.assertEqual(classify_book('null', '[]', 5., .01)[1], UNKNOWN)


    def test_crossed_and_invalid_metadata_stay_unknown(self):
        asks = json.dumps([dict(price='.4', size='10')])
        bids = json.dumps([dict(price='.5', size='10')])
        self.assertEqual(classify_book(asks, bids, 5., .01)[1:], (UNKNOWN, 'crossed_book'))
        self.assertEqual(classify_book(asks, bids, np.nan, .01)[1], UNKNOWN)


    def test_feasibility_matches_discovery_grid(self):
        rng = np.random.default_rng(123)
        rows = []
        for i in range(80):
            asks = [(p, float(rng.integers(0, 30))) for p in sorted(rng.choice(np.arange(1, 100) / 100, size=5))]
            bids = [(.01, 25)] if i % 7 else []
            rows.append((i, asks if i % 9 else [], bids))
        frame = _frame(rows)
        frame.loc[frame.index % 3 == 0, 'min_order_size'] = 12.
        frame.loc[frame.index % 4 == 0, 'tick_size'] = .03
        expected = token_quotes(frame).filter(like='cost_').notna().any(axis=1).to_numpy()
        actual = [classify_book(r.asks_json, r.bids_json, r.min_order_size, r.tick_size)[0] for r in frame.itertuples()]
        np.testing.assert_equal(actual, expected)


    def states(self, values):
        return pd.DataFrame(dict(time=[BASE + pd.Timedelta(seconds=s) for s, v in values],
            state=[v for s, v in values], size_qualified=[v == TRADABLE for s, v in values],
            reason=['empty_asks' if v == BLOCKED else '' for s, v in values]))


    def test_blocked_leg_proves_no_entry_even_if_other_leg_unknown(self):
        blocked = self.states([(0, BLOCKED), (3600, BLOCKED)])
        missing = pd.DataFrame(columns=blocked.columns)
        unknown, strict, blocked_intervals = package_intervals([blocked, missing], BASE, BASE + pd.Timedelta(hours=1))
        self.assertEqual(unknown, [])
        self.assertEqual(interval_fraction(strict, pd.DatetimeIndex([BASE]), pd.Timedelta(hours=1))[0], 1.)
        self.assertEqual(interval_fraction(blocked_intervals, pd.DatetimeIndex([BASE]), pd.Timedelta(hours=1))[0], 1.)


    def test_one_known_pair_does_not_clear_another_unknown_pair(self):
        valid = self.states([(0, TRADABLE), (3600, TRADABLE)])
        missing = pd.DataFrame(columns=valid.columns)
        known_pair = package_intervals([valid, valid], BASE, BASE + pd.Timedelta(hours=1))[0]
        unknown_pair = package_intervals([valid, missing], BASE, BASE + pd.Timedelta(hours=1))[0]
        self.assertEqual(interval_fraction(known_pair + unknown_pair, pd.DatetimeIndex([BASE]), pd.Timedelta(hours=1))[0], 1.)


    def test_recording_end_is_not_extended_or_filled_from_future(self):
        states = self.states([(600, TRADABLE), (1800, TRADABLE)])
        unknown, _, _ = package_intervals([states], BASE, BASE + pd.Timedelta(hours=1))
        self.assertAlmostEqual(interval_fraction(unknown, pd.DatetimeIndex([BASE]), pd.Timedelta(hours=1))[0], 2 / 3)


    def test_rule_deadline_does_not_create_missing_data_after_trading_ends(self):
        states = self.states([(0, TRADABLE), (1800, TRADABLE)])
        unknown, _, _ = package_intervals([states], BASE, BASE + pd.Timedelta(minutes=30))
        self.assertEqual(unknown, [])
        self.assertEqual(1 - interval_fraction(unknown, pd.DatetimeIndex([BASE]), pd.Timedelta(hours=1))[0], 1.)


