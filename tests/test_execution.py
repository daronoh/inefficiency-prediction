"""Synthetic causal-entry and frozen-limit execution-plausibility checks."""

from __future__ import annotations

import json
import unittest

import numpy as np
import pandas as pd

from src.data.books import RawGroup
from src.simulator.execution import Experiment, run_scenario
from src.config import decision_window


BASE = pd.Timestamp("2025-11-01T12:00:00Z")


def _time(seconds: float) -> pd.Timestamp:
    return BASE + pd.Timedelta(seconds=seconds)


def _side(levels: list[tuple[float, float]]) -> str:
    return json.dumps([{"price": str(price), "size": str(size)} for price, size in levels])


def _frame(rows: list[tuple]) -> pd.DataFrame:
    """Rows contain seconds after BASE, full ask levels, and full bid levels."""
    return pd.DataFrame(
        [
            {
                "snapshot_time": _time(seconds),
                "indexed_at_ms": int(_time(seconds).timestamp() * 1000),
                "asks_json": _side(asks),
                "bids_json": _side(bids),
                "min_order_size": 5.0,
                "tick_size": 0.001,
            }
            for seconds, asks, bids in rows
        ]
    )


def _group(*members: list[tuple]) -> RawGroup:
    return RawGroup([_frame(rows) for rows in members])


def _signals(intervals: list[tuple[float, float]]) -> pd.DataFrame:
    starts = pd.Series([_time(start) for start, _ in intervals])
    return pd.DataFrame(
        {
            "event_slug": "synthetic",
            "window": "development_test",
            "time": decision_window(starts),
            "start_time": starts,
            "end_time": [_time(end) for _, end in intervals],
        }
    )


class ImmediateEntryTests(unittest.TestCase):
    def test_zero_duration_flicker_is_attempted_and_can_end_in_no_fill(self) -> None:
        group = _group(
            [(1, [(0.4, 100)], [(0.39, 100)]),
             (1.2, [(0.6, 100)], [(0.59, 100)]),
             (10, [(0.6, 100)], [(0.59, 100)])],
            [(1, [(0.5, 100)], [(0.49, 100)]),
             (1.2, [(0.6, 100)], [(0.59, 100)]),
             (10, [(0.6, 100)], [(0.59, 100)])],
        )

        result = run_scenario(_signals([(1, 1)]), group, Experiment(wait_s=0))

        self.assertTrue(result.attempted.iloc[0])
        self.assertEqual(result.status.iloc[0], "no_fill")
        self.assertEqual(result.n_filled_legs.iloc[0], 0)
        self.assertEqual(result.pnl_usd.iloc[0], 0.0)

    def test_complete_basket_loss_is_not_filtered_after_gap_disappears(self) -> None:
        initial = [(0.2, 90), (0.6, 10)]
        member = [
            (1, initial, [(0.19, 100)]),
            (1.2, [(0.6, 100)], [(0.59, 100)]),
            (10, [(0.6, 100)], [(0.59, 100)]),
        ]

        result = run_scenario(
            _signals([(1, 1.2)]), _group(member, member), Experiment(wait_s=0)
        )

        self.assertEqual(result.status.iloc[0], "complete")
        self.assertEqual(result.n_filled_legs.iloc[0], 2)
        self.assertAlmostEqual(result.quoted_gross_usd.iloc[0], 52.0)
        self.assertAlmostEqual(result.entry_cost_usd.iloc[0], 120.0)
        self.assertAlmostEqual(result.pnl_usd.iloc[0], -20.0)
        np.testing.assert_allclose(json.loads(result.limits_json.iloc[0]), [0.6, 0.6])

    def test_five_second_confirmation_does_not_bridge_an_interruption(self) -> None:
        member0 = [
            (1, [(0.4, 100)], [(0.39, 100)]),
            (4, [(0.7, 100)], [(0.69, 100)]),
            (5, [(0.4, 100)], [(0.39, 100)]),
            (20, [(0.7, 100)], [(0.69, 100)]),
        ]
        member1 = [(1, [(0.5, 100)], [(0.49, 100)]),
                   (20, [(0.5, 100)], [(0.49, 100)])]

        result = run_scenario(
            _signals([(1, 4), (5, 20)]), _group(member0, member1), Experiment(wait_s=5)
        )

        self.assertEqual(result.status.tolist(), ["not_confirmed", "complete"])
        self.assertEqual(result.attempted.tolist(), [False, True])

    def test_arrival_outside_original_hour_is_not_submitted(self) -> None:
        member = [(3599.8, [(0.4, 100)], [(0.39, 100)]),
                  (3610, [(0.4, 100)], [(0.39, 100)])]

        result = run_scenario(
            _signals([(3599.8, 3610)]), _group(member, member), Experiment(wait_s=0)
        )

        self.assertEqual(result.status.iloc[0], "outside_monitoring_window")
        self.assertFalse(result.attempted.iloc[0])


class FrozenSizingTests(unittest.TestCase):
    def test_adaptive_quantity_and_limits_remain_frozen_at_arrival(self) -> None:
        member = [
            (1, [(0.2, 100), (0.4, 100)], [(0.19, 1000)]),
            (1.2, [(0.1, 1000)], [(0.09, 1000)]),
            (10, [(0.1, 1000)], [(0.09, 1000)]),
        ]

        result = run_scenario(
            _signals([(1, 10)]), _group(member, member),
            Experiment(wait_s=0, sizing="adaptive"),
        )

        self.assertEqual(result.status.iloc[0], "complete")
        self.assertEqual(result.quantity.iloc[0], 200)
        np.testing.assert_allclose(json.loads(result.limits_json.iloc[0]), [0.4, 0.4])
        self.assertAlmostEqual(result.quoted_gross_usd.iloc[0], 80.0)
        self.assertAlmostEqual(result.entry_cost_usd.iloc[0], 40.0)
        self.assertAlmostEqual(result.pnl_usd.iloc[0], 160.0)

    def test_partial_unwind_walks_bid_depth_at_actual_adaptive_quantity(self) -> None:
        member0 = [
            (1, [(0.2, 100), (0.4, 100)], [(0.19, 1000)]),
            (1.2, [(0.3, 200)], [(0.29, 200)]),
            (2.5, [(0.3, 200)], [(0.1, 100), (0.05, 100)]),
            (10, [(0.3, 200)], [(0.1, 100), (0.05, 100)]),
        ]
        member1 = [
            (1, [(0.2, 100), (0.4, 100)], [(0.19, 1000)]),
            (1.2, [(0.5, 200)], [(0.49, 200)]),
            (10, [(0.5, 200)], [(0.49, 200)]),
        ]

        result = run_scenario(
            _signals([(1, 1.2)]), _group(member0, member1),
            Experiment(wait_s=0, sizing="adaptive"),
        )

        self.assertEqual(result.status.iloc[0], "partial_unwound")
        self.assertEqual(result.quantity.iloc[0], 200)
        self.assertEqual(result.n_filled_legs.iloc[0], 1)
        self.assertAlmostEqual(result.entry_cost_usd.iloc[0], 60.0)
        self.assertAlmostEqual(result.pnl_usd.iloc[0], -45.0)

    def test_minimum_gross_is_decided_before_favourable_arrival_move(self) -> None:
        member = [
            (1, [(0.49, 100)], [(0.48, 100)]),
            (1.2, [(0.1, 100)], [(0.09, 100)]),
            (10, [(0.1, 100)], [(0.09, 100)]),
        ]

        result = run_scenario(
            _signals([(1, 10)]), _group(member, member),
            Experiment(wait_s=0, min_entry_gross_usd=3.0),
        )

        self.assertEqual(result.status.iloc[0], "no_acceptable_size")
        self.assertFalse(result.attempted.iloc[0])

    def test_cash_cap_uses_frozen_limits_not_cheaper_displayed_average(self) -> None:
        member = [(1, [(0.2, 90), (0.6, 10)], [(0.19, 100)]),
                  (10, [(0.2, 90), (0.6, 10)], [(0.19, 100)])]

        result = run_scenario(
            _signals([(1, 10)]), _group(member, member),
            Experiment(wait_s=0, cash_cap_usd=100.0),
        )

        # Displayed cost is only $48, but submitted limits commit up to $120.
        self.assertEqual(result.status.iloc[0], "no_acceptable_size")
        self.assertFalse(result.attempted.iloc[0])


class UnknownAndCausalityTests(unittest.TestCase):
    def test_unobserved_arrival_is_unknown_not_zero_pnl(self) -> None:
        member = [(1, [(0.4, 100)], [(0.39, 100)])]

        result = run_scenario(
            _signals([(1, 1)]), _group(member, member), Experiment(wait_s=0)
        )

        self.assertTrue(result.attempted.iloc[0])
        self.assertEqual(result.status.iloc[0], "unknown_arrival")
        self.assertTrue(np.isnan(result.pnl_usd.iloc[0]))

    def test_stale_arrival_is_unknown_not_a_known_failed_order(self) -> None:
        member = [(1, [(0.4, 100)], [(0.39, 100)]),
                  (10, [(0.4, 100)], [(0.39, 100)])]

        result = run_scenario(
            _signals([(1, 10)]), _group(member, member),
            Experiment(wait_s=0, freshness_s=0.1),
        )

        self.assertTrue(result.attempted.iloc[0])
        self.assertEqual(result.status.iloc[0], "unknown_arrival")
        self.assertTrue(np.isnan(result.pnl_usd.iloc[0]))

    def test_unobserved_partial_unwind_is_unknown_not_free_liquidation(self) -> None:
        member0 = [(1, [(0.4, 100)], [(0.39, 100)]),
                   (2, [(0.4, 100)], [(0.39, 100)])]
        member1 = [(1, [(0.5, 100)], [(0.49, 100)]),
                   (1.2, [(0.6, 100)], [(0.59, 100)]),
                   (2, [(0.6, 100)], [(0.59, 100)])]

        result = run_scenario(
            _signals([(1, 1.2)]), _group(member0, member1), Experiment(wait_s=0)
        )

        self.assertTrue(result.attempted.iloc[0])
        self.assertEqual(result.n_filled_legs.iloc[0], 1)
        self.assertEqual(result.status.iloc[0], "partial_unknown_unwind")
        self.assertTrue(np.isnan(result.pnl_usd.iloc[0]))

    def test_in_flight_skip_then_reservation_uses_arrival_quote_versions(self) -> None:
        member0 = [
            (1, [(0.4, 100)], [(0.39, 100)]),
            (1.1, [(0.7, 100)], [(0.69, 100)]),
            (1.2, [(0.39, 100)], [(0.38, 100)]),
            (2.8, [(0.7, 100)], [(0.69, 100)]),
            (3, [(0.38, 100)], [(0.37, 100)]),
            (10, [(0.38, 100)], [(0.37, 100)]),
        ]
        member1 = [(1, [(0.5, 100)], [(0.49, 100)]),
                   (1.4, [(0.49, 100)], [(0.48, 100)]),
                   (10, [(0.49, 100)], [(0.48, 100)])]

        result = run_scenario(
            _signals([(1, 1.1), (1.2, 2.8), (3, 10)]),
            _group(member0, member1), Experiment(wait_s=0),
        )

        self.assertEqual(
            result.status.tolist(), ["complete", "in_flight", "quote_reuse_blocked"]
        )
        self.assertEqual(result.attempted.tolist(), [True, False, False])

    def test_future_quotes_cannot_change_earlier_order_or_pnl(self) -> None:
        initial = (1, [(0.4, 100)], [(0.39, 100)])
        original = [initial, (20, [(0.4, 100)], [(0.39, 100)])]
        perturbed = [initial, (20, [(0.8, 1000)], [(0.79, 1000)])]
        signals = _signals([(1, 30)])

        results = [
            run_scenario(signals, _group(member, member), Experiment(wait_s=0))
            for member in (original, perturbed)
        ]

        pd.testing.assert_frame_equal(results[0], results[1])


if __name__ == "__main__":
    unittest.main()
