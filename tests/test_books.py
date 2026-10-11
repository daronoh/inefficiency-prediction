"""Synthetic depth, causal as-of and order-sizing checks; no raw data needed."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.data.books import Ladder, RawBook, RawGroup
from src.simulator.sizing import choose_order
from src.config import SPEC


BASE = pd.Timestamp("2025-11-01T12:00:00Z")


def levels(*rows):
    return [{"price": price, "size": size} for price, size in rows]


def book(asks, *, bid=0.1, minimum=5, tick=0.01):
    return RawBook(BASE, Ladder(asks, True), Ladder(levels((bid, 2000)), False), minimum, tick)


def frame(seconds=(0, 10), prices=(0.2, 0.3), indexed=None):
    return pd.DataFrame({
        "snapshot_time": [BASE + pd.Timedelta(seconds=s) for s in seconds],
        "indexed_at_ms": indexed if indexed is not None else range(len(seconds)),
        "asks_json": [json.dumps(levels((price, 1000))) for price in prices],
        "bids_json": [json.dumps(levels((0.1, 1000)))] * len(seconds),
        "min_order_size": [5] * len(seconds),
        "tick_size": [0.01] * len(seconds),
    })


class LadderTests(unittest.TestCase):
    def test_walks_unsorted_depth_and_returns_total_not_vwap(self):
        asks = Ladder(levels((0.6, 50), (0.4, 100), (0.1, 0)), True)
        self.assertEqual(asks.quote(125), (55.0, 0.6))
        self.assertEqual(asks.quote(100), (40.0, 0.4))
        self.assertEqual(asks.quote(100.5), (40.3, 0.6))
        self.assertIsNone(asks.quote(151))
        self.assertIsNone(asks.quote(125, limit=0.59))
        self.assertEqual(asks.quote(125, limit=0.6), (55.0, 0.6))

    def test_sell_limit_has_opposite_direction_and_no_partial_fills(self):
        bids = Ladder(levels((0.3, 100), (0.5, 100)), False)
        self.assertEqual(bids.quote(150), (65.0, 0.3))
        self.assertIsNone(bids.quote(150, limit=0.4))
        self.assertEqual(bids.quote(100, limit=0.5), (50.0, 0.5))
        self.assertIsNone(Ladder([], False).quote(100))

    def test_invalid_levels_and_requests_are_rejected(self):
        for price, size in [(-0.1, 1), (1.1, 1), (np.nan, 1), (0.5, -1), (0.5, np.inf)]:
            with self.subTest(price=price, size=size), self.assertRaises(ValueError):
                Ladder(levels((price, size)), True)
        ladder = Ladder(levels((0.4, 100)), True)
        for q in [0, -1, np.nan, np.inf]:
            with self.subTest(q=q), self.assertRaises(ValueError):
                ladder.quote(q)
        with self.assertRaises(ValueError):
            ladder.quote(100, limit=np.nan)

    def test_book_cleanliness_includes_both_sides_and_allows_locked(self):
        self.assertTrue(book(levels((0.1, 100)), bid=0.1).clean)
        self.assertFalse(book(levels((0.1, 100)), bid=0.2).clean)
        self.assertFalse(book([]).clean)


class RawGroupTests(unittest.TestCase):
    def test_backward_only_queries_and_group_union_extent(self):
        group = RawGroup([frame(), frame(seconds=(5, 20))])
        self.assertIsNone(group.at(BASE))
        at_nine = group.at(BASE + pd.Timedelta(seconds=9))
        self.assertEqual([b.asks.quote(100)[0] for b in at_nine], [20, 20])
        self.assertEqual([b.stamp for b in at_nine], [BASE, BASE + pd.Timedelta(seconds=5)])
        at_twenty = group.at(BASE + pd.Timedelta(seconds=20))
        self.assertEqual(at_twenty[0].stamp, BASE + pd.Timedelta(seconds=10))
        self.assertIsNone(group.at(BASE + pd.Timedelta(seconds=20, microseconds=1)))
        with self.assertRaises(ValueError):
            group.at(BASE.tz_localize(None))

    def test_latest_index_dedup_empty_member_and_lockbox_cutoff(self):
        duplicates = frame(seconds=(10, 0, 0), prices=(0.4, 0.2, 0.3), indexed=[30, 20, 10])
        group = RawGroup([duplicates, frame()])
        self.assertEqual(group.at(BASE)[0].asks.quote(100)[0], 20)
        self.assertIsNone(RawGroup([frame().iloc[:0], frame()]).at(BASE))
        future = frame(seconds=(0,), prices=(0.9,))
        future["snapshot_time"] = pd.Timestamp(SPEC.lockbox_start)
        bounded = RawGroup([pd.concat([frame(), future]), frame()])
        self.assertEqual(bounded.last_time, BASE + pd.Timedelta(seconds=10))
        self.assertIsNone(bounded.at(pd.Timestamp(SPEC.lockbox_start)))

    def test_parse_cache_is_lazy_bounded_and_reused(self):
        group = RawGroup([frame(seconds=(0, 1, 2), prices=(0.2, 0.3, 0.4))])
        group.CACHE_LIMIT = 2
        self.assertEqual(len(group._cache), 0)
        first = group.at(BASE)[0]
        self.assertIs(group.at(BASE)[0], first)
        group.at(BASE + pd.Timedelta(seconds=1))
        group.at(BASE + pd.Timedelta(seconds=2))
        self.assertEqual(len(group._cache), 2)
        self.assertNotIn((0, 0), group._cache)

    def test_loader_uses_explicit_development_predicates_and_member_order(self):
        members = pd.DataFrame({"yes_token_id": ["200", "100"]})
        with patch("src.data.books.pd.read_parquet", side_effect=[frame(), frame()]) as read:
            group = RawGroup.from_members(members)
        self.assertEqual([path.name for path in group.paths], ["ob_200.parquet", "ob_100.parquet"])
        for call in read.call_args_list:
            self.assertEqual(call.kwargs["columns"], RawGroup.COLUMNS)
            self.assertEqual(call.kwargs["filters"], [
                ("snapshot_time", ">=", pd.Timestamp(SPEC.development_start)),
                ("snapshot_time", "<", pd.Timestamp(SPEC.lockbox_start)),
            ])
        with self.assertRaises(ValueError):
            RawGroup.from_members(pd.DataFrame({"yes_token_id": ["../not_a_token"]}))


class SizingTests(unittest.TestCase):
    def test_nonlinear_depth_finds_interior_maximum_not_largest_size(self):
        books = [book(levels((0.3, 200), (0.6, 800))), book(levels((0.5, 1000)))]
        order = choose_order(books)
        self.assertEqual(order["quantity"], 200)
        np.testing.assert_allclose(order["ask_costs"], [60, 100])
        np.testing.assert_allclose(order["limits"], [0.3, 0.5])
        self.assertAlmostEqual(order["quoted_gross_usd"], 40)
        self.assertAlmostEqual(order["reserved_cash_usd"], 160)
        self.assertEqual(choose_order(books, "fixed")["quantity"], 100)

    def test_cash_reserves_use_limits_not_cheaper_depth_costs(self):
        books = [book(levels((0.1, 100), (0.4, 900)), bid=0.05), book(levels((0.4, 1000)))]
        order = choose_order(books, cash_cap=100)
        self.assertEqual(order["quantity"], 125)
        self.assertAlmostEqual(sum(order["ask_costs"]), 70)
        self.assertAlmostEqual(order["reserved_cash_usd"], 100)
        self.assertIsNone(choose_order(books, "fixed", cash_cap=49))

    def test_minimum_size_tick_metadata_and_gross_threshold(self):
        books = [book(levels((0.3, 1000)), minimum=250.5), book(levels((0.6, 1000)))]
        self.assertIsNone(choose_order(books, "fixed"))
        self.assertIsNone(choose_order(books, max_quantity=250))
        self.assertEqual(choose_order(books, max_quantity=251)["quantity"], 251)
        self.assertIsNone(choose_order(books, min_gross=101))
        for minimum, tick in [(np.nan, 0.01), (5, np.nan), (0, 0.01), (5, 0)]:
            changed = [book(levels((0.3, 1000)), minimum=minimum, tick=tick), books[1]]
            self.assertIsNone(choose_order(changed))
        off_tick = [book(levels((0.305, 1000))), books[1]]
        self.assertIsNone(choose_order(off_tick))

    def test_tied_gross_chooses_less_quantity_and_frozen_limits_reject_arrival(self):
        books = [book(levels((0.3, 100), (0.4, 900))), book(levels((0.6, 1000)))]
        order = choose_order(books)
        self.assertEqual(order["quantity"], 100)
        arrival = Ladder(levels((0.3, 99), (0.31, 1000)), True)
        self.assertIsNone(arrival.quote(order["quantity"], order["limits"][0]))
        self.assertIsNotNone(arrival.quote(order["quantity"]))

    def test_no_profit_incomplete_depth_and_bad_configuration(self):
        self.assertIsNone(choose_order([book(levels((0.5, 1000))), book(levels((0.5, 1000)))]))
        self.assertIsNone(choose_order([book(levels((0.3, 99))), book(levels((0.4, 1000)))]))
        self.assertIsNone(choose_order([], "fixed"))
        for kwargs in [{"mode": "unknown"}, {"max_quantity": 100.5}, {"min_gross": -1}, {"cash_cap": 0}]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                choose_order([], **kwargs)


if __name__ == "__main__":
    unittest.main()
