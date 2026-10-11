"""Uncapped depth optimization and discovery must not inherit the old cap."""
from dataclasses import replace
import unittest
import numpy as np
import pandas as pd
from fixtures import BASE, _frame
from src.config import SPEC
from src.data.books import RawGroup, RawBook, Ladder
from src.simulator.execution import experiment, run_scenario
from src.simulator.sizing import choose_order
from src.simulator.capacity import discover_depth_signals


def book(prices, sizes, minimum=5):
    return RawBook(BASE, Ladder([dict(price=p, size=s) for p,s in zip(prices,sizes)], True),
        Ladder([dict(price=.001, size=sum(sizes))], False), minimum, .001)


class CapacityTests(unittest.TestCase):
    def test_large_depth_without_enumerating_shares(self):
        books = [book([.4], [100_000_000]), book([.5], [200_000_000])]
        order = choose_order(books, max_quantity=None, cash_cap=None, min_quantity=5, min_limit_margin=0)
        self.assertEqual(order['quantity'], 100_000_000)
        self.assertAlmostEqual(order['quoted_gross_usd'], 10_000_000)

    def test_stop_before_unprofitable_marginal_depth(self):
        books = [book([.4,.7], [1234.5,10000]), book([.5], [20000])]
        order = choose_order(books, max_quantity=None, cash_cap=None, min_quantity=5, min_limit_margin=0)
        self.assertEqual(order['quantity'], 1234)

    def test_depth_endpoints_match_exhaustive_integer_search(self):
        rng = np.random.default_rng(23)
        for _ in range(250):
            books = [book(np.sort(rng.integers(10,800,6))/1000, rng.uniform(.1,100,6), rng.uniform(1,30)) for leg in range(2)]
            cash = None if rng.random() < .5 else float(rng.uniform(5,100))
            margin = None if rng.random() < .5 else float(rng.uniform(0,5))
            args = dict(cash_cap=cash, min_quantity=5, min_limit_margin=margin, min_gross=float(rng.uniform(0,2)))
            sparse = choose_order(books, max_quantity=None, **args)
            dense = choose_order(books, max_quantity=1000, **args)
            self.assertEqual(sparse is None, dense is None)
            if sparse:
                self.assertEqual(sparse['quantity'], dense['quantity'])
                self.assertAlmostEqual(sparse['quoted_gross_usd'], dense['quoted_gross_usd'])

    def test_discovery_accepts_minimum_above_old_cap_and_bounds_recordings(self):
        frames = {name:_frame([(0,[(price,5000)],[(price-.01,5000)]),
                              (2,[(price,5000)],[(price-.01,5000)])])
                  for name,price in [('a',.4),('b',.5)]}
        for frame in frames.values():
            frame['min_order_size'] = 2000.
        plan = dict(event_slug='synthetic', packages=[(('a','b'),BASE,BASE+pd.Timedelta(hours=1))])
        setup = replace(experiment(SPEC), max_quantity=None, cash_cap_usd=None, delay_s=.1)
        signals = discover_depth_signals(plan, frames, setup)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals.iloc[0].start_time, BASE)
        self.assertEqual(signals.iloc[0].end_time, BASE+pd.Timedelta(seconds=2))
        self.assertFalse(signals.iloc[0].start_observed)
        books = RawGroup(list(frames.values())).at(BASE)
        self.assertIsNone(choose_order(books, min_quantity=5, min_limit_margin=0))
        self.assertEqual(choose_order(books,max_quantity=None,cash_cap=None,min_quantity=5,min_limit_margin=0)['quantity'],5000)

    def test_tick_and_minimum_still_apply(self):
        books = [book([.4005], [10000]), book([.5], [10000])]
        self.assertIsNone(choose_order(books,max_quantity=None,cash_cap=None,min_quantity=5,min_limit_margin=0))

    def test_uncapped_order_keeps_prearrival_size_and_records_actual_failure(self):
        a = _frame([(0,[(.4,5000)],[(.39,5000)]),(2,[(.4,5000)],[(.39,5000)])])
        b = _frame([(0,[(.5,5000)],[(.49,5000)]),(.05,[(.5,4999)],[(.49,5000)]),
                    (2,[(.5,4999)],[(.49,5000)])])
        signal = pd.DataFrame([dict(event_slug='test',time=BASE,window='development',
            start_time=BASE,end_time=BASE+pd.Timedelta(seconds=2),monitoring_end_time=BASE+pd.Timedelta(hours=1))])
        setup = replace(experiment(SPEC),max_quantity=None,cash_cap_usd=None,delay_s=.1)
        row = run_scenario(signal,RawGroup([a,b]),setup).iloc[0]
        self.assertEqual(row.quantity,5000)
        self.assertEqual(row.status,'partial_unwound')
        self.assertAlmostEqual(row.pnl_usd,-50.)
        self.assertTrue(pd.isna(row.forced_failure_worst_pnl_usd))
