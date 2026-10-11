import unittest
import pandas as pd
from src.data.books import RawGroup
from src.simulator.execution import Experiment
from src.data.rules import broad_first
from src.simulator.scheduler import run_event
from fixtures import _frame, _time, BASE

def book(price, later=()):
    return _frame([(0, [(price, 100)], [(price - .01, 100)]), *later,
                   (20, [(price, 100)], [(price - .01, 100)])])


def signal(pid, second=0):
    return dict(pair_id=pid, event_slug="same_event", start_time=_time(second), end_time=_time(15),
                time=BASE, window="test", monitoring_end_time=_time(21))


class SchedulerTests(unittest.TestCase):
    def test_threshold_and_deadline_direction(self):
        self.assertTrue(broad_first("threshold", 2, 6))
        self.assertTrue(broad_first("reach", 115, 120))
        self.assertTrue(broad_first("dip", 100, 90))
        self.assertTrue(broad_first("deadline", 15, 7))
        self.assertFalse(broad_first("dip", 90, 100))
        with self.assertRaises(ValueError):
            broad_first("reach", 100, 100)


    def test_same_time_rank_uses_entry_not_future_fill(self):
        b = book(.5)
        good_now = book(.3, [(.5, [(.9, 100)], [(.89, 100)])])
        weaker_now = book(.4)
        groups = {"best":RawGroup([good_now,b]), "other":RawGroup([weaker_now,b])}
        d,a = run_event(pd.DataFrame([signal("best"),signal("other")]),groups,
                        {"best":("c","b"),"other":("a","b")},Experiment(wait_s=0,delay_s=.5))
        self.assertEqual(d.pair_id.tolist(), ["best"])
        self.assertEqual(d.status.tolist(), ["partial_unwound"])
        self.assertLess(d.pnl_usd.iloc[0], 0)
        self.assertIn("competing_same_time", a.reason.tolist())


    def test_shared_book_blocked_across_different_pairs(self):
        a,b,c = book(.4),book(.5),book(.3)
        d,audit = run_event(pd.DataFrame([signal("cb"),signal("ab",2)]),
            {"cb":RawGroup([c,b]),"ab":RawGroup([a,b])},
            {"cb":("c","b"),"ab":("a","b")},Experiment(wait_s=0,delay_s=.5))
        self.assertEqual(len(d),1)
        self.assertIn("shared_quote_reserved",audit.reason.tolist())


    def test_new_timestamp_with_unchanged_asks_is_not_replenishment(self):
        a,c = book(.4),book(.3)
        b = book(.5,[(2,[(.5,100)],[(.49,100)])])
        d,audit = run_event(pd.DataFrame([signal("cb"),signal("ab",2)]),
            {"cb":RawGroup([c,b]),"ab":RawGroup([a,b])},
            {"cb":("c","b"),"ab":("a","b")},Experiment(wait_s=0,delay_s=.5))
        self.assertEqual(len(d),1)
        self.assertIn("shared_quote_reserved",audit.reason.tolist())


    def test_new_changed_book_can_be_used_after_busy_period(self):
        a,c = book(.4),book(.3)
        b = book(.5,[(2,[(.49,100)],[(.48,100)])])
        d,_ = run_event(pd.DataFrame([signal("cb"),signal("ab",2)]),
            {"cb":RawGroup([c,b]),"ab":RawGroup([a,b])},
            {"cb":("c","b"),"ab":("a","b")},Experiment(wait_s=0,delay_s=.5))
        self.assertEqual(d.pair_id.tolist(),["cb","ab"])


    def test_event_busy_gate_blocks_even_nonsharing_pair(self):
        a,b,c,d = book(.4),book(.5),book(.3),book(.5)
        result,audit = run_event(pd.DataFrame([signal("ab"),signal("cd",.7)]),
            {"ab":RawGroup([a,b]),"cd":RawGroup([c,d])},
            {"ab":("a","b"),"cd":("c","d")},Experiment(wait_s=0,delay_s=.5))
        self.assertEqual(len(result),1)
        self.assertIn("event_in_flight",audit.reason.tolist())


