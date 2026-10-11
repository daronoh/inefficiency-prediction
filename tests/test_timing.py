"""Timing benchmarks retain failed legs and use only the selected arrival state."""
from dataclasses import replace
import unittest
import pandas as pd
from fixtures import BASE, _frame
from src.config import SPEC
from src.data.books import RawGroup
from src.simulator.execution import experiment
from src.simulator.scheduler import run_event
from src.simulator.sensitivity import summarize


class TimingTests(unittest.TestCase):
    def test_zero_delay_fills_basket_but_later_missing_leg_is_unwound(self):
        a = _frame([(0,[(.4,10)],[(.39,10)]),(2,[(.4,10)],[(.39,10)])])
        b = _frame([(0,[(.5,10)],[(.49,10)]),(.1,[(.6,10)],[(.49,10)]),(2,[(.6,10)],[(.49,10)])])
        group = RawGroup([a,b])
        signals = pd.DataFrame([dict(event_slug='test',pair_id='p',window='development',time=BASE,
            start_time=BASE,end_time=BASE+pd.Timedelta(seconds=.1),monitoring_end_time=BASE+pd.Timedelta(hours=1))])
        setup = replace(experiment(SPEC), max_quantity=10)
        instant = run_event(signals,{'p':group},{'p':('a','b')},replace(setup,delay_s=0.))[0]
        delayed = run_event(signals,{'p':group},{'p':('a','b')},setup)[0]
        self.assertEqual(instant.iloc[0].status,'complete')
        self.assertAlmostEqual(instant.iloc[0].pnl_usd,1.)
        self.assertEqual(delayed.iloc[0].status,'partial_unwound')
        self.assertAlmostEqual(delayed.iloc[0].pnl_usd,-.1)
        self.assertEqual(instant.iloc[0].quantity,delayed.iloc[0].quantity)
        self.assertEqual(instant.iloc[0].limits_json,delayed.iloc[0].limits_json)

    def test_unknown_pnl_is_counted_separately_from_known_subtotal(self):
        details = pd.DataFrame({'attempted':[True,True], 'status':['complete','partial_unknown_unwind'],
                                'pnl_usd':[2.,float('nan')]})
        row = summarize(details,'model','scenario',.5,1.)
        self.assertEqual(row['known_pnl_usd'],2.)
        self.assertEqual(row['unknown_attempts'],1)
        self.assertEqual(row['partial_attempts'],1)
