"""Book, payoff, target and execution safeguards for the submitted study."""
import json
import unittest
from dataclasses import replace
import numpy as np
import pandas as pd
from FYP.config import ScreeningSpec
from FYP.data.books import Ladder, RawBook, RawGroup, ObservationGroup
from FYP.data.coverage import classify_book, UNKNOWN, BLOCKED
from FYP.data.labels import event_labels
from FYP.data.value import peak_quotes
from FYP.data.preparation import BoundedGroup
from FYP.models.value import OpportunityValueModel, value_metrics
from FYP.rules.baskets import ladder_payoff, complete_set_payoff, validate_plan
from FYP.simulator.sizing import choose_order
from FYP.simulator.funding import setup
from FYP.simulator.ledger import run_scenario

BASE=pd.Timestamp('2025-11-12T00:00:00Z')
def frame(rows):
    return pd.DataFrame([dict(snapshot_time=BASE+pd.Timedelta(seconds=t),indexed_at_ms=i,
        asks_json=json.dumps([dict(price=p,size=q) for p,q in asks]),
        bids_json=json.dumps([dict(price=p,size=q) for p,q in bids]),
        min_order_size=5.,tick_size=.01) for i,(t,asks,bids) in enumerate(rows)])
def plan():return dict(event_slug='event',packages=[(['a','b'],BASE,BASE+pd.Timedelta(hours=1))])
def book(p,q=10):return RawBook(BASE,Ladder([dict(price=p,size=q)],True),Ladder([dict(price=p-.01,size=q)],False),5.,.01)

class CoreTests(unittest.TestCase):
    def test_basket_payoffs_and_membership(self):
        self.assertEqual([ladder_payoff(a,b) for a,b in [(False,False),(True,False),(True,True)]],[1,2,1])
        self.assertEqual(complete_set_payoff([False,True,False]),1)
        with self.assertRaises(ValueError):ladder_payoff(False,True)
        with self.assertRaises(ValueError):complete_set_payoff([True,True])
        with self.assertRaises(ValueError):validate_plan(dict(stratum='ladder',packages=[(['a','a'],BASE,BASE+pd.Timedelta(hours=1))],paths={'a':'a'}))

    def test_asof_never_reads_future_and_duplicate_time_uses_latest_index(self):
        f=frame([(0,[(.4,10)],[(.3,10)]),(0,[(.5,10)],[(.3,10)]),(10,[(.6,10)],[(.3,10)])])
        group=RawGroup([f])
        self.assertIsNone(group.at(BASE-pd.Timedelta(seconds=1)))
        self.assertEqual(group.at(BASE+pd.Timedelta(seconds=5))[0].asks.prices[0],.5)

    def test_each_member_recording_endpoint_bounds_replay(self):
        a=frame([(0,[(.4,10)],[(.3,10)]),(1,[(.4,10)],[(.3,10)])])
        b=frame([(0,[(.4,10)],[(.3,10)]),(10,[(.4,10)],[(.3,10)])])
        group=ObservationGroup([a,b])
        self.assertIsNone(group.at(BASE+pd.Timedelta(seconds=2)))
        a.attrs['recording_end']=BASE+pd.Timedelta(seconds=1)
        bounded=BoundedGroup([a,b],context_end=BASE+pd.Timedelta(seconds=11))
        self.assertIsNone(bounded.at(BASE+pd.Timedelta(seconds=2)))

    def test_empty_side_is_observed_blocker_but_missing_side_is_unknown(self):
        self.assertEqual(classify_book('[]','[]',5,.01,None)[1],BLOCKED)
        self.assertEqual(classify_book(None,'[]',5,.01,None)[1],UNKNOWN)
        self.assertEqual(classify_book('[{"price":0.2,"size":10}]','[{"price":0.3,"size":10}]',5,.01,None)[1],UNKNOWN)

    def test_integer_sizing_matches_exhaustive_depth_search(self):
        books=[RawBook(BASE,Ladder([dict(price=.2,size=6),dict(price=.5,size=18)],True),Ladder([dict(price=.1,size=30)],False),5,.01),book(.4,30)]
        sparse=choose_order(books,max_quantity=None,cash_cap=None,min_quantity=5,min_limit_margin=0.)
        brute=choose_order(books,max_quantity=100,cash_cap=None,min_quantity=5,min_limit_margin=0.)
        self.assertEqual(sparse,brute)

    def test_uncapped_sizing_can_exceed_one_thousand_shares(self):
        order=choose_order([book(.4,5000),book(.5,5000)],min_limit_margin=0.)
        self.assertEqual(order['quantity'],5000)
        self.assertGreater(order['reserved_cash_usd'],1000)

    def test_worst_limit_margin_blocks_unsafe_deep_basket(self):
        books=[RawBook(BASE,Ladder([dict(price=.1,size=5),dict(price=.8,size=5)],True),Ladder([dict(price=.05,size=10)],False),5,.01),book(.3)]
        order=choose_order(books,min_limit_margin=0.)
        self.assertEqual(order['quantity'],5)

    def test_quote_tick_and_minimum_size_are_enforced(self):
        bad=book(.405)
        self.assertIsNone(choose_order([bad,book(.4)]))
        self.assertIsNone(choose_order([book(.4,4),book(.4,4)]))

    def test_ten_millisecond_opportunity_counts_without_persistence_filter(self):
        f=frame([(0,[(.6,10)],[(.5,10)]),(.01,[(.4,10)],[(.3,10)]),(.02,[(.6,10)],[(.5,10)]),(3600,[(.6,10)],[(.5,10)])])
        labels,signals=event_labels(plan(),{'a':f,'b':f},[BASE])
        self.assertEqual(labels.target.iloc[0],1.)
        self.assertAlmostEqual((signals.end_time.iloc[0]-signals.start_time.iloc[0]).total_seconds(),.01)

    def test_incomplete_positive_window_remains_unknown(self):
        f=frame([(0,[(.4,10)],[(.3,10)]),(100,[(.6,10)],[(.5,10)]),(200,[(.6,10)],[(.5,10)]),(3600,[(.6,10)],[(.5,10)])])
        f.loc[1,'asks_json']=None
        labels,_=event_labels(plan(),{'a':f,'b':f},[BASE])
        self.assertTrue(labels.witnessed_opportunity.iloc[0])
        self.assertFalse(labels.label_known.iloc[0])
        self.assertTrue(pd.isna(labels.target.iloc[0]))

    def test_window_endpoint_does_not_count_in_previous_hour(self):
        f=frame([(0,[(.6,10)],[(.5,10)]),(3600,[(.4,10)],[(.3,10)])])
        labels,_=event_labels(plan(),{'a':f,'b':f},[BASE])
        self.assertEqual(labels.target.iloc[0],0.)

    def test_peak_value_is_maximum_instead_of_sum_of_snapshots(self):
        f=frame([(0,[(.4,10)],[(.3,10)]),(10,[(.4,20)],[(.3,10)]),(20,[(.4,20)],[(.3,10)]),(3600,[(.4,20)],[(.3,10)])])
        _,signals=event_labels(plan(),{'a':f,'b':f},[BASE])
        peak=peak_quotes(plan(),{'a':f,'b':f},signals)
        self.assertAlmostEqual(peak.peak_quoted_usd.iloc[0],4.)
        self.assertEqual(peak.states_checked.iloc[0],3)

    def test_value_hurdle_uses_positive_only_amounts_and_natural_gate(self):
        x=pd.DataFrame(dict(x=range(6)))
        model=OpportunityValueModel('random_forest',dict(n_estimators=2,max_depth=2,min_samples_leaf=1,max_features=1.),['x'],[]).fit(x,[0,0,0,1,2,3])
        self.assertEqual(model.positive_rows_,3)
        self.assertIsNone(model.gate_[-1].class_weight)
        p,v=model.components(x);np.testing.assert_allclose(model.predict(x),p*v)

    def test_unknown_selection_consumes_budget_without_imputed_value(self):
        f=pd.DataFrame(dict(event_slug=['unknown','large','small'],time=[BASE]*3,label_known=[False,True,True],target=[np.nan,1.,1.],value_target=[np.nan,100.,1.],future_positive=[False,True,True],opportunity_at_scan=[False]*3,x_active_tokens=[2]*3))
        metric=value_metrics(f,[3.,2.,1.],1)
        self.assertEqual(metric['captured_peak_value'],0.)
        self.assertEqual(metric['selected_unknown'],1)

    def test_primary_execution_contract_has_no_imposed_failures_or_caps(self):
        s=setup(.1)
        self.assertEqual(s.delay_s,.1);self.assertEqual(s.wait_s,0.)
        self.assertIsNone(s.max_quantity);self.assertIsNone(s.cash_cap_usd)
        self.assertEqual(s.unwind_delay_s,1.)
        self.assertEqual(s.freshness_s,float('inf'))

    def test_partial_fill_loss_is_actual_unwind_difference(self):
        a=frame([(0,[(.4,10)],[(.3,10)]),(.1,[(.4,10)],[(.3,10)]),(2,[(.4,10)],[(.3,10)])])
        b=frame([(0,[(.5,10)],[(.4,10)]),(.05,[(.6,10)],[(.4,10)]),(2,[(.6,10)],[(.4,10)])])
        signals=pd.DataFrame([dict(event_slug='event',window='study',time=BASE,start_time=BASE,end_time=BASE+pd.Timedelta(seconds=.05),monitoring_end_time=BASE+pd.Timedelta(hours=1))])
        trades=run_scenario(signals,ObservationGroup([a,b]),setup(.1),entry_end=BASE+pd.Timedelta(hours=1),context_end=BASE+pd.Timedelta(seconds=3601))
        self.assertEqual(trades.status.iloc[0],'partial_unwound')
        self.assertAlmostEqual(trades.pnl_usd.iloc[0],-1.)

    def test_unknown_arrival_does_not_become_zero_profit(self):
        f=frame([(0,[(.4,10)],[(.3,10)])])
        signals=pd.DataFrame([dict(event_slug='event',window='study',time=BASE,start_time=BASE,end_time=BASE+pd.Timedelta(seconds=.01),monitoring_end_time=BASE+pd.Timedelta(hours=1))])
        trades=run_scenario(signals,ObservationGroup([f,f]),setup(.1),entry_end=BASE+pd.Timedelta(hours=1),context_end=BASE+pd.Timedelta(seconds=3601))
        self.assertEqual(trades.status.iloc[0],'unknown_arrival')
        self.assertTrue(pd.isna(trades.pnl_usd.iloc[0]))
