import unittest
import numpy as np
import pandas as pd

from fixtures import BASE, _frame
from src.data.books import ObservationGroup
from src.simulator.opportunities import POLICIES, selections, replay_event, funding_metrics, setup


class OpportunityReplayTests(unittest.TestCase):
    def test_selections_do_not_filter_unknown_labels_and_are_nested(self):
        pool=pd.DataFrame(dict(event_slug=list('abcdef'),time=[BASE]*6,score=[6,5,4,3,2,1],
            label_known=[False,True,True,True,True,True],target=[np.nan,1,0,0,0,0]))
        prediction=pd.concat([pool.assign(model=name) for name in POLICIES],ignore_index=True)
        result=selections(prediction,random_count=2)
        part=result.loc[result.scope.eq('all_events')&result.model.eq('quoted_gap_rule')]
        self.assertEqual(set(part.loc[part.top_n.eq(3),'event_slug']),set('abc'))
        self.assertEqual(set(part.loc[part.top_n.eq(5),'event_slug']),set('abcde'))
        altered=prediction.assign(label_known=True,target=0)
        other=selections(altered,random_count=2)
        cols=['scope','top_n','model','event_slug','time','score']
        pd.testing.assert_frame_equal(result[cols],other[cols])

    def test_delay_changes_fills_not_selected_events(self):
        a=_frame([(0,[(.4,10)],[(.3,10)]),(.075,[(.7,10)],[(.6,10)]),(3600,[(.7,10)],[(.6,10)])])
        b=_frame([(0,[(.4,10)],[(.3,10)]),(3600,[(.4,10)],[(.3,10)])])
        signals=pd.DataFrame([dict(event_slug='event',pair_id='p',time=BASE,start_time=BASE,
            end_time=BASE+pd.Timedelta(seconds=.075),monitoring_end_time=BASE+pd.Timedelta(hours=1),window='development')])
        chosen=pd.DataFrame([dict(event_slug='event',time=BASE,scope='all_events',top_n=5,model='test')])
        trades,_=replay_event(signals,{'p':ObservationGroup([a,b])},{'p':('a','b')},chosen)
        fast=trades.loc[trades.arrival_delay_s.isin([0.,.05])]
        slow=trades.loc[trades.arrival_delay_s.ge(.1)]
        self.assertTrue(fast.status.eq('complete').all())
        self.assertTrue(slow.status.eq('partial_unwound').all())
        np.testing.assert_allclose(fast.pnl_usd,2.)
        np.testing.assert_allclose(slow.pnl_usd,-1.)

    def test_reservations_survive_hour_boundary(self):
        f=_frame([(0,[(.4,10)],[(.3,10)]),(3600,[(.4,10)],[(.3,10)]),(7200,[(.4,10)],[(.3,10)])])
        signals=pd.DataFrame([dict(event_slug='event',pair_id='p',time=BASE+pd.Timedelta(hours=h),
            start_time=BASE+pd.Timedelta(hours=h),end_time=BASE+pd.Timedelta(hours=h+1),
            monitoring_end_time=BASE+pd.Timedelta(hours=h+1),window='development') for h in (0,1)])
        chosen=pd.DataFrame([dict(event_slug='event',time=t,scope='all_events',top_n=5,model='test') for t in signals.time])
        trades,audit=replay_event(signals,{'p':ObservationGroup([f,f])},{'p':('a','b')},chosen)
        self.assertTrue(trades.groupby('arrival_delay_s').size().eq(1).all())
        self.assertEqual(int(audit.reason.eq('shared_quote_reserved').sum()),5)

    def test_funding_keeps_completed_capital_and_returns_partial_proceeds(self):
        def row(t,status,cost,pnl):
            return dict(confirmation_time=BASE+pd.Timedelta(seconds=t),arrival_time=BASE+pd.Timedelta(seconds=t+.1),
                unwind_time=BASE+pd.Timedelta(seconds=t+1.1),attempted=True,reserved_cash_usd=10.,
                entry_cost_usd=cost,pnl_usd=pnl,status=status)
        part=pd.DataFrame([row(0,'complete',8.,2.),row(2,'partial_unwound',4.,-1.),row(4,'no_fill',0.,0.)])
        result=funding_metrics(part)
        self.assertAlmostEqual(result['peak_inflight_reservations_usd'],10.)
        self.assertAlmostEqual(result['no_settlement_funding_usd'],19.)
        self.assertIsNone(setup(.1).cash_cap_usd)
        self.assertFalse(setup(.1).compute_failure_stress)


if __name__=='__main__':
    unittest.main()
