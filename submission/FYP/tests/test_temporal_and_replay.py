import unittest
import tempfile
from unittest.mock import patch, MagicMock
from pathlib import Path

import numpy as np
import pandas as pd

from FYP.data.pipeline import FIRST, END, HORIZON
from FYP.experiments.study import folds, train_before, choose, fit_network, add_ensembles
from FYP.experiments.economics import selections, event_replay, delays_for, load_signals, summarize
from FYP.simulator.schema import ATTEMPT_COLUMNS
from FYP.data.books import RawBook, Ladder


class WeeklyModelTests(unittest.TestCase):
    def test_weekly_intervals_cover_every_hour_once_including_short_tail(self):
        windows=folds()
        self.assertEqual(len(windows),13)
        self.assertEqual(windows[0][1],FIRST)
        self.assertEqual(windows[-1][2],END)
        self.assertEqual(windows[-1][2]-windows[-1][1],pd.Timedelta(hours=8))
        self.assertTrue(all(left[2]==right[1] for left,right in zip(windows,windows[1:])))
        self.assertEqual(sum(int((b-a)/HORIZON) for _,a,b in windows),2024)

    def test_labels_must_finish_strictly_before_each_fit(self):
        f=pd.DataFrame(dict(time=[FIRST-pd.Timedelta(hours=h) for h in [4,3,2,1,0]],
            label_known=[False,True,True,True,True]))
        self.assertEqual(train_before(f,FIRST).index.tolist(),[1,2])

    def test_hyperparameters_choose_value_then_occurrence_then_ap(self):
        rows=[dict(candidate=i,total_peak_value=100,captured_peak_value=v,
            positive_windows=10,captured_positive_windows=c,average_precision=a)
            for i,v,c,a in [(0,90,8,.1),(1,80,10,.9)]]
        self.assertEqual(choose(rows),0)
        rows[1]['captured_peak_value']=90
        self.assertEqual(choose(rows),1)
        rows[1]['captured_positive_windows']=8
        self.assertEqual(choose(rows),1)
        rows[1]['average_precision']=.1
        self.assertEqual(choose(rows[::-1]),0)
        rows[1]['total_peak_value']=200
        with self.assertRaises(ValueError):choose(rows)

    def test_tabm_amount_stage_is_positive_only_and_preprocessing_sees_features(self):
        train=pd.DataFrame(dict(label_known=[True]*3,target=[0.,1.,1.],value_target=[0.,1.,9.],x=[1.,2.,3.]))
        valid=train.copy()
        with patch('FYP.experiments.study.TabMEstimator') as cls:
            fit_network(train,valid,'amount',{},['x'],'cpu')
            args,kwargs=cls.return_value.fit.call_args
            self.assertEqual(args[0].columns.tolist(),['x'])
            self.assertEqual(args[1].tolist(),[1.,9.])
            self.assertEqual(kwargs['validation'][1].tolist(),[1.,9.])
            self.assertEqual(cls.call_args.kwargs['k'] if 'k' in cls.call_args.kwargs else 32,32)

    def test_ensembles_rank_each_scan_independently(self):
        frame=pd.DataFrame(dict(time=[FIRST,FIRST,FIRST+HORIZON,FIRST+HORIZON]))
        keys=['tuned_combined_logistic','tuned_combined_random_forest','tuned_combined_xgboost','tabm_two_stage','forest_two_stage']
        scores={k:np.array([100.,200.,2.,1.]) for k in keys}
        result=add_ensembles(frame,scores)
        np.testing.assert_allclose(result['rank_ensemble'],[.5,1.,1.,.5])


class WeeklyEconomicTests(unittest.TestCase):
    def test_empty_source_signal_exports_do_not_require_a_time_column(self):
        with tempfile.TemporaryDirectory() as name:
            folder=Path(name)
            pd.DataFrame().to_parquet(folder/'empty.parquet')
            self.assertTrue(load_signals([folder/'empty.parquet']).empty)
            pd.DataFrame(dict(time=[FIRST])).to_parquet(folder/'valid.parquet')
            result=load_signals([folder/'empty.parquet',folder/'valid.parquet'])
            self.assertEqual(result.time.tolist(),[FIRST])
            pd.DataFrame(dict(other=[1])).to_parquet(folder/'invalid.parquet')
            with self.assertRaises(ValueError):load_signals([folder/'invalid.parquet'])

    def test_future_labels_do_not_change_selection_and_unknowns_consume_slots(self):
        frame=pd.DataFrame(dict(event_slug=['a','b','c','d'],time=[FIRST]*4,fold=['week_00']*4,
            model=['quoted_gap_rule']*4,score=[4.,3.,2.,1.],label_known=[False,True,True,True],
            target=[np.nan,1.,0.,0.]))
        a=selections(frame,random_count=2)
        b=selections(frame.assign(target=[1.,0.,1.,1.]),random_count=2)
        keys=['scope','top_n','model','event_slug','time']
        pd.testing.assert_frame_equal(a[keys],b[keys])
        q=a.loc[a.scope.eq('all_events')&a.top_n.eq(3)&a.model.eq('quoted_gap_rule')]
        self.assertEqual(q.event_slug.tolist(),['a','b','c'])
        self.assertEqual(int((~q.label_known).sum()),1)

    def test_sensitivities_and_primary_random_scenario_are_explicit(self):
        self.assertEqual(delays_for('all_events','random_00'),(.1,))
        self.assertEqual(delays_for('exclude_december_fed','model'),(.1,))
        self.assertEqual(set(delays_for('all_events','model')),{0.,.05,.1,.25,.5})

    def test_scenario_names_match_actual_latency_in_empty_execution_summary(self):
        chosen=pd.DataFrame(dict(event_slug=['a'],time=[FIRST],label_known=[True],target=[0.],
            scope=['all_events'],top_n=[3],model=['model']))
        result=summarize(pd.DataFrame(columns=ATTEMPT_COLUMNS),chosen)
        for row in result.itertuples():
            self.assertEqual(row.scenario,f'arrival_{int(row.arrival_delay_s*1000)}ms')

    def test_quote_reservations_persist_when_weekly_model_is_refitted(self):
        stamp=FIRST+pd.Timedelta(hours=9)
        books=[RawBook(stamp,Ladder([dict(price=p,size=10.)],True),
            Ladder([dict(price=p-.01,size=10.)],False),5.,.01) for p in (.4,.5)]
        class Group:
            def at(self,when):return books
        times=[stamp,stamp+pd.Timedelta(days=7)]
        signals=pd.DataFrame([dict(pair_id='event:0',event_slug='event',time=t,start_time=t,
            end_time=t+pd.Timedelta(seconds=1),monitoring_end_time=t+HORIZON,window='weekly') for t in times])
        choices=pd.DataFrame(dict(time=times,scope=['exclude_december_fed']*2,top_n=[3]*2,
            model=['weekly_model']*2,fold=['week_00','week_01']))
        trades,audit,cache=event_replay(signals,{'event:0':Group()},{'event:0':('yes','no')},choices)
        self.assertEqual(int(trades.attempted.sum()),1)
        self.assertEqual(trades.status.tolist(),['complete'])
        self.assertIn('shared_quote_reserved',audit.reason.tolist())

    def test_replay_covers_late_period_entries(self):
        stamp=pd.Timestamp('2026-01-20T09:00:00Z')
        books=[RawBook(stamp,Ladder([dict(price=p,size=10.)],True),
            Ladder([dict(price=p-.01,size=10.)],False),5.,.01) for p in (.4,.5)]
        class Group:
            def at(self,when):return books
        signals=pd.DataFrame([dict(pair_id='event:0',event_slug='event',time=stamp,start_time=stamp,
            end_time=stamp+pd.Timedelta(seconds=1),monitoring_end_time=stamp+HORIZON,window='study')])
        choices=pd.DataFrame(dict(time=[stamp],scope=['exclude_december_fed'],top_n=[3],model=['weekly_model']))
        trades,_,_=event_replay(signals,{'event:0':Group()},{'event:0':('yes','no')},choices)
        self.assertEqual(trades.status.tolist(),['complete'])


if __name__=='__main__':
    unittest.main()
