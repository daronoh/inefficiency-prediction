import unittest
from dataclasses import asdict, replace
from unittest.mock import patch

import numpy as np
import pandas as pd

from fixtures import BASE, _frame
from src.config import SPEC
from src.data.opportunities import Qualification, event_labels


class OpportunityTests(unittest.TestCase):
    def plan(self):
        return dict(event_slug='event',packages=[(['a','b'],BASE,BASE+pd.Timedelta(hours=1))])

    def test_ten_millisecond_gap_counts_without_any_trade_execution(self):
        f=_frame([(0,[(.6,10)],[(.5,10)]),(.01,[(.4,10)],[(.3,10)]),
                  (.02,[(.6,10)],[(.5,10)]),(3600,[(.6,10)],[(.5,10)])])
        with patch('src.simulator.scheduler.run_event',side_effect=AssertionError('No execution allowed')):
            labels,signals=event_labels(self.plan(),{'a':f,'b':f},[BASE])
        self.assertEqual(labels.target.iloc[0],1.)
        self.assertTrue(labels.future_positive.iloc[0])
        self.assertFalse(labels.opportunity_at_scan.iloc[0])
        self.assertAlmostEqual((signals.end_time.iloc[0]-signals.start_time.iloc[0]).total_seconds(),.01)

    def test_latency_unwind_and_cash_settings_do_not_change_labels(self):
        f=_frame([(0,[(.6,10)],[(.5,10)]),(.01,[(.4,10)],[(.3,10)]),
                  (.02,[(.6,10)],[(.5,10)]),(3600,[(.6,10)],[(.5,10)])])
        first,signals=event_labels(self.plan(),{'a':f,'b':f},[BASE],SPEC)
        changed=replace(SPEC,arrival_delay_s=20.,unwind_delay_s=99.,cash_cap_usd=.01)
        second,other=event_labels(self.plan(),{'a':f,'b':f},[BASE],changed)
        pd.testing.assert_frame_equal(first,second)
        pd.testing.assert_frame_equal(signals,other)
        self.assertNotIn('delay_s',asdict(Qualification()))

    def test_visible_gap_is_positive_but_not_a_new_onset(self):
        f=_frame([(0,[(.4,10)],[(.3,10)]),(3600,[(.4,10)],[(.3,10)])])
        labels,_=event_labels(self.plan(),{'a':f,'b':f},[BASE])
        self.assertEqual(labels.target.iloc[0],1.)
        self.assertTrue(labels.opportunity_at_scan.iloc[0])
        self.assertFalse(labels.future_positive.iloc[0])

    def test_positive_evidence_does_not_turn_incomplete_window_into_known_label(self):
        f=_frame([(0,[(.4,10)],[(.3,10)]),(100,[(.6,10)],[(.5,10)]),
                  (200,[(.6,10)],[(.5,10)]),(3600,[(.6,10)],[(.5,10)])])
        f.loc[1,'asks_json']=None
        labels,_=event_labels(self.plan(),{'a':f,'b':f},[BASE])
        self.assertTrue(labels.witnessed_opportunity.iloc[0])
        self.assertFalse(labels.label_known.iloc[0])
        self.assertTrue(np.isnan(labels.target.iloc[0]))

    def test_insufficient_depth_is_not_an_opportunity(self):
        f=_frame([(0,[(.4,2)],[(.3,10)]),(3600,[(.4,2)],[(.3,10)])])
        labels,_=event_labels(self.plan(),{'a':f,'b':f},[BASE])
        self.assertTrue(labels.label_known.iloc[0])
        self.assertEqual(labels.target.iloc[0],0.)

    def test_window_end_and_first_seen_gap_do_not_imply_future_onset(self):
        f=_frame([(0,[(.6,10)],[(.5,10)]),(3600,[(.4,10)],[(.3,10)])])
        labels,_=event_labels(self.plan(),{'a':f,'b':f},[BASE])
        self.assertEqual(labels.target.iloc[0],0.)
        g=_frame([(10,[(.4,10)],[(.3,10)]),(3600,[(.4,10)],[(.3,10)])])
        labels,_=event_labels(self.plan(),{'a':g,'b':g},[BASE])
        self.assertTrue(labels.witnessed_opportunity.iloc[0])
        self.assertFalse(labels.witnessed_new_onset.iloc[0])


if __name__ == '__main__':
    unittest.main()
