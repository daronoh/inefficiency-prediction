import unittest

import numpy as np
import pandas as pd

from src.models.opportunities import choose_candidate, opportunity_metrics, split_masks


class OpportunityModelTests(unittest.TestCase):
    def test_training_purges_label_window_and_unknowns(self):
        start=pd.Timestamp('2025-11-01',tz='UTC')
        frame=pd.DataFrame({'time':[start-pd.Timedelta(hours=h) for h in [3,2,1,0]],
                            'label_known':[False,True,True,True]})
        train,test=split_masks(frame,start,start+pd.Timedelta(hours=1),pd.Timedelta(hours=1))
        self.assertEqual(train.tolist(),[False,True,False,False])
        self.assertEqual(test.tolist(),[False,False,False,True])

    def test_unknown_candidates_consume_budget_before_scoring(self):
        frame=pd.DataFrame(dict(event_slug=['unknown','positive','negative'],
            time=[pd.Timestamp('2025-11-01',tz='UTC')]*3,label_known=[False,True,True],
            target=[np.nan,1.,0.],future_positive=[False,True,False],
            opportunity_at_scan=[False]*3,x_active_tokens=[2]*3))
        result=opportunity_metrics(frame,[3.,2.,1.],1)
        self.assertEqual(result['recall'],0.)
        self.assertEqual(result['selected_unknown'],1)
        self.assertEqual(result['new_without_scan_recall'],0.)
        self.assertAlmostEqual(result['expected_random_captured'],1/3)

    def test_tuning_uses_pooled_recall_not_ap_or_dollars(self):
        rows=[dict(candidate=i,positive_windows=100,captured_positive_windows=c,
                   average_precision=ap,pnl=pnl)
              for i,c,ap,pnl in [(0,10,.9,1000),(1,11,.1,-1000)]]
        self.assertEqual(choose_candidate(rows),1)
        rows[0]['captured_positive_windows']=11
        self.assertEqual(choose_candidate(rows),0)
        rows[1]['average_precision']=.9
        self.assertEqual(choose_candidate(rows[::-1]),0)

    def test_candidates_must_use_same_validation_population(self):
        with self.assertRaises(ValueError):
            choose_candidate([dict(candidate=i,positive_windows=i+1,
                captured_positive_windows=1,average_precision=.5) for i in range(2)])


if __name__=='__main__':
    unittest.main()
