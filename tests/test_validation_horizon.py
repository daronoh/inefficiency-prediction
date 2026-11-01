import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from src.models.validation_horizon import training_rows, forward_rows, paired_block_intervals, evaluate_predictions


class ValidationHorizonTests(unittest.TestCase):
    def test_shortened_training_purges_labels_and_unknowns(self):
        cutoff = pd.Timestamp('2025-10-28', tz='UTC')
        frame = pd.DataFrame(dict(time=[cutoff-pd.Timedelta(hours=h) for h in (3,2,1,0)],
            label_known=[False,True,True,True]))
        self.assertEqual(training_rows(frame, cutoff).index.tolist(), [1])

    def test_forward_population_retains_unknown_and_rejects_original_final(self):
        cutoff = pd.Timestamp('2025-10-28', tz='UTC')
        frame = pd.DataFrame(dict(time=[cutoff-pd.Timedelta(hours=1),cutoff], label_known=[True,False]))
        self.assertEqual(forward_rows(frame, cutoff).index.tolist(), [1])
        with self.assertRaisesRegex(ValueError, 'original final-period'):
            forward_rows(pd.DataFrame(dict(time=[pd.Timestamp('2026-01-08',tz='UTC')])), cutoff)

    def test_paired_blocks_preserve_correlated_rows_and_zero_denominators(self):
        times = pd.date_range('2025-11-11', periods=2, freq='7D', tz='UTC')
        pool = pd.DataFrame(dict(time=times, target=[1.,1.], value_target=[1.,9.]))
        same = paired_block_intervals(pool, pool, pool, repetitions=100)
        self.assertEqual(same['seven_day_blocks'],2)
        for key in ('recall_difference','recall_lower95','recall_upper95','value_capture_difference'):
            self.assertEqual(same[key],0.)
        empty = pool.iloc[:0]
        lost = paired_block_intervals(pool, empty, pool, repetitions=100)
        self.assertEqual(lost['recall_difference'],-1.)
        self.assertEqual(lost['value_capture_difference'],-1.)
        unknown = pool.assign(target=np.nan,value_target=np.nan)
        result = paired_block_intervals(unknown,unknown,unknown,repetitions=100)
        self.assertTrue(np.isnan(result['recall_lower95']))
        self.assertEqual(result['recall_defined_draws'],0)

    def test_evaluation_ranks_unknowns_and_compares_same_scans(self):
        time = pd.Timestamp('2025-11-11',tz='UTC')
        pool = pd.DataFrame(dict(event_slug=['a','b','c','d'],time=[time]*4,
            label_known=[False,True,True,True], target=[np.nan,1.,0.,0.],
            value_target=[np.nan,10.,0.,0.], future_positive=[False,True,False,False],
            opportunity_at_scan=[False]*4,x_active_tokens=[2]*4))
        frame = pd.concat([pool.assign(origin='train_14d',model='quoted_gap_rule',score=[4.,3.,2.,1.]),
            pool.assign(origin='train_14d',model='model',score=[4.,1.,3.,2.])],ignore_index=True)
        with tempfile.TemporaryDirectory() as folder:
            evaluate_predictions(frame,Path(folder))
            metrics=pd.read_csv(Path(folder)/'metrics.csv')
            result=metrics.query("model == 'model' and top_n == 3 and scope == 'common_forward'").iloc[0]
            self.assertEqual(result.selected_unknown,1)
            self.assertEqual(result.recall,0)
            self.assertEqual(result.candidates,4)
            self.assertEqual(result.expected_random_captured,.75)


if __name__ == '__main__':
    unittest.main()
