import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.models.final_preparation import choose, epoch_counts, training_mask, score_bundle, shortlist, predict_final


class ConstantModel:
    def __init__(self,value):
        self.value=value

    def predict(self,x):
        return np.full(len(x),self.value)


class FinalPreparationTests(unittest.TestCase):
    def test_n3_choice_uses_declared_objective_not_n5_or_pnl(self):
        rows=[dict(candidate=i,top_n=3,positive_windows=10,captured_positive_windows=c,
                   average_precision=.5,captured_peak_value=v,total_peak_value=100,
                   n5_capture=10-c,pnl=1000*(10-c))
              for i,c,v in [(0,9,20),(1,8,90)]]
        self.assertEqual(choose(rows,'logistic'),0)
        self.assertEqual(choose(rows,'random_forest'),0)
        self.assertEqual(choose(rows,'tabm_two_stage'),1)
        rows[0]['top_n']=5
        with self.assertRaises(ValueError):
            choose(rows,'logistic')

    def test_full_refit_strictly_purges_boundary_and_unknowns(self):
        cutoff=pd.Timestamp('2026-01-08',tz='UTC')
        frame=pd.DataFrame(dict(time=[cutoff-pd.Timedelta(hours=i) for i in (3,2,1,0)],
                                label_known=[False,True,True,True]))
        self.assertEqual(training_mask(frame,cutoff,pd.Timedelta(hours=1)).tolist(),[False,True,False,False])

    def test_epoch_choice_keeps_gate_and_amount_distinct(self):
        rows=[dict(candidate=c,classifier_epochs=g,amount_epochs=a)
              for c,g,a in [(0,3,10),(0,4,20),(1,100,100)]]
        self.assertEqual(epoch_counts(rows,0),{'classifier':4,'amount':15})
        with self.assertRaises(ValueError):
            epoch_counts(rows,1)

    def test_seed_ensemble_averages_products_and_ignores_labels(self):
        x=pd.DataFrame(dict(event_slug=['a','b'],time=[pd.Timestamp('2026-01-08',tz='UTC')]*2,
                            x_gap=[0.,1.],target=[np.nan,1.],label_known=[False,True]))
        bundle={'policies':{'tabm':dict(kind='hurdle_ensemble',features=['x_gap'],members=[
            dict(gate='g1',amount='a1'),dict(gate='g2',amount='a2')])}}
        mapping={'g1':ConstantModel(.2),'a1':ConstantModel(10.),'g2':ConstantModel(.8),'a2':ConstantModel(2.)}
        with patch('src.models.final_preparation.joblib.load',side_effect=lambda p:mapping[p.name]):
            scores=score_bundle(Path('.'),bundle,x)
            np.testing.assert_allclose(scores.score,1.8)
            pd.testing.assert_frame_equal(scores,score_bundle(Path('.'),bundle,x.drop(columns=['target','label_known'])))
        self.assertEqual(shortlist(scores,1).event_slug.tolist(),['a'])
        with self.assertRaises(ValueError):
            shortlist(scores,0)

    def test_final_interface_rejects_training_times(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            (root/'final_refit_bundle.json').write_text(json.dumps({'trained_before':'2026-01-08T00:00:00Z','policies':{}}))
            (root/'complete.json').write_text(json.dumps({'files_sha256':{}}))
            with self.assertRaises(ValueError):
                predict_final(root,pd.DataFrame(dict(time=[pd.Timestamp('2026-01-07',tz='UTC')])) )


if __name__=='__main__':
    unittest.main()
