import unittest
import numpy as np
import pandas as pd
from dataclasses import replace
from pathlib import Path
import subprocess
import sys
import tempfile
import joblib

from src.config import SPEC
from src.models.tuning import (inner_windows, inner_masks, choose_candidate, candidates,
                               METHODS, fit_model, score_model)
from src.models.profitability import selected_value


class TuningTests(unittest.TestCase):
    def test_nested_time_boundaries_purge_labels_and_keep_unknown_candidates(self):
        spec = replace(SPEC, arrival_delay_s=.1)
        frame = pd.DataFrame({'time':pd.date_range('2025-10-14', '2025-11-03', freq='h', tz='UTC')})
        frame['label_known'] = True
        frame.loc[frame.index % 3 == 0, 'label_known'] = False
        outer = pd.Timestamp('2025-11-01', tz='UTC')
        gap = spec.horizon + pd.Timedelta(seconds=1.1)
        for start, end in inner_windows(frame, '2025-11-01', spec):
            a, b = inner_masks(frame, start, end, spec)
            self.assertTrue(frame.loc[a, 'label_known'].all())
            self.assertTrue((frame.loc[a, 'time'] + gap < start).all())
            self.assertTrue((frame.loc[b, 'time'] + gap < outer).all())
            self.assertTrue((~frame.loc[b, 'label_known']).any())

    def test_selection_keeps_losses_and_unknown_rows_consume_slots(self):
        frame = pd.DataFrame(dict(event_slug=['loss','gain','unknown'],
            time=[pd.Timestamp('2025-11-01',tz='UTC')]*3,
            label_known=[True,True,False],pnl_target=[-10.,1.,np.nan]))
        self.assertEqual(selected_value(frame,[3,2,1],1), -10.)
        self.assertEqual(selected_value(frame,[1,2,3],1), 0.)
        rows = [dict(candidate=0,hours=1,selected_standalone_pnl_usd=-10.),
                dict(candidate=1,hours=1,selected_standalone_pnl_usd=1.)]
        self.assertEqual(choose_candidate(rows),1)
        rows[0]['selected_standalone_pnl_usd']=1.
        self.assertEqual(choose_candidate(rows),0)

    def test_all_search_budgets_are_equal(self):
        for kind, name in METHODS:
            self.assertEqual(len(candidates(name,kind)),8)

    def test_xgboost_and_two_stage_preserve_negative_amounts(self):
        frame = pd.DataFrame(dict(x_value=np.arange(30,dtype=float),
            target=[0,1]*15, activity_target=[0]*10+[1]*20,
            pnl_target=[0.]*10+[-2.]*20))
        params = dict(candidates('xgboost','two_stage')[0], n_estimators=2, min_child_weight=1)
        model = fit_model(frame,'two_stage','xgboost',params,['x_value'])
        p, amount = model.components(frame[['x_value']])
        self.assertTrue(np.all(amount < 0))
        self.assertEqual(model.gate_[-1].get_params()['scale_pos_weight'],1.)
        np.testing.assert_allclose(score_model(model,frame[['x_value']],'two_stage'),p*amount)
        frame['activity_target'],frame['pnl_target']=0,0.
        model = fit_model(frame,'two_stage','xgboost',params,['x_value'])
        np.testing.assert_equal(model.predict(frame[['x_value']]),np.zeros(30))

    def test_saved_two_stage_model_loads_in_a_fresh_process(self):
        frame = pd.DataFrame(dict(x_value=np.arange(12,dtype=float),
            target=[0,1]*6,activity_target=[0]*4+[1]*8,pnl_target=[0.]*4+[-2.]*8))
        params = dict(candidates('xgboost','two_stage')[0],n_estimators=2,min_child_weight=1)
        model = fit_model(frame,'two_stage','xgboost',params,['x_value'])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'model.joblib'
            joblib.dump(model,path)
            result = subprocess.run([sys.executable,'-c',
                "import joblib,sys,pandas as pd; m=joblib.load(sys.argv[1]); "
                "assert m.predict(pd.DataFrame({'x_value':[1.,8.]})).shape == (2,)",str(path)],
                capture_output=True,text=True,timeout=60)
            self.assertEqual(result.returncode,0,result.stderr)


if __name__ == '__main__':
    unittest.main()
