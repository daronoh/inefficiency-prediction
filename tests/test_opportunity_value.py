import unittest

import numpy as np
import pandas as pd

from fixtures import BASE, _frame
from src.data.opportunities import event_labels
from src.data.opportunity_value import peak_quotes
from src.models.opportunity_value import OpportunityValueModel, choose_value_candidate, value_metrics


class OpportunityValueTests(unittest.TestCase):
    def test_peak_rechecks_updates_without_adding_repeated_liquidity(self):
        plan=dict(event_slug='event',packages=[(['a','b'],BASE,BASE+pd.Timedelta(hours=1))])
        f=_frame([(0,[(.4,10)],[(.3,10)]),(10,[(.4,20)],[(.3,10)]),
                  (20,[(.4,20)],[(.3,10)]),(3600,[(.4,20)],[(.3,10)])])
        _,signals=event_labels(plan,{'a':f,'b':f},[BASE])
        values=peak_quotes(plan,{'a':f,'b':f},signals)
        self.assertEqual(len(values),1)
        self.assertAlmostEqual(values.peak_quoted_usd.iloc[0],4.)
        self.assertEqual(values.quantity.iloc[0],20)
        self.assertEqual(values.states_checked.iloc[0],3)
        self.assertEqual(values.peak_time.iloc[0],BASE+pd.Timedelta(seconds=10))

    def test_tuning_prefers_value_to_occurrence(self):
        rows=[dict(candidate=0,captured_peak_value=1.,total_peak_value=100.,captured_positive_windows=10),
              dict(candidate=1,captured_peak_value=10.,total_peak_value=100.,captured_positive_windows=1)]
        self.assertEqual(choose_value_candidate(rows),1)
        rows[0]['captured_peak_value']=10.
        self.assertEqual(choose_value_candidate(rows),0)

    def test_unknowns_consume_budget_without_imputed_value(self):
        frame=pd.DataFrame(dict(event_slug=['unknown','large','small'],time=[BASE]*3,
            label_known=[False,True,True],target=[np.nan,1.,1.],value_target=[np.nan,100.,1.],
            future_positive=[False,True,True],opportunity_at_scan=[False]*3,x_active_tokens=[2]*3))
        result=value_metrics(frame,[3.,2.,1.],1)
        self.assertEqual(result['captured_peak_value'],0.)
        result=value_metrics(frame,[1.,3.,2.],1)
        self.assertEqual(result['recall'],.5)
        self.assertAlmostEqual(result['peak_value_recall'],100/101)

    def test_hurdle_fits_only_positive_amounts_and_unweighted_gate(self):
        x=pd.DataFrame({'x':range(6)})
        params=dict(n_estimators=2,max_depth=2,min_samples_leaf=1,max_features=1.)
        model=OpportunityValueModel('random_forest',params,['x'],[]).fit(x,[0,0,0,1,2,3])
        self.assertEqual(model.positive_rows_,3)
        self.assertIsNone(model.gate_[-1].class_weight)
        p,v=model.components(x)
        np.testing.assert_allclose(model.predict(x),p*v)
        self.assertTrue((model.predict(x)>=0).all())
        zero=OpportunityValueModel('random_forest',params,['x'],[]).fit(x,[0]*6)
        np.testing.assert_array_equal(zero.predict(x),np.zeros(6))
        with self.assertRaises(ValueError):
            model.fit(x,[-1,0,0,0,0,1])


if __name__=='__main__':
    unittest.main()
