import tempfile
import unittest
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import torch

from src.models.tabm import TabMEstimator, TabMHurdle, aggregate, member_loss
from src.models.tabm_study import median_epochs
from src.models.tabm_study import REPLAY_POLICIES
from src.simulator.tabm import selections


class TabMTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def inputs(self):
        x=pd.DataFrame({'x_gap':np.linspace(-2,2,40),'x_missing':[np.nan]*40,
                        'x_flag':[0,1]*20,'m_topic':['a','b']*20})
        return x,(x.x_gap.gt(.5)).astype(float).to_numpy()

    def estimator(self,task='classifier'):
        return TabMEstimator(task,dict(n_blocks=1,d_block=16,dropout=.1,lr=.002,weight_decay=.0003),
                             k=4,max_epochs=3,patience=2,batch_size=16)

    def test_independent_member_loss_and_probability_aggregation(self):
        logits=torch.tensor([[-2.,4.]])
        truth=torch.tensor([1.])
        expected=(torch.nn.functional.softplus(torch.tensor(2.))+torch.nn.functional.softplus(torch.tensor(-4.)))/2
        self.assertAlmostEqual(member_loss(logits,truth,'classifier').item(),expected.item())
        self.assertAlmostEqual(aggregate(logits,'classifier').item(),logits.sigmoid().mean().item())
        self.assertNotAlmostEqual(aggregate(logits,'classifier').item(),logits.mean().sigmoid().item())

    def test_training_only_preprocessing_and_portable_predictions(self):
        x,y=self.inputs()
        valid=x.iloc[:6].copy()
        valid['x_gap']=1e8
        valid['m_topic']='unseen'
        model=self.estimator().fit(x,y,validation=(valid,np.array([0,1]*3)))
        quantiles=model.preprocessor_.named_transformers_['continuous'][-1].quantiles_
        self.assertLessEqual(quantiles.max(),2.)
        score=model.predict(valid)
        self.assertTrue(np.isfinite(score).all())
        self.assertTrue(((score>=0)&(score<=1)).all())
        self.assertTrue(all(p.device.type=='cpu' for p in model.model_.parameters()))
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'model.joblib'
            joblib.dump(model,path)
            np.testing.assert_array_equal(joblib.load(path).predict(valid),score)
        with self.assertRaises(ValueError):
            model.predict(valid[list(reversed(valid.columns))])

    def test_seed_reproducibility_and_fixed_epoch_refit(self):
        x,y=self.inputs()
        a=self.estimator().fit(x,y,epochs=2)
        b=self.estimator().fit(x,y,epochs=2)
        np.testing.assert_array_equal(a.predict(x),b.predict(x))
        self.assertEqual(a.best_epoch_,2)
        self.assertEqual(a.history_,[])
        with self.assertRaises(ValueError):
            self.estimator().fit(x,y,validation=(x,y),epochs=2)

    def test_unknown_targets_rejected_and_constant_cases_supported(self):
        x,y=self.inputs()
        y=y.copy()
        y[0]=np.nan
        with self.assertRaises(ValueError):
            self.estimator().fit(x,y)
        model=self.estimator().fit(x,np.zeros(len(x)),epochs=2)
        np.testing.assert_array_equal(model.predict(x),np.zeros(len(x)))

    def test_value_units_and_hurdle_product(self):
        x,y=self.inputs()
        gate=self.estimator().fit(x,y,epochs=2)
        amount=self.estimator('regression').fit(x[y>0],np.full(int(y.sum()),12.),epochs=2)
        hurdle=TabMHurdle(gate,amount)
        np.testing.assert_allclose(hurdle.predict(x),12*gate.predict(x))
        self.assertEqual(amount.training_rows_,int(y.sum()))

    def test_refit_epochs_come_from_matching_inner_task(self):
        rows=[dict(fold='a',view='combined',candidate=c,method=m,
                   classifier_epochs=g,regression_epochs=r,amount_epochs=v)
              for c,g,r,v in [(0,3,8,5),(0,8,11,10),(1,99,99,99)]
              for m in ('classifier','regression','two_stage')]
        self.assertEqual(median_epochs(rows,'a','combined',0,'classifier'),6)
        self.assertEqual(median_epochs(rows,'a','combined',0,'regression'),10)
        self.assertEqual(median_epochs(rows,'a','combined',0,'amount'),8)

    def test_replay_keeps_unknown_candidates_and_reranks_exclusions(self):
        pool=pd.DataFrame(dict(event_slug=['unknown','fed-decision-in-december','positive','a','b','c'],
            time=[pd.Timestamp('2025-12-01',tz='UTC')]*6,score=[6.,5.,4.,3.,2.,1.],
            label_known=[False,True,True,True,True,True],target=[np.nan,1.,1.,0.,0.,0.]))
        prediction=pd.concat([pool.assign(model=name) for name in ('quoted_gap_rule',)+REPLAY_POLICIES],ignore_index=True)
        chosen=selections(prediction)
        first=chosen.loc[chosen.model.eq(REPLAY_POLICIES[0])&chosen.top_n.eq(3)]
        self.assertEqual(first.loc[first.scope.eq('all_events'),'event_slug'].tolist(),
                         ['unknown','fed-decision-in-december','positive'])
        self.assertEqual(first.loc[first.scope.eq('exclude_december_fed'),'event_slug'].tolist(),
                         ['unknown','positive','a'])


if __name__=='__main__':
    unittest.main()
