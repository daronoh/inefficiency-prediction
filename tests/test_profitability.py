"""Profit targets retain losses, missing coverage and causal depth features."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
from fixtures import BASE,_frame,_levels
from src.config import SPEC
from src.data.books import RawGroup
from src.data.coverage import classify_book,TRADABLE,BLOCKED
from src.data.profitability import build_profitability,load_profitability,quoted_net
from src.models.profitability import ActivityPnLModel
from src.simulator.profitability import policy_choices


class ProfitabilityTests(unittest.TestCase):
    def test_uncapped_coverage_accepts_large_minimum_and_fractional_depth(self):
        asks,bids=_levels([(.4,3000)]),_levels([(.3,3000)])
        self.assertEqual(classify_book(asks,bids,2000,.001,None)[1],TRADABLE)
        self.assertEqual(classify_book(asks,bids,2000,.001)[1],BLOCKED)
        self.assertEqual(classify_book(_levels([(.4,4.9)]),bids,5,.001,None)[1],BLOCKED)

    def test_quote_feature_keeps_negative_values_and_uses_large_depth(self):
        a=_frame([(0,[(.4,5000)],[(.39,5000)])])
        b=_frame([(0,[(.5,5000)],[(.49,5000)])])
        self.assertAlmostEqual(quoted_net(RawGroup([a,b]).at(BASE)),500.)
        b=_frame([(0,[(.7,5000)],[(.69,5000)])])
        self.assertAlmostEqual(quoted_net(RawGroup([a,b]).at(BASE)),-.5)

    def test_hurdle_retains_negative_conditional_outcomes(self):
        x=pd.DataFrame({'x':np.zeros(20)})
        model=ActivityPnLModel('ridge').fit(x,np.full(20,-4.),np.ones(20))
        np.testing.assert_allclose(model.predict(x),-4.)
        p,amount=model.components(x)
        np.testing.assert_allclose(model.predict(x),p*amount)
        model=ActivityPnLModel('ridge').fit(x,np.zeros(20),np.zeros(20))
        np.testing.assert_allclose(model.predict(x),0.)
        with self.assertRaises(ValueError):
            model.fit(x,np.ones(20),np.zeros(20))

    def test_excluding_event_reallocates_monitoring_slot(self):
        frame=pd.DataFrame(dict(model=['m']*3,event_slug=['dominant','a','b'],time=[BASE]*3,score=[3.,2.,1.]))
        self.assertEqual(policy_choices(frame,1)['m'],{('dominant',BASE)})
        self.assertEqual(policy_choices(frame,1,'dominant')['m'],{('a',BASE)})

    def test_rebuilt_hourly_target_includes_loss_and_keeps_unknown_missing(self):
        for last,known in [(3602,True),(1800,False)]:
            with self.subTest(known=known),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)
                a=_frame([(0,[(.4,5000)],[(.39,5000)]),(last,[(.4,5000)],[(.39,5000)])])
                b=_frame([(0,[(.5,5000)],[(.49,5000)]),(.05,[(.5,4999)],[(.49,5000)]),
                          (last,[(.5,4999)],[(.49,5000)])])
                frames={'a':a,'b':b}
                paths={t:root/f'{t}.parquet' for t in frames}
                for t,f in frames.items():
                    f.to_parquet(paths[t],index=False)
                plan=dict(event_slug='test',packages=[(('a','b'),BASE,BASE+pd.Timedelta(hours=1))],paths=paths)
                features=pd.DataFrame([dict(event_slug='test',time=BASE,stratum='ladder',eligible_snapshot=True,
                    x_best_quoted_net_usd=100.,x_size_feasible_package_fraction=1.,x_positive_quoted_net_package_fraction=1.)])
                feature_path=root/'features.parquet'
                features.to_parquet(feature_path,index=False)
                with patch('src.data.profitability.build_plan',return_value=([plan],[])),patch('src.data.profitability.load_frames',return_value=frames):
                    build_profitability(root/'profit',feature_path,SPEC)
                row=load_profitability(root/'profit').iloc[0]
                self.assertEqual(bool(row.label_known),known)
                self.assertAlmostEqual(row.known_pnl_usd,-50.)
                self.assertAlmostEqual(row.x_best_quoted_net_usd,500.)
                if known:
                    self.assertEqual(row.activity_target,1.)
                    self.assertAlmostEqual(row.pnl_target,-50.)
                else:
                    self.assertTrue(pd.isna(row.pnl_target))
                    self.assertTrue(pd.isna(row.activity_target))
