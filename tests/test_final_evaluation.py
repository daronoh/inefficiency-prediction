import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.config import SPEC, ScreeningSpec, require_split_access
from src.data.books import RawGroup, ObservationGroup
from src.data.final_evaluation import (FinalGroup, sample_features, quote_labels,
                                      discover_final, build)
from src.data.opportunities import event_labels
from src.data.opportunity_value import peak_quotes
from src.models.final_evaluation import measure, scored_references, selections, canonicalize_forest_alias
from src.simulator.capacity import discover_depth_signals
from src.data.opportunities import Qualification
from src.simulator.execution import run_scenario as historical_scenario
from src.simulator.scheduler import run_event as historical_event
from src.simulator.final_execution import run_scenario, run_event
from src.simulator.opportunities import setup
from fixtures import BASE, _frame


def frames(base=BASE):
    values = {}
    for token,price in [('a',.4),('b',.5)]:
        f = _frame([(0,[(price,100)],[(price-.01,100)]),
                    (2,[(price-.01,100)],[(price-.02,100)]),
                    (3601,[(price-.01,100)],[(price-.02,100)])],base=base)
        f.attrs['recording_end'] = base+pd.Timedelta(seconds=3601)
        values[token] = f
    return values


def plan(base=BASE):
    return dict(event_slug='example',stratum='negrisk',packages=[(['a','b'],base,base+pd.Timedelta(hours=1))])


class FinalEvaluationTests(unittest.TestCase):
    def test_final_access_is_explicit_and_old_guard_unchanged(self):
        with self.assertRaises(ValueError):
            build('unused',open_final=False)
        with self.assertRaises(ValueError):
            require_split_access('final')

    def test_final_reader_never_uses_future_and_stops_each_member(self):
        f = frames(pd.Timestamp('2026-01-09',tz='UTC'))
        base = f['a'].snapshot_time.iloc[0]
        group = FinalGroup(list(f.values()),context_end=base+pd.Timedelta(hours=2))
        self.assertIsNone(group.at(base-pd.Timedelta(seconds=1)))
        self.assertEqual(group.at(base+pd.Timedelta(seconds=1))[0].stamp,base)
        f['b'].attrs['recording_end'] = base+pd.Timedelta(seconds=1)
        short = FinalGroup(list(f.values()),context_end=base+pd.Timedelta(hours=2))
        self.assertIsNone(short.at(base+pd.Timedelta(seconds=2)))
        features = sample_features(plan(base),f,pd.DatetimeIndex([base+pd.Timedelta(seconds=2)]),base+pd.Timedelta(hours=2))
        self.assertTrue(features.eligible_snapshot.iloc[0])

    def test_bounded_projection_preserves_timestamp_supported_carry(self):
        f = frames()
        projected = {t:x.iloc[:2].copy() for t,x in f.items()}
        g = FinalGroup(list(projected.values()),context_end=BASE+pd.Timedelta(hours=1))
        self.assertIsNotNone(g.at(BASE+pd.Timedelta(minutes=59)))
        self.assertEqual(g.at(BASE+pd.Timedelta(minutes=59))[0].stamp,BASE+pd.Timedelta(seconds=2))

    def test_feature_clean_fraction_counts_unseen_members_in_denominator(self):
        f = frames()
        f['b'] = f['b'].assign(snapshot_time=f['b'].snapshot_time+pd.Timedelta(seconds=30))
        row = sample_features(plan(),f,pd.DatetimeIndex([BASE]),BASE+pd.Timedelta(hours=2)).iloc[0]
        self.assertEqual(row.x_seen_fraction,.5)
        self.assertEqual(row.x_clean_fraction,.5)

    def test_reader_rejects_books_beyond_declared_context(self):
        with self.assertRaises(ValueError):
            FinalGroup(list(frames().values()),context_end=BASE+pd.Timedelta(seconds=2))

    def test_final_discovery_and_labels_match_frozen_rules_on_shared_period(self):
        f = frames()
        spec = ScreeningSpec(development_start=str(BASE),lockbox_start=str(BASE+pd.Timedelta(hours=1)))
        a = discover_depth_signals(plan(),f,Qualification(),spec)
        b = discover_final(plan(),f,spec,BASE+pd.Timedelta(hours=2))
        cols = [c for c in a if c != 'window']
        pd.testing.assert_frame_equal(a[cols],b[cols])
        old,signals = event_labels(plan(),f,pd.DatetimeIndex([BASE]),spec)
        labels,_,peaks = quote_labels(plan(),f,pd.DatetimeIndex([BASE]),spec,BASE+pd.Timedelta(hours=2))
        pd.testing.assert_frame_equal(old,labels[old.columns])
        original_peaks = peak_quotes(plan(),f,signals)
        pd.testing.assert_frame_equal(original_peaks,peaks)

    def test_final_scheduler_matches_historical_fills_and_reservations(self):
        f = frames()
        signals = pd.DataFrame([dict(pair_id='example:0',event_slug='example',window='test',time=BASE,
            start_time=BASE+pd.Timedelta(seconds=i),end_time=BASE+pd.Timedelta(seconds=i+1),
            monitoring_end_time=BASE+pd.Timedelta(hours=1)) for i in (0,1,3)])
        original = historical_event(signals,{'example:0':ObservationGroup(list(f.values()))},{'example:0':('a','b')},setup(.1))
        final = run_event(signals,{'example:0':FinalGroup(list(f.values()),context_end=BASE+pd.Timedelta(hours=2))},
            {'example:0':('a','b')},setup(.1),entry_end=pd.Timestamp(SPEC.lockbox_start),context_end=pd.Timestamp(SPEC.lockbox_start))
        pd.testing.assert_frame_equal(original[0],final[0])
        pd.testing.assert_frame_equal(original[1],final[1])

    def test_terminal_partial_exit_uses_context_but_disallows_late_entries(self):
        base = pd.Timestamp('2026-01-09',tz='UTC')
        a = _frame([(0,[(.4,100)],[(.39,100)]),(2.1,[(.4,100)],[(.39,100)])],base)
        b = _frame([(0,[(.5,100)],[(.49,100)]),(.55,[(.6,100)],[(.59,100)]),(2.1,[(.6,100)],[(.59,100)])],base)
        context = base+pd.Timedelta(seconds=2.1)
        group = FinalGroup([a,b],context_end=context)
        s = pd.DataFrame([dict(event_slug='example',window='final',time=base,start_time=base+pd.Timedelta(seconds=.5),
                              end_time=base+pd.Timedelta(seconds=.55),monitoring_end_time=base+pd.Timedelta(seconds=1))])
        result = run_scenario(s,group,setup(.1),entry_end=base+pd.Timedelta(seconds=1),context_end=context)
        self.assertEqual(result.status.iloc[0],'partial_unwound')
        self.assertLess(result.pnl_usd.iloc[0],0)
        self.assertEqual(historical_scenario(s,group,setup(.1)).status.iloc[0],'outside_monitoring_window')
        late = s.assign(start_time=base+pd.Timedelta(seconds=.95))
        self.assertFalse(run_scenario(late,group,setup(.1),entry_end=base+pd.Timedelta(seconds=1),context_end=context).attempted.iloc[0])

    def test_ranking_keeps_unknowns_and_is_independent_of_their_labels(self):
        times = [BASE]*4
        part = pd.DataFrame(dict(event_slug=list('abcd'),time=times,model='quoted_gap_rule',score=[4,3,2,1],
                                 development_event=[True]*4,label_known=[False,True,True,True],target=[np.nan,0.,1.,0.]))
        a = selections(scored_references(part,[20261001]))
        altered = part.assign(target=1.,label_known=True)
        b = selections(scored_references(altered,[20261001]))
        pd.testing.assert_frame_equal(a[['scope','top_n','model','time','event_slug']],b[['scope','top_n','model','time','event_slug']])
        chosen = a.loc[a.scope.eq('frozen_supported_universe')&a.model.eq('quoted_gap_rule')&a.top_n.eq(3)]
        self.assertIn('a',chosen.event_slug.tolist())

    def test_zero_positive_metrics_stay_undefined(self):
        part = pd.DataFrame(dict(event_slug=['a'],time=[BASE],score=[0.],label_known=[True],target=[0.],
            value_target=[0.],opportunity_at_scan=[False],future_positive=[False],x_active_tokens=[2.]))
        result = measure(part,part,3)
        self.assertTrue(np.isnan(result['recall']))
        self.assertTrue(np.isnan(result['peak_value_recall']))
        self.assertTrue(np.isnan(result['average_precision']))

    def test_same_forest_alias_canonicalizes_only_numerical_roundoff(self):
        p = pd.DataFrame(dict(event_slug=['a','a'],time=[BASE,BASE],
            model=['fixed_book_random_forest','n3_book_random_forest'],score=[.2,.2+1e-16]))
        result = canonicalize_forest_alias(p)
        self.assertEqual(result.score.iloc[0],result.score.iloc[1])
        p.loc[1,'score'] = .3
        with self.assertRaises(AssertionError):
            canonicalize_forest_alias(p)


if __name__ == '__main__':
    unittest.main()
