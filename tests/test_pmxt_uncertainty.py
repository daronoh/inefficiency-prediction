import json
from pathlib import Path
import shutil
import tempfile
import unittest

import pandas as pd

from src.paths import DATA_DIR,PROJECT_ROOT
from src.common import sha256
from src.data.pmxt_archive import write_json
from src.data.pmxt_books import milliseconds
from src.data.pmxt_uncertainty import BoundedBook,basket_bounds,arrival_bounds
from src.data.pmxt_revalidation import rerun,finalize_labels,BoundedEvent,causal_features
from test_pmxt_source import BASE,book,delta


def seeded(ask=.6,size=10):
    b=BoundedBook(milliseconds(BASE))
    b.apply_batch(BASE,[dict(book(BASE,ask),asks=json.dumps([[str(ask),str(size)]]))])
    return b


class BoundedTest(unittest.TestCase):
    def test_identical_updates_and_snapshots_are_idempotent(self):
        b=BoundedBook(milliseconds(BASE));b.apply_batch(BASE,[book(BASE),book(BASE)])
        t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.6,3),delta(t,.6,3)])
        self.assertEqual(b.depth['SELL'][6000],3)
        self.assertEqual(b.counters['identical_snapshot_replays'],1)
        self.assertEqual(b.counters['identical_level_replays'],1)

    def test_same_time_conflict_preserves_quantity_bounds(self):
        b=seeded(.3);t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.3,5,ask=.3),delta(t,.3,10,ask=.3)])
        self.assertFalse(b.exact('SELL'));self.assertTrue(b.available('SELL'))
        self.assertEqual(b.ladder('SELL','lower').sizes.tolist(),[5])
        self.assertEqual(b.ladder('SELL','upper').sizes.tolist(),[10])
        self.assertFalse(b.drift_intervals)
        bounds=basket_bounds([b,seeded(.6)])
        self.assertEqual(bounds['status'],'positive')
        self.assertAlmostEqual(bounds['lower'],.5);self.assertAlmostEqual(bounds['upper'],1.)

    def test_absolute_update_resolves_only_its_ambiguous_level(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.7,2),delta(t,.7,4),delta(t,.8,1),delta(t,.8,3)])
        b.apply_batch(t+pd.Timedelta(seconds=1),[delta(t+pd.Timedelta(seconds=1),.7,3)])
        self.assertNotIn(7000,b.alternatives['SELL']);self.assertIn(8000,b.alternatives['SELL'])
        self.assertEqual(b.counters['level_conflicts_resolved_by_later_update'],1)

    def test_compatible_snapshot_resolves_tied_sizes(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.6,5),delta(t,.6,10),book(t)])
        self.assertTrue(b.exact('SELL'));self.assertFalse(b.alternatives['SELL'])
        self.assertEqual(b.counters['level_conflicts_resolved_by_snapshot'],1)

    def test_incompatible_snapshot_does_not_select_an_order(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.6,3),book(t)])
        self.assertEqual(b.alternatives['SELL'][6000],{3,10})
        self.assertFalse(b.exact('SELL'));self.assertFalse(b.drift_intervals)

    def test_snapshot_state_change_is_not_a_data_gap(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[book(t,.7)])
        self.assertTrue(b.exact('SELL'));self.assertFalse(b.drift_intervals)

    def test_conflicting_snapshots_supply_outer_bounds(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[book(t,.6),book(t,.7)])
        self.assertEqual(b.alternatives['SELL'][6000],{0,10})
        self.assertEqual(b.alternatives['SELL'][7000],{0,10})
        self.assertTrue(b.available('SELL'));self.assertFalse(b.exact('SELL'))

    def test_best_price_cannot_invent_depth(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.8,5,ask=.5)])
        self.assertFalse(b.available('SELL'))
        self.assertNotIn(5000,b.depth['SELL'])
        self.assertEqual(b.drift_intervals,[('SELL',milliseconds(BASE),milliseconds(t))])
        self.assertTrue(b.available('BUY'))
        b.apply_batch(t+pd.Timedelta(seconds=1),[book(t+pd.Timedelta(seconds=1))])
        self.assertTrue(b.available('SELL'))

    def test_unreliable_bid_does_not_discard_reliable_buy_depth(self):
        b=seeded(.3);t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[dict(delta(t,.8,5,ask=.3),best_bid=.1)])
        self.assertFalse(b.available('BUY'));self.assertTrue(b.available('SELL'))
        self.assertEqual(basket_bounds([b,seeded(.6)])['status'],'positive')

    def test_temporary_cross_in_a_batch_is_not_emitted(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        rows=[dict(delta(t,.7,5,ask=.8),side='BUY',best_bid=.7),
            dict(delta(t,.6,0,ask=.8),best_bid=.7),dict(delta(t,.8,10,ask=.8),best_bid=.7)]
        b.apply_batch(t,rows)
        self.assertTrue(b.available('SELL'));self.assertTrue(b.available('BUY'))
        self.assertEqual(b.counters['crossed_final_group'],0)

    def test_persistent_cross_is_unknown_until_new_snapshot(self):
        b=seeded();t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[dict(delta(t,.7,5,ask=.6),side='BUY',best_bid=.7)])
        self.assertFalse(b.available('SELL'));self.assertFalse(b.available('BUY'))
        self.assertFalse(b.drift_intervals)  # Clearly introduced at current source time.
        b.apply_batch(t+pd.Timedelta(seconds=1),[book(t+pd.Timedelta(seconds=1))])
        self.assertTrue(b.available('SELL'))

    def test_counterfactual_past_mask_does_not_change_causal_cross_detection(self):
        b=seeded();b.past_blocked['BUY']=True;t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[dict(delta(t,.7,5,ask=.6),side='BUY',best_bid=.7)])
        self.assertEqual(b.counters['crossed_final_group'],1)

    def test_known_legs_can_rule_out_basket_with_missing_leg(self):
        unknown=BoundedBook(milliseconds(BASE))
        out=basket_bounds([seeded(.7),seeded(.4),unknown])
        self.assertEqual(out['status'],'blocked');self.assertEqual(out['upper'],0.)
        self.assertEqual(out['reason'],'known_legs_cost_at_least_payout')

    def test_known_empty_or_shallow_leg_is_a_blocker(self):
        unknown=BoundedBook(milliseconds(BASE))
        self.assertEqual(basket_bounds([seeded(.6,size=3),unknown])['status'],'blocked')
        self.assertEqual(basket_bounds([seeded(.6,size=0),unknown])['status'],'blocked')

    def test_unknown_leg_is_never_assumed_free_for_a_trade(self):
        out=basket_bounds([seeded(.3),BoundedBook(milliseconds(BASE))])
        self.assertEqual(out['status'],'unknown');self.assertIsNone(out['upper']);self.assertIsNone(out['candidate'])

    def test_optional_cheap_level_can_leave_occurrence_unknown(self):
        b=seeded(.8);t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.3,0,ask=.3),delta(t,.3,10,ask=.3)])
        out=basket_bounds([b,seeded(.6)])
        self.assertEqual(out['status'],'unknown');self.assertEqual(out['lower'],0.)
        self.assertAlmostEqual(out['upper'],1.)

    def test_all_possible_unprofitable_depth_can_be_certified(self):
        b=seeded(.8);t=BASE+pd.Timedelta(seconds=1)
        b.apply_batch(t,[delta(t,.8,5,ask=.8),delta(t,.8,10,ask=.8)])
        self.assertEqual(basket_bounds([b,seeded(.3)])['status'],'blocked')

    def test_different_order_of_tied_rows_cannot_choose_profitable_state(self):
        t=BASE+pd.Timedelta(seconds=1);rows=[delta(t,.3,0,ask=.3),delta(t,.3,10,ask=.3)]
        outcomes=[]
        for order in [rows,list(reversed(rows))]:
            b=seeded(.8);b.apply_batch(t,order);o=basket_bounds([b,seeded(.6)])
            outcomes.append((o['status'],o['lower'],o['upper']))
        self.assertEqual(outcomes[0],outcomes[1])

    def test_fixed_arrival_checks_use_quantity_and_limits(self):
        a,b=seeded(.3),seeded(.6);signal=dict(quantity=10,limits=[.3,.6])
        self.assertEqual(arrival_bounds(signal,[a,b])['status'],'display_supported')
        t=BASE+pd.Timedelta(milliseconds=100)
        a.apply_batch(t,[delta(t,.3,5,ask=.3),delta(t,.3,10,ask=.3)])
        self.assertEqual(arrival_bounds(signal,[a,b])['status'],'unknown')
        a.apply_batch(t+pd.Timedelta(milliseconds=1),[delta(t+pd.Timedelta(milliseconds=1),.3,3,ask=.3)])
        self.assertEqual(arrival_bounds(signal,[a,b])['status'],'insufficient_displayed_depth')
        self.assertEqual(signal['quantity'],10);self.assertEqual(signal['limits'],[.3,.6])


class LabelTest(unittest.TestCase):
    def test_hindsight_quality_masks_never_enter_features(self):
        a,b=seeded(.3),seeded(.6)
        a.past_blocked['SELL']=True
        books={'a':a,'b':b}
        state=BoundedEvent(dict(event_id='e',event_slug='test',classification='negrisk'),
            [dict(package_id='p',token_ids=['a','b'])],books)
        self.assertEqual(state.status,'unknown')
        row=causal_features({'e':state},books,BASE+pd.Timedelta(hours=1))[0]
        self.assertEqual(row['snapshot_occurrence_status'],'positive')
        self.assertAlmostEqual(row['snapshot_value_lower_usd'],1.)
        self.assertTrue(a.past_blocked['SELL'])
        self.assertEqual(state.status,'unknown')

    def test_partial_positive_does_not_create_exact_value(self):
        frame=finalize_labels([dict(peak_value_lower_usd=2.,peak_value_upper_usd=None,ruled_out_seconds=100.)])
        self.assertEqual(frame.iloc[0].label_occurrence,1)
        self.assertTrue(pd.isna(frame.iloc[0].label_peak_value_usd))
        self.assertEqual(frame.iloc[0].value_target_status,'lower_bound')

    def test_negative_requires_whole_window_proof(self):
        frame=finalize_labels([dict(peak_value_lower_usd=0.,peak_value_upper_usd=0.,ruled_out_seconds=3600.),
            dict(peak_value_lower_usd=0.,peak_value_upper_usd=None,ruled_out_seconds=3599.999)])
        self.assertEqual(frame.iloc[0].label_occurrence,0)
        self.assertEqual(frame.iloc[0].label_peak_value_usd,0.)
        self.assertTrue(pd.isna(frame.iloc[1].label_occurrence))

    def test_maximum_can_be_exact_despite_local_size_ambiguity(self):
        frame=finalize_labels([dict(peak_value_lower_usd=3.,peak_value_upper_usd=3.,ruled_out_seconds=100.)])
        self.assertEqual(frame.iloc[0].label_peak_value_usd,3.)


class EndToEndTest(unittest.TestCase):
    def test_missing_leg_can_have_certified_negative_and_empty_buffer_hour_is_valid(self):
        parent=DATA_DIR/'pmxt';parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='blocker_test_',dir=parent) as directory:
            root=Path(directory);(root/'raw').mkdir()
            results=PROJECT_ROOT/'results/pmxt_pilot'/root.name
            self.addCleanup(lambda:self.cleanup(results))
            pd.DataFrame([dict(event_id='e',event_slug='test',classification='negrisk')]).to_parquet(root/'events.parquet',index=False)
            pd.DataFrame([dict(package_id='p',event_id='e',token_ids=['a','b','c'])]).to_parquet(root/'packages.parquet',index=False)
            pd.DataFrame([dict(market_id='m')]).to_parquet(root/'markets.parquet',index=False)
            start=BASE+pd.Timedelta(hours=1);end=BASE+pd.Timedelta(hours=2)
            write_json(root/'plan.json',dict(start=start,end=end+pd.Timedelta(hours=1),warmup_start=BASE,
                assets=['a','b','c'],metadata_mode='test',files_sha256={n:sha256(root/f'{n}.parquet') for n in ['events','markets','packages']}))
            warm=BASE+pd.Timedelta(minutes=59)
            first=pd.DataFrame([book(warm,.7,'a'),book(warm,.4,'b')])
            quiet=pd.DataFrame([dict(book(start+pd.Timedelta(minutes=30),.7,'a'),event_type='last_trade_price')])
            for i,f in enumerate([first,quiet,first.iloc[:0].copy()]):
                path=root/'raw'/f'2026-05-04T{i:02}.parquet';f['archive_row']=range(len(f));f.to_parquet(path,index=False)
                write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=['a','b','c'],transfer_bytes=1))
            summary=rerun(root,analysis_end=end.isoformat())
            labels=pd.read_parquet(root/'source_validation_v3/hourly_label_bounds.parquet').iloc[0]
            self.assertEqual(summary['full_book_event_hours'],0)
            self.assertEqual(summary['occurrence_negative_hours'],1)
            self.assertEqual(labels.ruled_out_seconds,3600.)
            self.assertEqual(labels.label_peak_value_usd,0.)
            self.assertEqual(labels.blocked_with_incomplete_books_seconds,3600.)

    def test_late_messages_no_future_features_fixed_latency_and_separate_results(self):
        parent=DATA_DIR/'pmxt';parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='bounded_test_',dir=parent) as directory:
            root=Path(directory);(root/'raw').mkdir()
            results=PROJECT_ROOT/'results/pmxt_pilot'/root.name
            self.addCleanup(lambda:self.cleanup(results))
            pd.DataFrame([dict(event_id='e',event_slug='test',classification='negrisk')]).to_parquet(root/'events.parquet',index=False)
            pd.DataFrame([dict(package_id='p',event_id='e',token_ids=['a','b'])]).to_parquet(root/'packages.parquet',index=False)
            pd.DataFrame([dict(market_id='m')]).to_parquet(root/'markets.parquet',index=False)
            start=BASE+pd.Timedelta(hours=1);end=BASE+pd.Timedelta(hours=2)
            write_json(root/'plan.json',dict(start=start,end=end+pd.Timedelta(hours=1),warmup_start=BASE,
                assets=['a','b'],metadata_mode='test',files_sha256={n:sha256(root/f'{n}.parquet') for n in ['events','markets','packages']}))
            warm=BASE+pd.Timedelta(minutes=59);change=start+pd.Timedelta(milliseconds=50);late=end+pd.Timedelta(seconds=1)
            frames=[pd.DataFrame([book(warm,token=t) for t in ['a','b']]),
                pd.DataFrame([delta(change,.3,0,ask=.8),delta(change,.8,10,ask=.8)]),
                pd.DataFrame([delta(start,.6,0,ask=.3,receive=late),delta(start,.3,10,ask=.3,receive=late)])]
            for i,f in enumerate(frames):
                path=root/'raw'/f'2026-05-04T{i:02}.parquet';f['archive_row']=range(len(f));f.to_parquet(path,index=False)
                write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=['a','b'],transfer_bytes=1))
            s=rerun(root,analysis_end=end.isoformat());output=root/'source_validation_v3'
            feature=pd.read_parquet(output/'hourly_features.parquet').iloc[0]
            labels=pd.read_parquet(output/'hourly_label_bounds.parquet').iloc[0]
            self.assertEqual(feature.snapshot_value_lower_usd,0.)
            self.assertEqual(feature.snapshot_occurrence_status,'blocked')
            self.assertEqual(labels.label_occurrence,1);self.assertAlmostEqual(labels.label_peak_value_usd,1.)
            checks=pd.read_csv(results/'source_validation_v3/arrival_checks.csv')
            self.assertEqual(checks.loc[checks.latency_ms==0,'status'].iloc[0],'display_supported')
            self.assertEqual(checks.loc[checks.latency_ms==100,'status'].iloc[0],'insufficient_displayed_depth')
            self.assertFalse(s['net_pnl_estimated']);self.assertFalse((root/'source_validation_v2').exists())

    @staticmethod
    def cleanup(path):
        if not path.resolve().is_relative_to((PROJECT_ROOT/'results/pmxt_pilot').resolve()):raise ValueError('Unsafe cleanup')
        if path.exists():shutil.rmtree(path)


if __name__=='__main__':unittest.main()
