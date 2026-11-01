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
from src.data.pmxt_source import SourceBook,QualityMask,source_batches,fixed_arrival_check
from src.data.pmxt_validation import validate

BASE=pd.Timestamp('2026-05-04T00:00Z')


def book(at,ask=.6,token='a',receive=None):
    return dict(asset_id=token,event_type='book',timestamp=at,timestamp_received=receive or at,
        bids='[["0.2","10"]]',asks=json.dumps([[str(ask),'10']]),
        archive_hour=at.floor('h'),archive_row=0)


def delta(at,price,size,*,ask=.6,token='a',receive=None):
    return dict(asset_id=token,event_type='price_change',timestamp=at,timestamp_received=receive or at,
        price=price,size=size,side='SELL',best_bid=.2,best_ask=ask,archive_hour=at.floor('h'),archive_row=1)


class SourceTest(unittest.TestCase):
    def setUp(self): self.b=SourceBook(milliseconds(BASE))

    def apply(self,second,rows):
        stamp=BASE+pd.Timedelta(seconds=second)
        self.b.apply_batch(stamp,rows(stamp))

    def test_source_order_differs_from_receipt_order(self):
        early=BASE+pd.Timedelta(seconds=1)
        late=BASE+pd.Timedelta(seconds=2)
        f=pd.DataFrame([book(late),book(early,receive=BASE+pd.Timedelta(seconds=3))])
        self.assertEqual(next(source_batches(f))[0][0],early)

    def test_snapshot_checks_before_replacement(self):
        self.b.strict_checkpoints=True
        self.apply(0,lambda t:[book(t)])
        self.apply(1,lambda t:[delta(t,.7,12)])
        self.apply(2,lambda t:[dict(book(t),asks='[["0.6","10"],["0.7","12"]]')])
        self.assertEqual(self.b.counters['checkpoint_exact'],1)
        self.apply(3,lambda t:[book(t)])
        self.assertEqual(self.b.counters['checkpoint_disagreements'],1)
        self.assertEqual(self.b.asks,{6000:10})
        self.assertTrue(QualityMask(self.b.bad_intervals).contains(milliseconds(BASE)+2500))

    def test_new_snapshot_state_is_not_automatically_a_missing_delta(self):
        self.apply(0,lambda t:[book(t)])
        self.apply(1,lambda t:[book(t,.7)])
        self.assertTrue(self.b.usable)
        self.assertEqual(self.b.counters['checkpoint_disagreements'],1)
        self.assertFalse(self.b.bad_intervals)

    def test_repeated_snapshot_does_not_create_extra_checks(self):
        self.apply(0,lambda t:[book(t),book(t)])
        self.assertEqual(self.b.counters['repeated_snapshot_rows'],1)
        self.assertEqual(self.b.counters['checkpoint_comparisons'],0)

    def test_conflicting_same_source_revisions_are_unknown(self):
        self.apply(0,lambda t:[book(t)])
        self.apply(1,lambda t:[delta(t,.6,2),delta(t,.6,3)])
        self.assertFalse(self.b.usable)
        self.apply(2,lambda t:[book(t)])
        self.assertTrue(self.b.usable)

    def test_conflicting_same_source_snapshots_are_unknown(self):
        self.apply(0,lambda t:[book(t),book(t,.7)])
        self.assertFalse(self.b.usable)

    def test_update_size_replaces_and_zero_deletes(self):
        self.apply(0,lambda t:[book(t)])
        self.apply(1,lambda t:[delta(t,.6,3)])
        self.assertEqual(self.b.asks[6000],3)
        self.apply(2,lambda t:[delta(t,.6,0,ask=1)])
        self.assertFalse(self.b.asks)

    def test_no_delta_only_seed_or_old_full_book_seed(self):
        self.apply(0,lambda t:[delta(t,.6,10)])
        self.assertFalse(self.b.usable)
        old=BASE-pd.Timedelta(seconds=1)
        b=SourceBook(milliseconds(BASE));b.apply_batch(old,[book(old)])
        self.assertFalse(b.usable)

    def test_same_time_snapshot_update_disagreement_is_unknown(self):
        self.apply(0,lambda t:[book(t),delta(t,.6,3)])
        self.assertFalse(self.b.usable)

    def test_unknown_and_recovery_after_bad_best_quote(self):
        self.apply(0,lambda t:[book(t)])
        self.apply(1,lambda t:[delta(t,.7,3,ask=.5)])
        self.assertFalse(self.b.usable)
        self.apply(2,lambda t:[book(t)])
        self.assertTrue(self.b.usable)

    def test_crossed_full_book_is_unknown_not_a_negative_example(self):
        self.apply(0,lambda t:[dict(book(t,.1),bids='[["0.2","10"]]')])
        self.assertFalse(self.b.usable)
        self.assertEqual(self.b.counters['crossed_groups'],1)

    def test_mask_is_half_open_and_merges(self):
        mask=QualityMask([(1,3),(2,4),(6,8)])
        self.assertEqual(mask.intervals,[(1,4),(6,8)])
        self.assertTrue(mask.contains(1));self.assertFalse(mask.contains(4))

    def test_fixed_quantity_and_limit_are_not_resized_at_arrival(self):
        a=SourceBook(milliseconds(BASE));b=SourceBook(milliseconds(BASE))
        a.apply_batch(BASE,[book(BASE,.3)]);b.apply_batch(BASE,[book(BASE,.6,token='b')])
        signal=dict(quantity=10,limits=[.3,.6])
        self.assertEqual(fixed_arrival_check(signal,[a.raw_book(BASE),b.raw_book(BASE)])['status'],'display_supported')
        later=BASE+pd.Timedelta(milliseconds=100)
        a.apply_batch(later,[delta(later,.3,0,ask=.4),delta(later,.4,10,ask=.4)])
        self.assertEqual(fixed_arrival_check(signal,[a.raw_book(later),b.raw_book(later)])['status'],'insufficient_displayed_depth')
        self.assertEqual(signal['quantity'],10);self.assertEqual(signal['limits'],[.3,.6])
        self.assertEqual(fixed_arrival_check(signal,[None,b.raw_book(later)])['status'],'unknown')


class EndToEndTest(unittest.TestCase):
    def test_late_records_boundary_features_checkpoint_masks_and_arrival_order(self):
        parent=DATA_DIR/'pmxt';parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='source_test_',dir=parent) as directory:
            root=Path(directory);(root/'raw').mkdir()
            results=PROJECT_ROOT/'results/pmxt_pilot'/root.name
            self.addCleanup(lambda:self.cleanup(results))
            pd.DataFrame([dict(event_id='e',event_slug='test',classification='negrisk')]).to_parquet(root/'events.parquet',index=False)
            pd.DataFrame([dict(package_id='p',event_id='e',token_ids=['a','b'])]).to_parquet(root/'packages.parquet',index=False)
            pd.DataFrame([dict(market_id='m')]).to_parquet(root/'markets.parquet',index=False)
            start=BASE+pd.Timedelta(hours=1);end=BASE+pd.Timedelta(hours=2)
            plan=dict(start=start,end=end+pd.Timedelta(hours=1),warmup_start=BASE,assets=['a','b'],
                selected={'negrisk':1},metadata_mode='test',
                files_sha256={n:sha256(root/f'{n}.parquet') for n in ['events','markets','packages']})
            write_json(root/'plan.json',plan)
            warm=BASE+pd.Timedelta(minutes=59);finish=end-pd.Timedelta(minutes=1)
            change=start+pd.Timedelta(milliseconds=50);late=end+pd.Timedelta(seconds=1)
            frames=[pd.DataFrame([book(warm,token=t) for t in ['a','b']]),
                pd.DataFrame([delta(change,.3,0,ask=.8),delta(change,.8,10,ask=.8),
                    book(finish,.8),book(finish,.6,token='b')]),
                pd.DataFrame([delta(start,.6,0,ask=.3,receive=late),delta(start,.3,10,ask=.3,receive=late)])]
            for i,f in enumerate(frames):
                path=root/'raw'/f'2026-05-04T{i:02}.parquet'
                f['archive_row']=range(len(f));f.to_parquet(path,index=False)
                write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=['a','b'],transfer_bytes=1))
            summary=validate(root,analysis_end=end.isoformat())
            output=root/'source_validation_v2'
            features=pd.read_parquet(output/'hourly_features.parquet');labels=pd.read_parquet(output/'hourly_quote_labels.parquet')
            self.assertAlmostEqual(features.iloc[0].quoted_gap,-.2)
            self.assertAlmostEqual(labels.iloc[0].conditional_peak_gross_usd,1.)
            self.assertEqual(labels.iloc[0].label_conditional_gross,1)
            self.assertEqual(summary['late_records_for_analysis'],2)
            self.assertEqual(summary['checkpoint_exact_fraction'],1.)
            checks=pd.read_csv(results/'source_validation_v2/arrival_checks.csv')
            self.assertEqual(checks.loc[checks.latency_ms==0,'status'].iloc[0],'display_supported')
            for latency in [50,100,250,500]:
                self.assertEqual(checks.loc[checks.latency_ms==latency,'status'].iloc[0],'insufficient_displayed_depth')
            self.assertFalse(summary['net_pnl_estimated'])

    @staticmethod
    def cleanup(path):
        if not path.resolve().is_relative_to((PROJECT_ROOT/'results/pmxt_pilot').resolve()):
            raise ValueError('Refusing cleanup outside test PMXT results')
        if path.exists(): shutil.rmtree(path)


if __name__=='__main__':unittest.main()
