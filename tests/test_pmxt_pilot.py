"""Adapter invariants, independently of downloads and the saved Dome experiment."""
import io
import json
from pathlib import Path
import unittest
import tempfile
from unittest.mock import patch

import pandas as pd

from src.paths import DATA_DIR,PROJECT_ROOT
from src.data.pmxt_archive import RangeFile,TransferBudget,archive_url,isolated
from src.data.pmxt_books import TokenBook,milliseconds,receive_batches
from src.data.pmxt_audit import conditional_quote
from src.data.pmxt_audit import audit
from src.common import sha256
from src.data.pmxt_archive import write_json

BASE = pd.Timestamp('2026-05-04T00:00:00Z')


def message(kind='book',source=0,received=0,**fields):
    row = dict(event_type=kind,timestamp=BASE+pd.Timedelta(milliseconds=source),
        timestamp_received=BASE+pd.Timedelta(milliseconds=received),asset_id='a',archive_row=0)
    if kind=='book': row.update(bids='[["0.4","20"]]',asks='[["0.6","20"]]')
    if kind=='price_change': row.update(side='SELL',price=.6,size=10,best_bid=.4,best_ask=.6)
    row.update(fields)
    return row


class BookTest(unittest.TestCase):
    def setUp(self): self.book = TokenBook(milliseconds(BASE))

    def test_absolute_size_and_deletion(self):
        self.book.apply(message())
        self.book.apply(message('price_change',1,1,size=7))
        self.book.apply(message('price_change',1,1,size=7))
        self.assertEqual(self.book.asks,{6000:7})
        self.book.apply(message('price_change',2,2,size=0,best_ask=1))
        self.book.validate_batch()
        self.assertEqual(self.book.asks,{})
        self.assertTrue(self.book.usable)

    def test_late_snapshot_rebases_already_received_updates(self):
        self.book.apply(message('price_change',20,30,size=9))
        self.book.apply(message(source=10,received=40))
        self.book.validate_batch()
        self.assertEqual(self.book.asks[6000],9)
        self.assertTrue(self.book.usable)
        self.book.apply(message('price_change',50,60,size=3))
        self.assertEqual(self.book.asks[6000],3)

    def test_rebase_preserves_deletion(self):
        self.book.apply(message('price_change',20,30,size=0,best_ask=1))
        self.book.apply(message(source=10,received=40))
        self.book.validate_batch()
        self.assertFalse(self.book.asks)
        self.assertTrue(self.book.usable)

    def test_old_revision_on_other_level_is_not_dropped(self):
        self.book.apply(message())
        self.book.apply(message('price_change',20,30,size=9))
        self.book.apply(message('price_change',10,40,price=.7,size=4))
        self.assertEqual(self.book.asks[7000],4)
        self.book.apply(message('price_change',15,50,size=8))
        self.assertEqual(self.book.asks[6000],9)

    def test_snapshot_does_not_roll_back_newer_baseline(self):
        self.book.apply(message(source=20,received=30))
        self.book.apply(message(source=10,received=40,asks='[["0.8","20"]]'))
        self.assertEqual(self.book.asks,{6000:20})

    def test_unknown_seed_and_recovery(self):
        self.book.apply(message(source=-100,received=0))
        self.assertFalse(self.book.usable)
        self.book.apply(message('price_change',1,1))
        self.assertFalse(self.book.usable)
        self.book.apply(message(source=2,received=2))
        self.assertTrue(self.book.usable)

    def test_mismatch_needs_fresh_snapshot(self):
        self.book.apply(message())
        self.book.apply(message('price_change',1,1,best_ask=.5))
        self.book.validate_batch()
        self.assertFalse(self.book.usable)
        self.book.apply(message('price_change',2,2,best_ask=.6))
        self.book.validate_batch()
        self.assertFalse(self.book.usable)
        self.book.apply(message(source=3,received=3))
        self.book.validate_batch()
        self.assertTrue(self.book.usable)

    def test_same_millisecond_batch(self):
        self.book.apply(message())
        self.book.apply(message('price_change',1,1,price=.5,size=2,best_ask=.6))
        self.book.apply(message('price_change',1,1,price=.5,size=0))
        self.book.validate_batch()
        self.assertTrue(self.book.usable)

    def test_conflicting_same_source_best_quotes_do_not_depend_on_archive_row_order(self):
        for order in [(0,1),(1,0)]:
            book = TokenBook(milliseconds(BASE))
            book.apply(message())
            changes = [message('price_change',1,1,price=.7,size=2,best_bid=.5),
                message('price_change',1,1,price=.8,size=2,best_bid=.4)]
            for i in order: book.apply(changes[i])
            book.validate_batch()
            self.assertTrue(book.usable)

    def test_old_tick_revision_does_not_override_new_tick(self):
        self.book.apply(message('tick_size_change',20,30,new_tick_size=.01))
        self.book.apply(message('tick_size_change',10,40,new_tick_size=.001))
        self.assertEqual(self.book.tick_size,.01)

    def test_trade_does_not_decrement_depth(self):
        self.book.apply(message())
        self.book.apply(message('last_trade_price',1,1,price=.6,size=15,side='BUY'))
        self.assertEqual(self.book.asks,{6000:20})

    def test_gap_and_clock_guards(self):
        self.book.apply(message())
        self.book.invalidate(BASE+pd.Timedelta(hours=1))
        self.book.apply(message(source=1,received=3_600_001))
        self.assertFalse(self.book.usable)
        with self.assertRaises(ValueError): self.book.apply(message(received=2))

    def test_malformed_and_crossed_books(self):
        self.book.apply(message(asks='[["0.6","-1"]]'))
        self.assertFalse(self.book.usable)
        self.book.apply(message(source=1,received=1,bids='[["0.8","20"]]'))
        self.assertIsNone(conditional_quote([self.book.raw_book(BASE)]*2))

    def test_receive_sort_never_uses_source_time(self):
        frame = pd.DataFrame([message(source=1,received=20),message(source=5,received=10)])
        batches = list(receive_batches(frame))
        self.assertEqual(batches[0][1][0]['timestamp'],BASE+pd.Timedelta(milliseconds=5))


class Response:
    def __init__(self,code=206,data=b'PAR1',range='bytes 96-99/100',etag='a'):
        self.status_code=code;self.raw=io.BytesIO(data)
        self.headers={'Content-Range':range,'ETag':etag}
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def close(self): pass


class Session:
    def __init__(self,responses): self.responses=iter(responses); self.calls=[]
    def get(self,*args,**kwargs): self.calls.append(kwargs);return next(self.responses)
    def close(self): pass


def tail(**kw):
    return Response(data=b'0000PAR1',range='bytes 92-99/100',**kw)


class ArchiveTest(unittest.TestCase):
    def test_path_guard(self):
        for p in [DATA_DIR/'raw/orderbooks',DATA_DIR/'pmxt',PROJECT_ROOT/'results/baseline']:
            with self.assertRaises(ValueError): isolated(p)
        self.assertEqual(isolated(DATA_DIR/'pmxt/pilot/x'),(DATA_DIR/'pmxt/pilot/x').resolve())
        with self.assertRaises(ValueError): isolated(DATA_DIR/'pmxt/pilot/../../raw')

    def test_hour_guard(self):
        with self.assertRaises(ValueError): archive_url('2026-05-04')
        with self.assertRaises(ValueError): archive_url('2026-05-04T00:00:01Z')
        self.assertTrue(archive_url(BASE).endswith('2026-05-04T00.parquet'))

    def test_strict_ranges_and_seek(self):
        session = Session([tail(),Response()])
        with RangeFile(archive_url(BASE),session=session) as f:
            f.seek(-4,2);self.assertEqual(f.read(4),b'PAR1')
            self.assertEqual(f.tell(),100)
            self.assertEqual(f.read(1),b'')
            self.assertEqual(session.calls[1]['headers']['If-Match'],'a')
            with self.assertRaises(ValueError): f.seek(-1)
            with self.assertRaises(ValueError): f.seek(0,4)

    def test_refuse_whole_file_response(self):
        with self.assertRaises(RuntimeError):
            RangeFile(archive_url(BASE),session=Session([tail(code=200)]))

    def test_changed_object_refused(self):
        with RangeFile(archive_url(BASE),session=Session([tail(),Response(etag='b')])) as f:
            f.seek(96)
            with self.assertRaises(ValueError): f.read(4)

    def test_transfer_budget(self):
        budget = TransferBudget(7)
        with self.assertRaises(RuntimeError): RangeFile(archive_url(BASE),budget,Session([]))
        self.assertEqual(budget.used,0)

    def test_short_range_refused(self):
        with self.assertRaises(ValueError):
            RangeFile(archive_url(BASE),session=Session([Response(data=b'PAR1',range='bytes 92-99/100')]))


class AuditTest(unittest.TestCase):
    def test_boundary_features_do_not_see_labels_and_gaps_do_not_become_negatives(self):
        parent = DATA_DIR/'pmxt'
        parent.mkdir(exist_ok=True,parents=True)
        with tempfile.TemporaryDirectory(prefix='adapter_test_',dir=parent) as directory:
            root = Path(directory)
            result_root = PROJECT_ROOT/'results/pmxt_pilot'/root.name
            # TemporaryDirectory cleanup targets are explicit children of the
            # pilot roots, never computed Dome paths.
            result_root.mkdir(parents=True,exist_ok=False)
            self.addCleanup(lambda: self.remove_results(result_root))
            (root/'raw').mkdir()
            pd.DataFrame([dict(event_id='e',event_slug='test-event',classification='negrisk')]).to_parquet(root/'events.parquet',index=False)
            pd.DataFrame([dict(package_id='p',event_id='e',token_ids=['a','b'])]).to_parquet(root/'packages.parquet',index=False)
            pd.DataFrame([dict(market_id='m')]).to_parquet(root/'markets.parquet',index=False)
            plan = dict(start=BASE+pd.Timedelta(hours=1),end=BASE+pd.Timedelta(hours=3),
                warmup_start=BASE,assets=['a','b'],selected={'negrisk':1},
                files_sha256={n:sha256(root/f'{n}.parquet') for n in ['events','markets','packages']})
            write_json(root/'plan.json',plan)
            warm = pd.DataFrame([message(source=3_500_000,received=3_500_000,
                asset_id=t,bids='[["0.2","10"]]',asks='[["0.6","10"]]') for t in ['a','b']])
            later = pd.DataFrame([message('price_change',3_600_000,3_600_000,
                price=.6,size=0,best_bid=.2,best_ask=.3,archive_row=0),
                message('price_change',3_600_000,3_600_000,price=.3,size=10,
                    best_bid=.2,best_ask=.3,archive_row=1)])
            for hour,frame in [('00',warm),('01',later)]:
                path = root/'raw'/f'2026-05-04T{hour}.parquet'
                frame.to_parquet(path,index=False)
                write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=['a','b'],transfer_bytes=1))
            result = audit(root)
            self.assertEqual(result['analysis_hours'],1)
            self.assertFalse(result['full_week_complete'])
            features = pd.read_parquet(root/'hourly_features.parquet')
            labels = pd.read_parquet(root/'hourly_quote_audit.parquet')
            self.assertAlmostEqual(features.iloc[0].quoted_gap,-.2)
            self.assertAlmostEqual(labels.iloc[0].conditional_peak_gross_usd,1.)
            self.assertEqual(labels.iloc[0].label_conditional_gross,1)
            self.assertEqual(labels.iloc[0].fully_observed_seconds,3600)
            self.assertFalse(result['net_profit_estimated'])

    @staticmethod
    def remove_results(path):
        import shutil
        if not path.resolve().is_relative_to((PROJECT_ROOT/'results/pmxt_pilot').resolve()):
            raise ValueError('Refusing test cleanup outside PMXT results')
        shutil.rmtree(path)


if __name__ == '__main__': unittest.main()
