"""Feasibility pilot safeguards and unknown-interval attribution."""
import json
import io
from pathlib import Path
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq

from src.common import sha256
from src.paths import DATA_DIR, PROJECT_ROOT
from src.data.pmxt_archive import write_json
from src.data.pmxt_diagnostics import AuditBook, ask_reason, audit, explain_state
from src.data.pmxt_multiday import SharedBudget, FileBudget, RetryingRangeFile, freeze, extract_bounded
from src.data.pmxt_revalidation import BoundedEvent, rerun
from src.data.pmxt_feasibility import assess
from src.data.pmxt_bootstrap import seed_statistics
from test_pmxt_source import BASE, book


class BudgetTest(unittest.TestCase):
    def test_parallel_files_have_independent_accounting_and_shared_hard_limit(self):
        shared = SharedBudget(3000)
        files = [FileBudget(shared) for _ in range(3)]
        with ThreadPoolExecutor(max_workers=3) as executor:
            list(executor.map(lambda b: [b.charge(1) for _ in range(1000)], files))
        self.assertEqual(shared.used,3000)
        self.assertEqual([b.used for b in files],[1000,1000,1000])
        with self.assertRaises(RuntimeError):
            files[0].charge(1)
        self.assertEqual(files[0].used,1000)

    def test_network_disconnect_is_retried_but_schema_error_is_not(self):
        reader = object.__new__(RetryingRangeFile)
        reader.session=MagicMock()
        with patch.object(RetryingRangeFile,'request_range', side_effect=[requests.ConnectionError('closed'), b'ok']) as fetch:
            with patch('src.data.pmxt_multiday.time.sleep'):
                self.assertEqual(reader.fetch(0,2),b'ok')
            self.assertEqual(fetch.call_count,2)
        with patch.object(RetryingRangeFile,'request_range', side_effect=ValueError('bad range')) as fetch:
            with self.assertRaises(ValueError):
                reader.fetch(0,2)
            self.assertEqual(fetch.call_count,1)

    def test_unconditional_range_checks_etag_length_and_status(self):
        reader=object.__new__(RetryingRangeFile)
        reader.url='https://r2v2.pmxt.dev/polymarket_orderbook_2026-05-04T07.parquet'
        reader.budget=SharedBudget(100)
        reader.session=MagicMock()
        reader.size=8
        reader.etag='fixed'
        reader.requests=0
        response=MagicMock()
        response.status_code=206
        response.headers={'Content-Range':'bytes 0-3/8','Content-Length':'4','ETag':'fixed'}
        response.raw=io.BytesIO(b'abcd')
        reader.session.get.return_value.__enter__.return_value=response
        self.assertEqual(reader.request_range(0,4),b'abcd')
        self.assertNotIn('If-Match',reader.session.get.call_args.kwargs['headers'])
        response.headers['ETag']='changed'
        with self.assertRaises(ValueError):
            reader.request_range(0,4)
        response.status_code=200
        with self.assertRaises(RuntimeError):
            reader.request_range(0,4)

    def test_wall_clock_limit_stops_trickling_response(self):
        reader=object.__new__(RetryingRangeFile)
        reader.url='https://r2v2.pmxt.dev/polymarket_orderbook_2026-05-04T07.parquet'
        reader.budget=SharedBudget(100)
        reader.session=MagicMock()
        reader.size,reader.etag,reader.requests=8,'fixed',0
        response=MagicMock(status_code=206)
        response.headers={'Content-Range':'bytes 0-3/8','Content-Length':'4','ETag':'fixed'}
        response.raw=io.BytesIO(b'abcd')
        reader.session.get.return_value.__enter__.return_value=response
        with patch('src.data.pmxt_multiday.time.monotonic',side_effect=[0,61]):
            with self.assertRaises(requests.ConnectionError):
                reader.request_range(0,4)


class ExplanationTest(unittest.TestCase):
    def make_book(self):
        b=AuditBook(0)
        b.__post_init__()
        return b

    def test_local_conflict_and_hindsight_mask_are_distinct(self):
        b=self.make_book()
        self.assertEqual(ask_reason(b),'missing_initial_snapshot')
        b.apply_batch(BASE,[book(BASE,.4)])
        self.assertIsNone(ask_reason(b))
        b.alternatives['SELL'][4000]={0.,10.}
        self.assertEqual(ask_reason(b),'unresolved_quantity_alternatives')
        b.past_blocked['SELL']=True
        self.assertEqual(ask_reason(b),'retrospective_best_price_drift')

    def test_bid_fault_does_not_become_ask_uncertainty(self):
        b=self.make_book()
        b.apply_batch(BASE,[book(BASE,.4)])
        b.faults['BUY']=True
        b.fault_reason['BUY']='best_price_contradiction'
        self.assertIsNone(ask_reason(b))

    def test_unknown_leg_ignored_when_known_legs_already_block_basket(self):
        books={t:self.make_book() for t in ['a','b','c']}
        books['a'].apply_batch(BASE,[book(BASE,.7,'a')])
        books['b'].apply_batch(BASE,[book(BASE,.4,'b')])
        state=BoundedEvent({'event_id':'e'},[dict(package_id='p',token_ids=['a','b','c'])],books)
        self.assertEqual(state.status,'blocked')
        self.assertEqual(explain_state(state,books),(set(),None,set(),0))

    def test_missing_leg_attribution_requires_an_unresolved_basket(self):
        books={t:self.make_book() for t in ['a','b']}
        books['a'].apply_batch(BASE,[book(BASE,.4,'a')])
        state=BoundedEvent({'event_id':'e'},[dict(package_id='p',token_ids=['a','b'])],books)
        reasons,primary,tokens,count=explain_state(state,books)
        self.assertEqual(reasons,{'missing_initial_snapshot'})
        self.assertEqual(tokens,{'b'})
        self.assertEqual(count,1)

    def test_old_and_future_books_do_not_backfill_starting_depth(self):
        start=BASE+pd.Timedelta(hours=1)
        end=start+pd.Timedelta(hours=1)
        def row(token,kind,at):
            return dict(asset_id=token,event_type=kind,timestamp=at,timestamp_received=max(at,BASE))
        frame=pd.DataFrame([row('a','book',BASE+pd.Timedelta(minutes=30)),
            row('b','book',BASE-pd.Timedelta(minutes=1)),
            row('a','price_change',BASE+pd.Timedelta(minutes=1)),
            row('a','price_change',start),row('b','price_change',start),
            row('b','book',end+pd.Timedelta(minutes=5))])
        stats,tokens=seed_statistics(frame,['a','b'],BASE,start,end)
        self.assertEqual(stats['assets_seeded_before_first_scan'],1)
        self.assertEqual(stats['assets_seeded_before_analysis_end'],1)
        self.assertEqual(stats['books_before_source_capture'],1)
        self.assertEqual(stats['price_updates_before_seed'],2)
        self.assertAlmostEqual(stats['unseeded_update_fraction'],2/3)
        self.assertFalse(tokens.set_index('asset_id').loc['b','seeded_before_scan'])

    def test_no_full_books_keeps_all_updates_unseeded(self):
        frame=pd.DataFrame([dict(asset_id='a',event_type='price_change',timestamp=BASE,timestamp_received=BASE)])
        stats,_=seed_statistics(frame,['a'],BASE,BASE+pd.Timedelta(hours=1),BASE+pd.Timedelta(hours=2))
        self.assertEqual(stats['price_updates_before_seed'],1)
        self.assertEqual(stats['assets_without_any_qualifying_book'],1)


class AuditIntegrationTest(unittest.TestCase):
    def test_staged_extract_retains_only_selected_tokens_and_closes_before_cleanup(self):
        parent=DATA_DIR/'pmxt'
        parent.mkdir(parents=True,exist_ok=True)
        rows=[dict(book(BASE,.4,t),market=b'condition') for t in ['a','other']]
        table=pa.Table.from_pandas(pd.DataFrame(rows).drop(columns=['archive_hour','archive_row']),preserve_index=False)
        output=io.BytesIO()
        pq.write_table(table,output,row_group_size=1)
        payload=output.getvalue()
        class Remote:
            def __init__(self,url,budget):
                self.url,self.budget,self.size,self.etag=url,budget,len(payload),'fixed'
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def fetch(self,a,z):
                self.budget.charge(z-a)
                return payload[a:z]
        with tempfile.TemporaryDirectory(prefix='extract_test_',dir=parent) as directory:
            root=Path(directory)
            with patch('src.data.pmxt_multiday.RetryingRangeFile',Remote):
                path,manifest=extract_bounded(BASE,root,['a'],{'condition'},SharedBudget(1000000))
            actual=pd.read_parquet(path)
            self.assertEqual(actual.asset_id.tolist(),['a'])
            self.assertEqual(actual.archive_row.tolist(),[0])
            self.assertEqual(manifest['selected_rows'],1)
            self.assertEqual(manifest['sha256'],sha256(path))
            self.assertFalse(list(root.glob('*.archive.*')))
            with patch('src.data.pmxt_multiday.RetryingRangeFile',side_effect=AssertionError('must use cache')):
                self.assertEqual(extract_bounded(BASE,root,['a'],{'condition'},SharedBudget(1))[1],manifest)

    def test_frozen_multiday_selection_and_universe_cannot_change(self):
        parent=DATA_DIR/'pmxt'
        parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='design_test_',dir=parent) as directory:
            source=Path(directory)/'original'
            target=Path(directory)/'expanded'
            source.mkdir()
            for name in ['events','markets','packages']:
                pd.DataFrame([dict(event_id='e')]).to_parquet(source/f'{name}.parquet',index=False)
            write_json(source/'plan.json',dict(start='2026-05-04T01:00:00+00:00',end='2026-05-11T01:00:00+00:00',
                selected=dict(negrisk=10,ladder=5),assets=['a'],files_sha256={n:sha256(source/f'{n}.parquet') for n in ['events','markets','packages']}))
            first=freeze(target,source)
            self.assertEqual(first['analysis_hours'],24)
            self.assertEqual(len(first['blocks']),4)
            self.assertEqual(freeze(target,source),first)
            self.assertEqual(len(list(target.glob('*/plan.json'))),4)
            first['analysis_hours']=25
            write_json(target/'study_design.json',first)
            with self.assertRaises(ValueError):
                freeze(target,source)

    def test_audit_matches_unknown_intervals_and_preserves_completed_replay(self):
        parent=DATA_DIR/'pmxt'
        parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='audit_test_',dir=parent) as directory:
            root=Path(directory)
            (root/'raw').mkdir()
            result=PROJECT_ROOT/'results/pmxt_pilot'/root.name
            self.addCleanup(lambda:self.cleanup(result))
            pd.DataFrame([dict(event_id='e',event_slug='test',classification='ladder')]).to_parquet(root/'events.parquet',index=False)
            pd.DataFrame([dict(package_id='p',event_id='e',token_ids=['a','b'])]).to_parquet(root/'packages.parquet',index=False)
            pd.DataFrame([dict(market_id='m')]).to_parquet(root/'markets.parquet',index=False)
            start=BASE+pd.Timedelta(hours=1)
            end=start+pd.Timedelta(hours=1)
            write_json(root/'plan.json',dict(start=start,end=end+pd.Timedelta(hours=1),warmup_start=BASE,
                assets=['a','b'],metadata_mode='test',files_sha256={n:sha256(root/f'{n}.parquet') for n in ['events','markets','packages']}))
            first=pd.DataFrame([book(BASE+pd.Timedelta(minutes=59),.4,'a')])
            middle=pd.DataFrame([dict(book(start+pd.Timedelta(minutes=30),.4,'a'),event_type='last_trade_price')])
            for i,frame in enumerate([first,middle,first.iloc[:0].copy()]):
                path=root/'raw'/f'2026-05-04T{i:02}.parquet'
                frame['archive_row']=range(len(frame))
                frame.to_parquet(path,index=False)
                write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=['a','b'],transfer_bytes=1))
            rerun(root,analysis_end=end.isoformat())
            summary=result/'source_validation_v3/summary.json'
            before=sha256(summary)
            audited=audit(root)
            self.assertEqual(audited['unknown_labels'],1)
            self.assertAlmostEqual(audited['unknown_seconds'],3600.)
            self.assertAlmostEqual(audited['primary_reason_seconds']['missing_initial_snapshot'],3600.)
            self.assertEqual(sha256(summary),before)
            write_json(root/'study_design.json',dict(blocks=[dict(name='block',start=start.isoformat(),analysis_end=end.isoformat())],
                analysis_hours=1,acceptance_targets=dict(full_ask_time_fraction=.95,certified_occurrence_fraction=.9,typical_ask_complete_events_per_scan=1),maximum_new_range_bytes=100))
            # Exercise the aggregator with an isolated one-block layout.
            block=root/'block'
            block.mkdir()
            for name in ['events.parquet','plan.json']:
                shutil.copy2(root/name,block/name)
            shutil.copytree(root/'source_validation_v3',block/'source_validation_v3')
            block_result=PROJECT_ROOT/'results/pmxt_pilot/block'
            if block_result.exists():
                raise ValueError('Synthetic block result name already exists')
            self.addCleanup(lambda:self.cleanup(block_result))
            shutil.copytree(result/'source_validation_v3',block_result/'source_validation_v3')
            shutil.copytree(result/'unknown_audit_v1',block_result/'unknown_audit_v1')
            write_json(root/'download_ledger.json',dict(charged_range_bytes=3))
            aggregate=assess(root,run_audits=False)
            self.assertEqual(aggregate['unknown_hours'],1)
            self.assertFalse(aggregate['engineering_targets_pass'])
            self.assertEqual(aggregate['candidate_episode_starts'],0)
            self.assertFalse(aggregate['models_trained'])

    @staticmethod
    def cleanup(path):
        if not path.resolve().is_relative_to((PROJECT_ROOT/'results/pmxt_pilot').resolve()):
            raise ValueError('Unsafe cleanup')
        if path.exists():
            shutil.rmtree(path)


if __name__=='__main__':
    unittest.main()
