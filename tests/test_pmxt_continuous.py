"""Continuous seeding safeguards and causal paired replay tests."""
from __future__ import annotations

import io
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common import sha256
from src.paths import DATA_DIR, PROJECT_ROOT
from src.data.pmxt_archive import write_json
from src.data.pmxt_continuous import select_packages, anchors_sufficient, seed_anchors, extract_selected
from src.data.pmxt_multiday import SharedBudget
from src.data.pmxt_revalidation import rerun
from src.data.pmxt_uncertainty import BoundedBook
from src.reporting.pmxt_continuous import ObservedBook, initial_and_analysis_evidence, independent_ask_check
from test_pmxt_source import BASE, book, delta


class SelectionTest(unittest.TestCase):
    def test_subset_uses_seed_status_size_and_hash_without_outcome_columns(self):
        records=[dict(package_id='n3',stratum='negrisk',token_ids=['a','b','c']),
            dict(package_id='n4',stratum='negrisk',token_ids=['a','b','c','d']),
            dict(package_id='l1',stratum='ladder',token_ids=['a','e']),
            dict(package_id='seeded',stratum='ladder',token_ids=['a','s'])]
        known={t:t=='s' for t in ['a','b','c','d','e','s']}
        selected=select_packages(pd.DataFrame(records),known)
        self.assertEqual([p['package_id'] for p in selected],['n3','l1'])
        table=pd.DataFrame(records[::-1])
        table['future_profit']=[999.,-999.,999.,-999.]
        self.assertEqual([p['package_id'] for p in select_packages(table,known)],['n3','l1'])

    def test_anchor_before_receipt_capture_requires_more_history(self):
        anchors={'a':BASE-pd.Timedelta(milliseconds=1),'b':BASE}
        self.assertFalse(anchors_sufficient(anchors,['a','b'],BASE))
        self.assertTrue(anchors_sufficient(anchors,['a','b'],BASE-pd.Timedelta(hours=1)))
        self.assertFalse(anchors_sufficient({'a':BASE},['a','b'],BASE))

    def test_observer_preserves_reconstruction_and_counts_only_analysis_issues(self):
        start=BASE+pd.Timedelta(hours=1)
        end=start+pd.Timedelta(hours=1)
        reference=BoundedBook(0)
        observed=ObservedBook(0,start,end)
        batches=[(BASE,[book(BASE,.03)]),
            (BASE+pd.Timedelta(minutes=30),[delta(BASE+pd.Timedelta(minutes=30),.04,10,ask=.031)]),
            (start,[delta(start,.04,10,ask=.031)])]
        for stamp,rows in batches:
            reference.apply_batch(stamp,rows)
            observed.apply_batch(stamp,rows)
        self.assertEqual(reference.depth,observed.depth)
        self.assertEqual(reference.alternatives,observed.alternatives)
        self.assertEqual(reference.faults,observed.faults)
        self.assertEqual(reference.counters,observed.counters)
        self.assertEqual(observed.analysis_issues['best_price_contradiction'],1)
        self.assertTrue(observed.analysis_examples[0]['already_faulty'])

    def test_first_scan_excludes_snapshot_at_the_scan_timestamp(self):
        start=BASE+pd.Timedelta(hours=1)
        frame=pd.DataFrame([book(start,.6,'a')])
        tokens,_=initial_and_analysis_evidence(frame,['a'],BASE,start,start+pd.Timedelta(hours=1))
        self.assertFalse(tokens.iloc[0].initialized)


class HistoryTest(unittest.TestCase):
    def workspace(self,prefix):
        parent=DATA_DIR/'pmxt'
        parent.mkdir(parents=True,exist_ok=True)
        return tempfile.TemporaryDirectory(prefix=prefix,dir=parent)

    def save(self,path,rows,assets):
        frame=pd.DataFrame(rows,columns=['asset_id','event_type','timestamp'])
        frame.to_parquet(path,index=False)
        write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=assets,transfer_bytes=0))

    def test_old_anchor_never_bridges_an_unobserved_receipt_hour(self):
        with self.workspace('seed_gap_') as directory:
            root=Path(directory)
            self.save(root/'2026-05-04T00.parquet',[dict(asset_id='a',event_type='book',timestamp=BASE)],['a'])
            with self.assertRaisesRegex(ValueError,'gap'):
                seed_anchors(root,['a'],BASE,BASE+pd.Timedelta(hours=2),BASE+pd.Timedelta(hours=3))
            for hour in [1,2]:
                self.save(root/f'2026-05-04T{hour:02}.parquet',[],['a'])
            anchors=seed_anchors(root,['a'],BASE,BASE+pd.Timedelta(hours=2),BASE+pd.Timedelta(hours=3))
            self.assertEqual(anchors,{'a':BASE})

    def test_after_scan_snapshot_cannot_supply_starting_state(self):
        with self.workspace('seed_future_') as directory:
            root=Path(directory)
            self.save(root/'2026-05-04T00.parquet',[dict(asset_id='a',event_type='book',timestamp=BASE+pd.Timedelta(hours=2))],['a'])
            self.assertEqual(seed_anchors(root,['a'],BASE,BASE+pd.Timedelta(hours=1),BASE+pd.Timedelta(hours=1)),{})

    def test_selected_range_extraction_preserves_original_row_numbers(self):
        records=[dict(book(BASE,.4,'other'),market=b'other'),dict(book(BASE,.4,'a'),market=b'condition')]
        table=pa.Table.from_pandas(pd.DataFrame(records).drop(columns=['archive_row','archive_hour']),preserve_index=False)
        buffer=io.BytesIO()
        pq.write_table(table,buffer,row_group_size=1)
        payload=buffer.getvalue()
        class Remote(io.BytesIO):
            def __init__(self,url,budget):
                super().__init__(payload)
                self.url,self.etag,self.size=url,'fixed',len(payload)
            def cache_row_group(self,group):pass
        with self.workspace('seed_extract_') as directory:
            with patch('src.data.pmxt_continuous.RetryingRangeFile',Remote):
                path,metadata=extract_selected(BASE,Path(directory),['a'],{'condition'},SharedBudget(1_000_000))
            frame=pd.read_parquet(path)
            self.assertEqual(frame.asset_id.tolist(),['a'])
            self.assertEqual(frame.archive_row.tolist(),[1])
            self.assertEqual(metadata['sha256'],sha256(path))
            with patch('src.data.pmxt_continuous.RetryingRangeFile',side_effect=AssertionError('cached')):
                self.assertEqual(extract_selected(BASE,Path(directory),['a'],{'condition'},SharedBudget(1))[1],metadata)

    def test_independent_check_exposes_unremoved_depth_and_preserves_trade_evidence(self):
        with self.workspace('seed_independent_') as directory:
            root=Path(directory)
            (root/'raw').mkdir()
            decision=BASE+pd.Timedelta(minutes=1)
            trade_at=decision+pd.Timedelta(milliseconds=19)
            rows=[book(BASE,.39,'a'),dict(delta(decision,.39,490,ask=.42),side='BUY',best_bid=.39),
                dict(delta(trade_at,.39,10,ask=.42),event_type='last_trade_price',side='BUY')]
            path=root/'raw/2026-05-04T00.parquet'
            pd.DataFrame(rows).to_parquet(path,index=False)
            write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=['a'],transfer_bytes=0))
            before=sha256(path)
            evidence=independent_ask_check(root,'a',BASE.value//1_000_000,decision.value//1_000_000)
            self.assertEqual(evidence['independently_reconstructed_best_ask'],'0.39')
            self.assertEqual(evidence['updates_at_original_best'],0)
            self.assertEqual(evidence['nearby_buy_trades'][0]['delay_ms'],19.)
            self.assertEqual(evidence['reported_best_asks'],['0.42'])
            self.assertEqual(sha256(path),before)

    def test_continuous_state_carries_through_hours_and_uses_intervening_updates(self):
        with self.workspace('seed_replay_') as directory:
            root=Path(directory)
            start=BASE+pd.Timedelta(hours=3)
            end=start+pd.Timedelta(hours=2)
            frames={0:pd.DataFrame([book(BASE,.6,t) for t in ['a','b']]),
                2:pd.DataFrame([delta(BASE+pd.Timedelta(hours=2),.6,0,ask=.2),
                    delta(BASE+pd.Timedelta(hours=2),.2,10,ask=.2)])}
            outputs={}
            for label,capture in [('isolated',BASE+pd.Timedelta(hours=2)),('continuous',BASE)]:
                folder=root/label
                (folder/'raw').mkdir(parents=True)
                result=PROJECT_ROOT/'results/pmxt_pilot'/label
                if result.exists():raise ValueError('Synthetic result name already exists')
                self.addCleanup(lambda path=result:self.cleanup(path))
                pd.DataFrame([dict(event_id='e',event_slug='test',classification='negrisk')]).to_parquet(folder/'events.parquet',index=False)
                pd.DataFrame([dict(event_id='e',package_id='p',token_ids=['a','b'])]).to_parquet(folder/'packages.parquet',index=False)
                pd.DataFrame([dict(market_id='m')]).to_parquet(folder/'markets.parquet',index=False)
                write_json(folder/'plan.json',dict(start=start,end=end+pd.Timedelta(hours=2),warmup_start=capture,
                    assets=['a','b'],metadata_mode='test',files_sha256={n:sha256(folder/f'{n}.parquet') for n in ['events','markets','packages']}))
                for hour in range(int((capture-BASE).total_seconds()/3600),7):
                    frame=frames.get(hour,frames[0].iloc[:0].copy()).copy()
                    path=folder/'raw'/f'2026-05-04T{hour:02}.parquet'
                    frame.to_parquet(path,index=False)
                    write_json(path.with_suffix('.json'),dict(sha256=sha256(path),assets=['a','b'],transfer_bytes=0))
                outputs[label]=rerun(folder,analysis_end=end.isoformat())
                if label=='continuous':
                    features=pd.read_parquet(folder/'source_validation_v3/hourly_features.parquet')
                    self.assertEqual(features.snapshot_value_lower_usd.tolist(),[2.,2.])
            self.assertEqual(outputs['isolated']['occurrence_unknown_hours'],2)
            self.assertEqual(outputs['continuous']['occurrence_positive_hours'],2)
            self.assertEqual(outputs['continuous']['full_book_event_hours'],2)

    @staticmethod
    def cleanup(path):
        if not path.resolve().is_relative_to((PROJECT_ROOT/'results/pmxt_pilot').resolve()):
            raise ValueError('Unsafe cleanup')
        if path.exists():shutil.rmtree(path)


if __name__=='__main__':
    unittest.main()
