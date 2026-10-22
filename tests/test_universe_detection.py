"""Economic relationship, data availability and universe-adapter regressions."""
import json
from pathlib import Path
import tempfile
import unittest
import pandas as pd
from src.data.contract_templates import parse_contract, amount
from src.data.universe_detection import DetectionResult, detect_universe, detection_plans, normalise_market, read_export

START = '2025-10-14T00:00:00Z'
END = '2026-01-08T00:00:00Z'


def market(mid, value, comparison='above', event='asset'):
    when = '2025-12-31T12:00:00Z'
    return dict(market_id=mid,event_id=event,event_slug=event,yes_token=mid+'y',no_token=mid+'n',
        listed_at=START,metadata_available_at=START,neg_risk=False,
        question=f'Will asset price be {comparison} ${value} on {when}?',
        description=f'This market will resolve to "Yes" if asset price is {comparison} ${value} on {when}. '
            'Otherwise, this market will resolve to "No". Resolution source: https://exchange.example/price')


def detect(rows, events=None, **kwargs):
    return detect_universe(pd.DataFrame(rows),pd.DataFrame(events) if events is not None else None,
        window_start=START,window_end=END,**kwargs)


def negrisk():
    rows = [dict(market(str(i),10*i),neg_risk=True,neg_risk_market_id='contract') for i in (1,2,3)]
    event = dict(event_id='asset',neg_risk=True,neg_risk_augmented=False,neg_risk_market_id='contract',
        member_market_ids=['1','2','3'],complete_set_verified=True,metadata_available_at=START)
    return rows,event


class UniverseDetectionTests(unittest.TestCase):
    def test_upper_threshold_direction_and_all_pairs(self):
        result = detect([market('a',10),market('b',20),market('c',30)])
        self.assertEqual(set(result.pairs.pair_id),{'a_b','a_c','b_c'})
        self.assertEqual(result.packages.iloc[0].token_ids,['ay','bn'])
        self.assertEqual(result.events.iloc[0].classification,'ladder')

    def test_lower_threshold_direction(self):
        result = detect([market('a',10,'below'),market('b',20,'below')])
        self.assertEqual(result.pairs.iloc[0].pair_id,'b_a')

    def test_no_pairs_between_incompatible_sources_dates_or_operators(self):
        a = market('a',10)
        for b in [market('b',20,'below'),market('b',20,'at least'),
                  dict(market('b',20),description=market('b',20)['description'].replace('exchange.example','other.example')),
                  {k:v.replace('2025-12-31','2025-12-30') if isinstance(v,str) else v for k,v in market('b',20).items()}]:
            with self.subTest(b=b['question']):
                self.assertTrue(detect([a,b]).pairs.empty)

    def test_extra_clauses_and_title_description_mismatch_are_not_accepted(self):
        for description in [market('b',20)['description']+' If the asset fails, both markets resolve Yes.',
                            market('b',20)['description'].replace('$20','$30')]:
            r=detect([market('a',10),dict(market('b',20),description=description)])
            self.assertTrue(r.pairs.empty)
            self.assertEqual(r.markets.iloc[1].status,'review')

    def test_equal_thresholds_and_different_events_are_not_pairs(self):
        self.assertTrue(detect([market('a',10),market('b',10)]).pairs.empty)
        self.assertTrue(detect([market('a',10),market('b',20,event='other')]).pairs.empty)

    def test_deadlines_use_later_date_as_broader(self):
        rows=[]
        for mid,date in [('a','2025-11-01'),('b','2025-12-01')]:
            when=date+'T00:00:00Z'
            r=market(mid,10)
            r.update(question=f'Will a launch occur by {when}?',
                description=f'This market will resolve to "Yes" if a launch occurs by {when}. Otherwise, this market will resolve to "No". Resolution source: https://example.com/launch')
            rows.append(r)
        result=detect(rows)
        self.assertEqual(result.pairs.iloc[0].pair_id,'b_a')
        self.assertEqual(result.pairs.iloc[0].window_end,pd.Timestamp('2025-11-01T00:00:00Z'))

    def test_metadata_availability_is_a_causal_boundary(self):
        rows=[market('a',10),dict(market('b',20),metadata_available_at='2025-12-01T00:00:00Z')]
        self.assertEqual(detect(rows).pairs.iloc[0].window_start,pd.Timestamp('2025-12-01T00:00:00Z'))
        rows[1]['metadata_available_at']=None
        self.assertTrue(detect(rows).pairs.empty)
        self.assertFalse(detect(rows,metadata_mode='frozen').pairs.empty)

    def test_future_listing_or_disjoint_metadata_intervals_exclude_pairs(self):
        rows=[market('a',10),dict(market('b',20),listed_at='2026-02-01T00:00:00Z')]
        self.assertTrue(detect(rows).pairs.empty)
        rows=[dict(market('a',10),valid_until='2025-11-01T00:00:00Z'),
              dict(market('b',20),metadata_available_at='2025-11-01T00:00:00Z')]
        self.assertTrue(detect(rows).pairs.empty)

    def test_closed_status_winners_volume_do_not_change_detection(self):
        rows=[market('a',10),market('b',20)]
        baseline=detect(rows)
        for r in rows:
            r.update(closed=True,closed_time='2025-10-15',volume=999999999,resolved_yes=False,outcomePrices='["0", "1"]')
        pd.testing.assert_frame_equal(baseline.pairs,detect(rows).pairs)

    def test_complete_negrisk_basket(self):
        rows,event=negrisk()
        result=detect(rows,[event])
        self.assertEqual(result.events.iloc[0].classification,'negrisk')
        self.assertEqual(result.events.iloc[0].status,'accepted')
        self.assertEqual(result.packages.iloc[0].token_ids,['1y','2y','3y'])
        self.assertTrue(result.pairs.empty)

    def test_negrisk_incomplete_unknown_augmented_or_unverified_never_trades(self):
        rows,event=negrisk()
        variants=[dict(event,member_market_ids=['1','2','3','4']),dict(event,complete_set_verified=False),
            dict(event,neg_risk_augmented=True),dict(event,neg_risk_augmented=None),dict(event,neg_risk_market_id='wrong'),
            dict(event,metadata_available_at=None),dict(event,member_market_ids=['1','1','2','3'])]
        for changed in variants:
            with self.subTest(event=changed):
                result=detect(rows,[changed])
                self.assertEqual(result.events.iloc[0].classification,'negrisk')
                self.assertTrue(result.packages.empty)
        self.assertTrue(detect(rows[:2],[event]).packages.empty)
        self.assertTrue(detect(rows).packages.empty)

    def test_negrisk_missing_member_token_or_future_member_blocks_whole_basket(self):
        rows,event=negrisk()
        rows[0]['yes_token']=''
        self.assertTrue(detect(rows,[event]).packages.empty)
        rows,event=negrisk()
        rows[0]['listed_at']='2026-02-01T00:00:00Z'
        self.assertTrue(detect(rows,[event]).packages.empty)

    def test_duplicate_tokens_never_create_artificial_baskets(self):
        rows=[market('a',10),dict(market('b',20),no_token='ay')]
        self.assertTrue(detect(rows).packages.empty)

    def test_duplicate_versions_and_ambiguous_event_names_fail(self):
        with self.assertRaisesRegex(ValueError,'duplicate market_id'):
            detect([market('a',10),market('a',20)])
        with self.assertRaisesRegex(ValueError,'ranking slug'):
            detect([market('a',10),dict(market('b',20),event_id='other')])

    def test_gamma_mapping_uses_outcome_labels_and_never_assumes_order(self):
        row=normalise_market(dict(id='m',events=[dict(id='e',slug='event')],outcomes='["No","Yes"]',clobTokenIds='["n","y"]',negRisk='false'))
        self.assertEqual((row['yes_token'],row['no_token']),('y','n'))
        self.assertEqual(row['event_id'],'e')
        ambiguous=normalise_market(dict(id='m',events=[dict(id='a'),dict(id='b')]))
        self.assertEqual(ambiguous['event_id'],'')

    def test_empty_input_and_export_roundtrip(self):
        result=detect([])
        self.assertTrue(result.packages.empty)
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'result'
            result.write(output)
            self.assertTrue(read_export(output/'pairs.parquet').empty)
            self.assertTrue(DetectionResult.read(output).packages.empty)
            with self.assertRaises(FileExistsError):
                result.write(output)

    def test_parquet_inventory_arrays_and_artifact_integrity(self):
        rows,event=negrisk()
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            pd.DataFrame([event]).to_parquet(root/'events.parquet')
            result=detect(rows,read_export(root/'events.parquet').to_dict('records'))
            self.assertEqual(len(result.packages),1)
            result.write(root/'out')
            reloaded=DetectionResult.read(root/'out')
            self.assertEqual(len(detection_plans(reloaded,lambda t:root/t,require_files=False)[0]),1)
            (root/'out/pairs.parquet').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'artifact changed'):
                DetectionResult.read(root/'out')

    def test_string_false_flags_do_not_create_negrisk_events(self):
        rows=[dict(market('a',10),neg_risk='false'),dict(market('b',20),neg_risk='false')]
        self.assertEqual(detect(rows).events.iloc[0].classification,'ladder')

    def test_plan_adapter_preserves_complete_baskets_and_excludes_missing_books(self):
        result=detect([market('a',10),market('b',20),market('c',30)])
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            for token in ['ay','bn']:
                (root/token).touch()
            plans,excluded=detection_plans(result,lambda token:root/token)
            self.assertEqual(len(plans),1)
            self.assertEqual(plans[0]['packages'][0][0],['ay','bn'])
            self.assertEqual(len(excluded),2)

    def test_malformed_currency_separators_rejected(self):
        with self.assertRaises(ValueError):
            amount('$1,2M')


class FrozenAuditedRegressionTests(unittest.TestCase):
    def test_saved_audited_threshold_families(self):
        # Optional integration check: private data is not needed for unit tests.
        from src.paths import MARKETS_PARQUET, EVENT_UNIVERSE, TOKENS_PARQUET
        if not all(p.exists() for p in (MARKETS_PARQUET,EVENT_UNIVERSE,TOKENS_PARQUET)):
            self.skipTest('Local frozen metadata is unavailable')
        from src.data.rules import build_manifest
        old_pairs,_,old_rules=build_manifest()
        m=pd.read_parquet(MARKETS_PARQUET)
        u=pd.read_parquet(EVENT_UNIVERSE)
        ids=set(old_rules.market_id)
        t=pd.read_parquet(TOKENS_PARQUET,columns=['market_id','outcome','token_id'])
        t=t.loc[t.market_id.isin(ids) & t.outcome.str.upper().isin(['YES','NO'])].copy()
        t['outcome']=t.outcome.str.lower()
        t=t.pivot(index='market_id',columns='outcome',values='token_id').rename(columns={'yes':'yes_token','no':'no_token'})
        rows=m.loc[m.market_id.isin(ids)].merge(u[['market_id','event_slug']],on='market_id').merge(t,on='market_id')
        rows['event_id']=rows.event_slug
        # Missing year must be reviewed, rather than guessed from end_date.
        original=detect_universe(rows,window_start=START,window_end=END,metadata_mode='frozen')
        deadline_ids=set(old_rules.loc[old_rules.kind.eq('deadline'),'market_id'])
        rejected=original.markets.loc[original.markets.market_id.isin(deadline_ids)]
        self.assertTrue(rejected.reason.eq('missing_or_ambiguous_contract_year').all())
        expected=old_pairs.loc[~old_pairs.kind.eq('deadline')]
        self.assertEqual(set(original.pairs.pair_id),set(expected.pair_id))
        # Regression-only year evidence from the previously audited deadlines.
        years={r.market_id: (r.end-pd.Timedelta(seconds=1)).tz_convert('America/New_York').year
               for r in old_rules.loc[old_rules.kind.eq('deadline')].itertuples()}
        rows['contract_year']=rows.market_id.map(years)
        with_year=detect_universe(rows,window_start=START,window_end=END,metadata_mode='frozen')
        self.assertEqual(set(with_year.pairs.pair_id),set(old_pairs.pair_id))


if __name__ == '__main__':
    unittest.main()
