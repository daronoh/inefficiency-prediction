"""Discovery must not turn retrospective metadata into execution evidence."""
import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from src.data.catalogue import (
    analyse_catalogue, bounds, classify_event, classify_events, fetch_catalogue, metadata_only,
)
from tests.test_universe_detection import market


def gamma_event(negrisk=False):
    markets = []
    for mid, value in [('a', 10), ('b', 20)]:
        row = market(mid, value)
        markets.append(dict(id=mid, question=row['question'], description=row['description'],
                            startDate=row['listed_at'], negRisk=negrisk,
                            negRiskMarketID='contract' if negrisk else None,
                            outcomes=['Yes', 'No'], clobTokenIds=[mid + 'y', mid + 'n']))
    return dict(id='asset', slug='asset', markets=markets, negRisk=negrisk,
                negRiskAugmented=False, negRiskMarketID='contract' if negrisk else None)


class CatalogueTests(unittest.TestCase):
    def test_batch_preserves_individual_event_classifications(self):
        first = gamma_event()
        second = gamma_event(True)
        second.update(id='second', slug='second')
        for row in second['markets']:
            row['id'] += '_second'
            row['clobTokenIds'] = [t + '_second' for t in row['clobTokenIds']]
        start, end = '2025-10-14T00:00:00Z', '2026-01-08T00:00:00Z'
        batch = classify_events([first, second], start, end)
        for event in (first, second):
            individual, _ = classify_event(event, start, end)
            combined, _ = batch[event['id']]
            self.assertEqual(individual.events.to_dict('records'), combined.events.to_dict('records'))
            self.assertEqual(individual.pairs.to_dict('records'), combined.pairs.to_dict('records'))

    def test_short_cursor_pages_continue_and_resume_without_network(self):
        replies = []
        for eid, cursor in [('a', 'opaque-next'), ('b', None)]:
            body = dict(events=[dict(id=eid, markets=[])], next_cursor=cursor)
            replies.append(Mock(status_code=200, content=json.dumps(body).encode(),
                                json=Mock(return_value=body)))
        with TemporaryDirectory() as temporary, patch('src.data.catalogue.requests.Session') as session:
            session.return_value.get.side_effect = replies
            kwargs = dict(start='2026-03-01T00:00:00Z', end='2026-09-01T00:00:00Z',
                          final_start='2026-08-01T00:00:00Z')
            result = fetch_catalogue(temporary, **kwargs)
            self.assertEqual(result['events'], 2)
            self.assertEqual(session.return_value.get.call_args_list[1].kwargs['params']['after_cursor'],
                             'opaque-next')
            session.return_value.get.reset_mock()
            self.assertEqual(fetch_catalogue(temporary, **kwargs)['events'], 2)
            session.return_value.get.assert_not_called()
            with self.assertRaises(ValueError):
                fetch_catalogue(temporary, **dict(kwargs, end='2026-10-01T00:00:00Z'))

    def test_page_limit_does_not_mark_partial_catalogue_complete(self):
        body = dict(events=[dict(id='a', markets=[])], next_cursor='more')
        reply = Mock(status_code=200, content=json.dumps(body).encode(), json=Mock(return_value=body))
        with TemporaryDirectory() as temporary, patch('src.data.catalogue.requests.Session') as session:
            session.return_value.get.return_value = reply
            with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                fetch_catalogue(temporary, start='2026-03-01T00:00:00Z',
                                end='2026-09-01T00:00:00Z', final_start='2026-08-01T00:00:00Z',
                                max_pages=1)
            self.assertFalse((Path(temporary) / 'complete.json').exists())
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                analyse_catalogue(temporary)
            summary = analyse_catalogue(temporary, allow_partial=True)
            self.assertFalse(summary['catalogue_complete'])
            self.assertTrue((Path(temporary) / 'discovery_partial/summary.json').exists())
            self.assertFalse((Path(temporary) / 'complete.json').exists())

    def test_no_prices_volumes_or_realised_outcomes_in_saved_metadata(self):
        event = gamma_event()
        event.update(volume=1e8, closed=True, outcome='Yes')
        event['markets'][0].update(outcomePrices=['1', '0'], lastTradePrice=1,
                                   volume=1e8, closedTime='2025-12-31')
        saved = metadata_only(event)
        for key in ('volume', 'closed', 'outcome'):
            self.assertNotIn(key, saved)
        for key in ('outcomePrices', 'lastTradePrice', 'volume', 'closedTime'):
            self.assertNotIn(key, saved['markets'][0])
        self.assertEqual(saved['markets'][0]['outcomes'], ['Yes', 'No'])

    def test_six_month_split_is_explicit_and_half_open(self):
        start, end, final = bounds('2026-03-01T00:00:00Z', '2026-09-01T00:00:00Z',
                                   '2026-08-01T00:00:00Z')
        self.assertEqual((end.year - start.year) * 12 + end.month - start.month, 6)
        self.assertEqual(final.month, 8)
        with self.assertRaises(ValueError):
            bounds('2026-03-01', '2026-09-01', '2026-08-01')
        with self.assertRaises(ValueError):
            bounds(str(start), str(end), str(end))

    def test_negrisk_flag_never_certifies_complete_basket(self):
        result, _ = classify_event(gamma_event(True), '2025-10-14T00:00:00Z',
                                   '2026-01-08T00:00:00Z')
        self.assertEqual(result.events.iloc[0].classification, 'negrisk')
        self.assertEqual(result.events.iloc[0].reason, 'exhaustive_outcome_set_not_verified')
        self.assertTrue(result.packages.empty)

    def test_ladder_uses_existing_rule_parser_and_respects_period(self):
        event = gamma_event()
        result, _ = classify_event(event, '2025-10-14T00:00:00Z', '2026-01-08T00:00:00Z')
        self.assertEqual(len(result.pairs), 1)
        expired, _ = classify_event(event, '2026-03-01T00:00:00Z', '2026-09-01T00:00:00Z')
        self.assertTrue(expired.pairs.empty)


if __name__ == '__main__':
    unittest.main()
