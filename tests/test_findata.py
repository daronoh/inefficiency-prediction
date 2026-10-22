"""Credential isolation and completeness checks for materialized query results."""
import json
import unittest
from unittest.mock import Mock, patch

from src.data.findata import FinData


class FinDataTests(unittest.TestCase):
    def test_relative_blob_and_row_count(self):
        client = FinData('test-only')
        receipt = dict(materialized_uri='/blobs/retrievals/test.jsonl', signed_url='',
                       rowcount=1, run_id='test')
        with patch.object(client, '_request') as request:
            request.side_effect = [
                Mock(json=Mock(return_value=receipt)),
                Mock(text=json.dumps({'asset_id': '123'}) + '\n'),
            ]
            rows, audit = client.query('SELECT asset_id FROM example LIMIT 1')
        self.assertEqual(rows, [{'asset_id': '123'}])
        self.assertNotIn('materialized_uri', audit)
        self.assertEqual(request.call_args.args[1],
                         'https://lum.id/findata/blobs/retrievals/test.jsonl')

    def test_external_signed_download_has_no_authorization(self):
        client = FinData('test-only')
        receipt = dict(signed_url='https://storage.example/result?signature=private', rowcount=0)
        with patch.object(client, '_request', return_value=Mock(json=Mock(return_value=receipt))), \
                patch('src.data.findata.requests.get', return_value=Mock(status_code=200, text='')) as get:
            rows, audit = client.query('SELECT asset_id FROM example LIMIT 0')
        self.assertFalse(rows)
        self.assertNotIn('headers', get.call_args.kwargs)
        self.assertNotIn('signed_url', audit)

    def test_rejects_nonselect_bad_hosts_and_truncated_materialization(self):
        client = FinData('test-only')
        with self.assertRaises(ValueError):
            client.query('DELETE FROM example')
        with self.assertRaises(ValueError):
            client._request('GET', 'https://other.example/private')
        with patch.object(client, '_request') as request:
            request.side_effect = [
                Mock(json=Mock(return_value=dict(materialized_uri='/blobs/test', rowcount=2))),
                Mock(text='{}\n'),
            ]
            with self.assertRaisesRegex(ValueError, 'row count'):
                client.query('SELECT asset_id FROM example LIMIT 2')


if __name__ == '__main__':
    unittest.main()
