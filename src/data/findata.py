"""Authenticated read-only FinData retrieval; credentials never enter manifests."""
from __future__ import annotations

import json
import os
import time
from urllib.parse import urljoin, urlparse

import requests

BASE = 'https://lum.id/findata'


class FinData:
    def __init__(self, token=None):
        token = token or os.environ.get('LUMID_TOKEN')
        if not token:
            raise ValueError('Set LUMID_TOKEN locally before authenticated retrieval')
        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {token}'

    def _request(self, method, url, **kwargs):
        # Do not forward bearer credentials to object stores or redirect targets.
        parsed = urlparse(url)
        if parsed.scheme != 'https' or parsed.netloc != 'lum.id':
            raise ValueError('Authenticated requests must stay on https://lum.id')
        for attempt in range(4):
            response = self.session.request(method, url, timeout=45, allow_redirects=False, **kwargs)
            if response.status_code != 429:
                break
            try:
                delay = float(response.headers.get('Retry-After', 2 ** attempt))
            except ValueError:
                delay = 2 ** attempt
            if delay > 60:
                raise RuntimeError('FinData requested a long backoff; resume later')
            time.sleep(max(0, delay))
        if response.status_code != 200:
            # URLs may contain signatures. Do not include them or response bodies.
            if response.status_code == 400:
                message = response.text.lower()
                if 'timeout' in message:
                    raise RuntimeError('FinData query exceeded its statement timeout')
                if 'row cap' in message or 'row limit' in message:
                    raise RuntimeError('FinData query exceeded its row cap')
            raise RuntimeError(f'FinData HTTP {response.status_code}')
        return response

    def query(self, sql):
        """Return JSONL rows and a safe receipt for a bounded SELECT query."""
        if not sql.lstrip().upper().startswith('SELECT '):
            raise ValueError('Only explicit SELECT queries are accepted')
        response = self._request('POST', BASE + '/retrieve',
                                 json={'sql': sql, 'output_format': 'jsonl'})
        result = response.json()
        reference = result.get('signed_url') or result.get('materialized_uri')
        if not isinstance(reference, str) or not reference:
            raise ValueError('FinData returned no readable materialization')
        if reference.startswith('/blobs/'):
            url = BASE + reference
        else:
            url = urljoin(BASE + '/', reference)
        parsed = urlparse(url)
        if parsed.scheme != 'https':
            raise ValueError('Materialized result requires an HTTPS URL')
        if parsed.netloc == 'lum.id':
            data = self._request('GET', url)
        elif result.get('signed_url'):
            # Signed object-storage URL: fresh request, without bearer credentials.
            data = requests.get(url, timeout=45, allow_redirects=False)
            if data.status_code != 200:
                raise RuntimeError(f'FinData materialization HTTP {data.status_code}')
        else:
            raise ValueError('Unsigned external materialization is unsupported')
        rows = [json.loads(line) for line in data.text.splitlines() if line.strip()]
        receipt = {key: result.get(key) for key in
                   ('run_id', 'rowcount', 'size_bytes', 'output_format', 'replay_latency_ms')}
        if receipt['rowcount'] is not None and int(receipt['rowcount']) != len(rows):
            raise ValueError('FinData materialization row count mismatch')
        return rows, receipt
