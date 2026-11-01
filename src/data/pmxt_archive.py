"""Bounded, credential-free PMXT v2 Parquet range reads and isolated outputs."""
from __future__ import annotations
from dataclasses import dataclass
import io
import json
from pathlib import Path
import re
import time

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from src.common import sha256
from src.paths import PROJECT_ROOT, DATA_DIR


BASE_URL = 'https://r2v2.pmxt.dev/'


def isolated(path, kind='data'):
    """Resolve before every write; existing Dome directories are never targets."""
    if kind not in ('data','results'):
        raise ValueError('Unknown pilot output kind')
    allowed = ((DATA_DIR/'pmxt') if kind == 'data' else PROJECT_ROOT/'results/pmxt_pilot').resolve()
    path = Path(path).resolve()
    if not path.is_relative_to(allowed) or path == allowed:
        raise ValueError(f'Pilot output must be a run below {allowed}')
    return path


def write_json(path, value):
    Path(path).write_text(json.dumps(value,indent=2,default=str)+'\n',encoding='utf-8')


def dome_fingerprint():
    """Hash original inputs, derived data, saved results and existing research code."""
    bases = [DATA_DIR/'raw/orderbooks',DATA_DIR/'raw/ladder_orderbooks',DATA_DIR/'metadata',
             DATA_DIR/'processed',PROJECT_ROOT/'results',PROJECT_ROOT/'src']
    files = set()
    for base in bases:
        for p in base.rglob('*'):
            if (not p.is_file() or '__pycache__' in p.parts or p.suffix == '.pyc'
                or p.is_relative_to(PROJECT_ROOT/'results/pmxt_pilot')
                or p.name.startswith('pmxt_')):
                continue
            files.add(p.resolve())
    for name in ('polymarket_markets_1y.jsonl','polymarket_orderbooks.jsonl'):
        p = DATA_DIR/'raw'/name
        if p.exists():
            files.add(p.resolve())
    return {str(p):sha256(p) for p in sorted(files)}


def verify_dome(before):
    after = dome_fingerprint()
    if before != after:
        changed = sorted(p for p in set(before)|set(after) if before.get(p) != after.get(p))
        raise ValueError(f'Protected Dome inputs/results/code changed: {changed[:5]}')
    return dict(unchanged=True,files=len(before))


@dataclass
class TransferBudget:
    maximum: int = 50_000_000_000
    used: int = 0

    def charge(self, size):
        if size < 0 or self.used+size > self.maximum:
            raise RuntimeError('PMXT transfer budget exceeded; increase explicitly to resume')
        self.used += size


class RangeFile(io.RawIOBase):
    """Strict HTTP byte ranges: never silently download a whole archive object."""
    def __init__(self, url, budget=None, session=None):
        super().__init__()
        if not re.fullmatch(r'https://r2v2\.pmxt\.dev/polymarket_orderbook_\d{4}-\d{2}-\d{2}T\d{2}\.parquet',url):
            raise ValueError('Expected an official PMXT v2 hourly object URL')
        self.url,self.budget,self.session = url,budget or TransferBudget(),session or requests.Session()
        self.position,self.size,self.etag,self.requests = 0,None,None,0
        self._region = None
        self._tail = self.fetch(-8,None)
        if self._tail[-4:] != b'PAR1':
            raise ValueError('Archive is not an ordinary Parquet file')

    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.position

    def close(self):
        if not self.closed:
            self.session.close()
        super().close()

    def seek(self, offset, whence=0):
        if whence not in (0,1,2):
            raise ValueError('Invalid seek origin')
        position = offset if whence == 0 else self.position+offset if whence == 1 else self.size+offset
        if position < 0 or position > self.size:
            raise ValueError('Seek outside archive object')
        self.position = position
        return position

    def fetch(self, start, stop):
        suffix = start < 0
        requested = -start if suffix else stop-start
        if requested > 64*1024*1024 or requested < 0:
            raise ValueError('Unexpectedly large individual archive range')
        self.budget.charge(requested)
        value = f'bytes={start}' if suffix else f'bytes={start}-{stop-1}'
        headers = {'Range':value,'Accept-Encoding':'identity'}
        if self.etag:
            headers['If-Match'] = self.etag
        for attempt in range(3):
            r = self.session.get(self.url,headers=headers,stream=True,timeout=(10,40),allow_redirects=False)
            if r.status_code in (429,500,502,503,504) and attempt < 2:
                r.close();time.sleep(2**attempt);continue
            break
        with r:
            if r.status_code != 206:
                raise RuntimeError(f'Archive range HTTP {r.status_code}; no file treated as empty')
            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',r.headers.get('Content-Range',''))
            if not match:
                raise ValueError('Missing or malformed Content-Range')
            a,b,total = map(int,match.groups())
            if (self.size is not None and self.size != total) or b-a+1 != requested or a != (total+start if suffix else start):
                raise ValueError('Range/object identity changed')
            self.size = total
            tag = r.headers.get('ETag')
            if self.etag is not None and tag != self.etag:
                raise ValueError('Archive ETag changed mid-read')
            self.etag = tag
            data = r.raw.read(requested+1)
            if len(data) != requested:
                raise ValueError('Truncated or oversized range response')
            self.requests += 1
            return data

    def read(self, size=-1):
        if size < 0:
            size = self.size-self.position
        size = min(size,self.size-self.position)
        if not size:
            return b''
        if self._region is not None and self._region[0]<=self.position and self.position+size<=self._region[1]:
            start,_,buffer = self._region
            data = buffer[self.position-start:self.position-start+size]
        else:
            data = self.fetch(self.position,self.position+size)
        self.position += size
        return data

    def cache_row_group(self, group):
        """One bounded contiguous read replaces many per-column round trips."""
        columns = [group.column(i) for i in range(group.num_columns)]
        starts = [min(c.dictionary_page_offset or c.data_page_offset,c.data_page_offset) for c in columns]
        start,stop = min(starts),max(s+c.total_compressed_size for s,c in zip(starts,columns))
        self._region = start,stop,self.fetch(start,stop)


def archive_url(hour):
    stamp = pd.Timestamp(hour)
    if stamp.tzinfo is None or stamp != stamp.floor('h'):
        raise ValueError('Timezone-aware exact UTC archive hour required')
    return BASE_URL+stamp.tz_convert('UTC').strftime('polymarket_orderbook_%Y-%m-%dT%H.parquet')


def inventory_hour(hour, output, budget):
    """Discovery reads identifiers/counts/timestamps, without bids/asks/prices."""
    output = isolated(output)
    output.mkdir(parents=True,exist_ok=True)
    path = output/'asset_inventory.parquet'
    if path.exists():
        saved = json.loads((output/'inventory_manifest.json').read_text())
        if saved['sha256'] != sha256(path) or saved['url'] != archive_url(hour):
            raise ValueError('Inventory cache changed')
        return pd.read_parquet(path),saved
    tables = []
    with RangeFile(archive_url(hour),budget) as source:
        file = pq.ParquetFile(source)
        for i in range(file.num_row_groups):
            table = file.read_row_group(i,columns=['market','asset_id','event_type','timestamp','timestamp_received'],use_threads=False)
            small = table.group_by(['market','asset_id','event_type']).aggregate([
                ('timestamp_received','min'),('timestamp_received','max'),
                ('timestamp','min'),('timestamp','max'),('asset_id','count')])
            tables.append(small.to_pandas())
            if i%10 == 0:
                print(f'Archive identifier inventory: group {i+1}/{file.num_row_groups}, {budget.used/1e6:.1f} MB transferred',flush=True)
        manifest = dict(url=source.url,object_bytes=source.size,etag=source.etag,
            row_groups=file.num_row_groups,rows=file.metadata.num_rows,read_columns=['market','asset_id','event_type','timestamp','timestamp_received'])
    frame = pd.concat(tables,ignore_index=True).groupby(['market','asset_id','event_type'],as_index=False).agg(
        first_received=('timestamp_received_min','min'),last_received=('timestamp_received_max','max'),
        first_source=('timestamp_min','min'),last_source=('timestamp_max','max'),rows=('asset_id_count','sum'))
    frame['market'] = frame.market.map(lambda x:x.decode('ascii') if isinstance(x,bytes) else x)
    frame.to_parquet(path,index=False)
    manifest.update(sha256=sha256(path),transfer_bytes=budget.used)
    write_json(output/'inventory_manifest.json',manifest)
    return frame,manifest


def extract_hour(hour, folder, assets, conditions, budget):
    folder = isolated(folder)
    folder.mkdir(parents=True,exist_ok=True)
    name = pd.Timestamp(hour).strftime('%Y-%m-%dT%H')
    path,manifest_path = folder/f'{name}.parquet',folder/f'{name}.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest['sha256'] != sha256(path) or set(manifest['assets']) != set(assets):
            raise ValueError('Selected archive cache/selection changed')
        return path,manifest
    parts = []
    before = budget.used
    with RangeFile(archive_url(hour),budget) as source:
        file = pq.ParquetFile(source)
        condition_bytes = sorted(str(c).encode('ascii') for c in conditions)
        column = file.schema_arrow.get_field_index('market')
        asset_set = pa.array(sorted(assets),type=pa.string())
        read_groups = 0
        for i in range(file.num_row_groups):
            stats = file.metadata.row_group(i).column(column).statistics
            if stats and stats.has_min_max and not any(stats.min<=c<=stats.max for c in condition_bytes):
                continue
            ids = file.read_row_group(i,columns=['asset_id'],use_threads=False)
            mask = pc.is_in(ids['asset_id'],value_set=asset_set)
            if not pc.any(mask).as_py():
                continue
            source.cache_row_group(file.metadata.row_group(i))
            table = file.read_row_group(i,use_threads=False)
            # Original row number is a deterministic receipt-time tie breaker.
            offset = sum(file.metadata.row_group(j).num_rows for j in range(i))
            table = table.append_column('archive_row',pa.array(range(offset,offset+len(table)),type=pa.int64()))
            parts.append(table.filter(mask));read_groups += 1
            if read_groups%10 == 0:
                print(f'Extract {name}: {read_groups} selected row groups, {(budget.used-before)/1e6:.1f} MB transferred',flush=True)
        selected = pa.concat_tables(parts) if parts else pa.Table.from_batches([],schema=file.schema_arrow.append(pa.field('archive_row',pa.int64())))
        manifest = dict(url=source.url,object_bytes=source.size,etag=source.etag,assets=sorted(assets),
            row_groups=file.num_row_groups,read_groups=read_groups,selected_rows=len(selected),transfer_bytes=budget.used-before)
    pq.write_table(selected,path,compression='zstd')
    manifest['sha256'] = sha256(path)
    write_json(manifest_path,manifest)
    print(f'Extracted {name}: {len(selected):,} selected rows; {(budget.used-before)/1e6:.1f} MB transferred',flush=True)
    return path,manifest
