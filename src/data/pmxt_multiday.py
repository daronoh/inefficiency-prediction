"""Frozen multi-day PMXT feasibility pilot; never modifies the Dome study.

Select dates before downloading prices. Keep the original universe and v3
reconstruction unchanged. Each block has one warm-up and two receipt-tail hours.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import re
import shutil
import threading
import time

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from urllib3.exceptions import HTTPError as TransportError

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import TransferBudget, RangeFile, archive_url, isolated, write_json
from src.data.pmxt_pilot import DEFAULT_ROOT, load_plan


STUDY_ROOT = DEFAULT_ROOT.parent / 'pilot_multiday_v1'
BLOCKS = (
    ('may04', '2026-05-04T01:00:00Z'),
    ('may06', '2026-05-06T09:00:00Z'),
    ('may08', '2026-05-08T17:00:00Z'),
    ('may10', '2026-05-10T09:00:00Z'),
)


class SharedBudget(TransferBudget):
    """A single bounded transfer allowance across concurrent public reads."""
    def __init__(self, maximum, used=0):
        super().__init__(maximum, used)
        self.lock = threading.Lock()

    def charge(self, size):
        with self.lock:
            super().charge(size)


class FileBudget:
    """Per-file accounting plus one shared hard ceiling; retries count too."""
    def __init__(self, shared):
        self.shared, self.used = shared, 0

    def charge(self, size):
        self.shared.charge(size)
        self.used += size


class RetryingRangeFile(RangeFile):
    def request_range(self, start, stop):
        """Verify response identity instead of sending a slow If-Match request.

        The tested R2 endpoint serves unconditioned ranges promptly but stalls
        on conditional bodies. Every returned ETag and object length must still
        match the first response, so an object change fails closed.
        """
        suffix = start < 0
        requested = -start if suffix else stop-start
        if not 0 < requested <= 64*1024*1024:
            raise ValueError('Unexpectedly large or empty archive range')
        self.budget.charge(requested)
        value = f'bytes={start}' if suffix else f'bytes={start}-{stop-1}'
        began = time.monotonic()
        with self.session.get(self.url,headers={'Range':value,'Accept-Encoding':'identity','Connection':'close'},
                stream=True,timeout=(10,30),allow_redirects=False) as response:
            if response.status_code in (429,500,502,503,504):
                raise requests.HTTPError(f'Retryable archive HTTP {response.status_code}')
            if response.status_code != 206:
                raise RuntimeError(f'Archive range HTTP {response.status_code}; never treated as empty')
            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',response.headers.get('Content-Range',''))
            if not match:
                raise ValueError('Missing or malformed Content-Range')
            a,b,total = map(int,match.groups())
            if (self.size is not None and self.size!=total) or b-a+1!=requested or a!=(total+start if suffix else start):
                raise ValueError('Archive range or object length changed')
            tag = response.headers.get('ETag')
            if not tag or (self.etag is not None and tag!=self.etag):
                raise ValueError('Archive ETag missing or changed')
            if int(response.headers.get('Content-Length',requested))!=requested:
                raise ValueError('Unexpected response body length')
            chunks = []
            remaining = requested
            while remaining:
                if time.monotonic()-began > 60:
                    raise requests.ConnectionError('Archive range exceeded 60-second wall-clock limit')
                chunk = response.raw.read1(min(64*1024,remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b''.join(chunks)
            if len(data)!=requested:
                raise requests.ConnectionError('Truncated archive range')
            self.size,self.etag = total,tag
            self.requests += 1
            return data

    def fetch(self, start, stop):
        for attempt in range(4):
            try:
                return self.request_range(start, stop)
            except (requests.RequestException, TransportError) as exc:
                if attempt == 3:
                    raise
                print(f'Archive transport retry {attempt+1}/3: {type(exc).__name__}',flush=True)
                self.session.close()
                self.session = requests.Session()
                time.sleep(2**attempt)


def extract_bounded(hour, folder, assets, conditions, shared):
    """Stage bounded archive chunks, then retain only selected-token records.

    The small fixed universe intersects most row groups. Many tiny remote column
    reads are slow and fragile, so explicit full-object staging is more reliable.
    Temporary archives are removed only after the selected subset is committed.
    """
    folder = isolated(folder)
    folder.mkdir(parents=True, exist_ok=True)
    name = pd.Timestamp(hour).strftime('%Y-%m-%dT%H')
    path, manifest_path = folder/f'{name}.parquet', folder/f'{name}.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest['sha256'] != sha256(path) or set(manifest['assets']) != set(assets):
            raise ValueError('Selected archive cache/selection changed')
        return path, manifest
    budget = FileBudget(shared)
    parts = []
    with RetryingRangeFile(archive_url(hour), budget) as source:
        if source.size > 2_000_000_000:
            raise RuntimeError('Unexpectedly large individual archive for the bounded pilot')
        staged = folder/f'{name}.archive.partial'
        staged_meta = folder/f'{name}.archive.json'
        identity = dict(url=source.url,etag=source.etag,object_bytes=source.size)
        previous_charge = 0
        if staged.exists() and staged_meta.exists():
            saved = json.loads(staged_meta.read_text())
            if saved['identity'] != identity or saved['sha256'] != sha256(staged):
                raise ValueError('Staged archive identity or bytes changed')
            previous_charge = saved['charged_bytes']
        else:
            if staged.exists() and staged.stat().st_size:
                raise ValueError('Nonempty staging file lacks its identity manifest')
            staged.touch()
            write_json(staged_meta,dict(identity=identity,sha256=sha256(staged),charged_bytes=0))
        position = staged.stat().st_size
        if position > source.size:
            raise ValueError('Oversized staged archive')
        with staged.open('ab') as target:
            while position < source.size:
                stop = min(position+32*1024*1024,source.size)
                target.write(source.fetch(position,stop))
                target.flush()
                position = stop
                write_json(staged_meta,dict(identity=identity,sha256=sha256(staged),
                    charged_bytes=previous_charge+budget.used))
                print(f'Stage {name}: {position/1e6:.1f}/{source.size/1e6:.1f} MB',flush=True)
        file = pq.ParquetFile(staged)
        condition_bytes = sorted(str(c).encode('ascii') for c in conditions)
        column = file.schema_arrow.get_field_index('market')
        asset_set = pa.array(sorted(assets), type=pa.string())
        read_groups = 0
        offset = 0
        for i in range(file.num_row_groups):
            group = file.metadata.row_group(i)
            stats = group.column(column).statistics
            if stats and stats.has_min_max and not any(stats.min<=c<=stats.max for c in condition_bytes):
                offset += group.num_rows
                continue
            ids = file.read_row_group(i, columns=['asset_id'], use_threads=False)
            mask = pc.is_in(ids['asset_id'], value_set=asset_set)
            if pc.any(mask).as_py():
                table = file.read_row_group(i, use_threads=False)
                table = table.append_column('archive_row', pa.array(range(offset,offset+len(table)),type=pa.int64()))
                parts.append(table.filter(mask))
                read_groups += 1
                if read_groups%20 == 0:
                    print(f'Extract {name}: {read_groups} selected row groups, {budget.used/1e6:.1f} MB charged', flush=True)
            offset += group.num_rows
        selected = pa.concat_tables(parts) if parts else pa.Table.from_batches([],schema=file.schema_arrow.append(pa.field('archive_row',pa.int64())))
        manifest = dict(url=source.url, object_bytes=source.size, etag=source.etag, assets=sorted(assets),
            row_groups=file.num_row_groups, read_groups=read_groups, selected_rows=len(selected),
            transfer_bytes=previous_charge+budget.used, accounting='Explicit staged full-object ranges, including retried requested ranges; shared ledger also includes failed attempts.')
        file.close()
    temporary = path.with_suffix('.parquet.partial')
    pq.write_table(selected,temporary,compression='zstd')
    temporary.replace(path)
    manifest['sha256'] = sha256(path)
    write_json(manifest_path,manifest)
    for temporary in (staged,staged_meta):
        if not temporary.resolve().is_relative_to(folder):
            raise ValueError('Staging cleanup escaped isolated raw folder')
        temporary.unlink()
    print(f'Extracted {name}: {len(selected):,} rows; {budget.used/1e6:.1f} MB charged', flush=True)
    return path,manifest


def freeze(root=STUDY_ROOT, source=DEFAULT_ROOT):
    root = isolated(root)
    source, original = load_plan(source)
    root.mkdir(parents=True, exist_ok=True)
    blocks = []
    for name, value in BLOCKS:
        start = pd.Timestamp(value)
        end = start + pd.Timedelta(hours=6)
        if not pd.Timestamp(original['start']) <= start < end <= pd.Timestamp(original['end']):
            raise ValueError('Development pilot block outside original pilot window')
        blocks.append(dict(name=name, start=start.isoformat(), analysis_end=end.isoformat(),
            warmup_start=(start-pd.Timedelta(hours=1)).isoformat(),
            receipt_end=(end+pd.Timedelta(hours=2)).isoformat()))
    design = dict(version=1, source_root=str(source), source_plan_sha256=sha256(source/'plan.json'),
        blocks=blocks, analysis_hours=24, events=original['selected'],
        selection='Same frozen events; deterministic dates/times; no outcome-dependent additions or exclusions.',
        prior_evidence='May04 01-04 was previously inspected; the remaining 21 hours were not inspected at design freeze.',
        warmup_hours=1, receipt_tail_hours=2, reconstruction='unchanged source_validation_v3',
        maximum_new_range_bytes=16_000_000_000, maximum_raw_cache_bytes=1_000_000_000,
        acceptance_targets=dict(full_ask_time_fraction=.95, certified_occurrence_fraction=.90,
            typical_ask_complete_events_per_scan=10),
        acceptance_note='Engineering targets, not industry standards. Also report full bid/ask coverage; no acceptance based on profits.',
        training='No model fitting or final-test access; report positive-label support before deciding on a larger dataset.',
        trading='Conditional zero-fee/min5/initial .0001 tick quote study; 100ms uncapped economic evaluation remains separate.',
        historical_parameters_verified=False)
    saved = root/'study_design.json'
    if saved.exists():
        if json.loads(saved.read_text()) != design:
            raise ValueError('Multi-day design already frozen differently')
    else:
        write_json(saved, design)
    for block in blocks:
        folder = root/block['name']
        folder.mkdir(exist_ok=True)
        for filename in ('events.parquet', 'markets.parquet', 'packages.parquet'):
            target = folder/filename
            if target.exists():
                if sha256(target) != sha256(source/filename):
                    raise ValueError('Multi-day universe differs from frozen original')
            else:
                shutil.copy2(source/filename, target)
        plan = dict(original, start=block['start'], end=block['receipt_end'],
            warmup_start=block['warmup_start'], parent_plan_sha256=design['source_plan_sha256'],
            selection=design['selection'])
        path = folder/'plan.json'
        if path.exists() and json.loads(path.read_text()) != plan:
            raise ValueError('Block plan changed')
        if not path.exists():
            write_json(path, plan)
        raw = folder/'raw'
        raw.mkdir(exist_ok=True)
        # Copy only already verified selected-token files; never mutate v1.
        for hour in pd.date_range(block['warmup_start'], block['receipt_end'], freq='h', inclusive='left'):
            name = f'{hour:%Y-%m-%dT%H}'
            manifest = source/'raw'/f'{name}.json'
            if not manifest.exists():
                continue
            metadata = json.loads(manifest.read_text())
            path = manifest.with_suffix('.parquet')
            if sha256(path) != metadata['sha256'] or set(metadata['assets']) != set(plan['assets']):
                raise ValueError('Original raw cache failed verification')
            for origin in (path, manifest):
                target = raw/origin.name
                if target.exists() and sha256(target) != sha256(origin):
                    raise ValueError('Copied raw file changed')
                if not target.exists():
                    shutil.copy2(origin, target)
    return design


def download(root=STUDY_ROOT, *, workers=3):
    root = isolated(root)
    design = freeze(root)
    ledger = root/'download_ledger.json'
    prior = json.loads(ledger.read_text()) if ledger.exists() else {}
    budget = SharedBudget(design['maximum_new_range_bytes'], prior.get('charged_range_bytes', 0))
    jobs = []
    for block in design['blocks']:
        folder, plan = load_plan(root/block['name'])
        for hour in pd.date_range(block['warmup_start'], block['receipt_end'], freq='h', inclusive='left'):
            jobs.append((hour, folder, plan))
    completed = []
    errors = []

    def fetch(job):
        hour, folder, plan = job
        if shutil.disk_usage(root).free < 2_000_000_000:
            raise RuntimeError('Insufficient disk reserve for bounded pilot')
        return extract_bounded(hour, folder/'raw', plan['assets'], set(plan['asset_conditions'].values()), budget)

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch, job): job for job in jobs}
            for future in as_completed(futures):
                hour, folder, _ = futures[future]
                try:
                    path, manifest = future.result()
                    completed.append(str(path.relative_to(root)))
                    cache = sum(p.stat().st_size for p in root.glob('*/raw/*.parquet'))
                    if cache > design['maximum_raw_cache_bytes']:
                        raise RuntimeError('Pilot raw-cache size allowance exceeded')
                except Exception as exc:
                    errors.append(dict(hour=str(hour), block=folder.name, error=str(exc)))
                    for pending in futures:
                        pending.cancel()
                    raise
                finally:
                    write_json(ledger, dict(charged_range_bytes=budget.used, completed_this_invocation=completed,
                        errors=errors, updated_at=pd.Timestamp.now(tz='UTC')))
    finally:
        write_json(ledger, dict(charged_range_bytes=budget.used, completed_this_invocation=completed,
            errors=errors, updated_at=pd.Timestamp.now(tz='UTC')))
    return dict(cached_archive_hours=len(jobs), new_range_bytes_charged=budget.used,
        raw_cache_bytes=sum(p.stat().st_size for p in root.glob('*/raw/*.parquet')))


def replay(root=STUDY_ROOT):
    from src.data.pmxt_revalidation import rerun
    root = isolated(root)
    design = freeze(root)
    summaries = {}
    for block in design['blocks']:
        print(f"Replaying {block['name']} {block['start']} to {block['analysis_end']}", flush=True)
        summaries[block['name']] = rerun(root/block['name'], analysis_end=block['analysis_end'])
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('step', choices=['freeze', 'download', 'replay'])
    parser.add_argument('--root', type=Path, default=STUDY_ROOT)
    parser.add_argument('--workers', type=int, choices=range(1, 5), default=3)
    args = parser.parse_args()
    result = download(args.root, workers=args.workers) if args.step == 'download' else globals()[args.step](args.root)
    print(json.dumps(result, default=str, indent=2))


if __name__ == '__main__':
    main()
