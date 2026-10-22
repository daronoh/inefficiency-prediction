"""Freeze public event metadata for discovery, without inspecting trading returns.

The catalogue is retrospective. Matching a contract does not establish historical
membership, metadata availability, fee history, or order-book coverage.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pandas as pd
import requests

from src.common import sha256
from src.data.contract_templates import parse_contract
from src.data.universe_detection import detect_universe, normalise_event, normalise_market

URL = 'https://gamma-api.polymarket.com/events/keyset'
EVENT_FIELDS = (
    'id', 'slug', 'title', 'description', 'resolutionSource', 'startDate',
    'endDate', 'createdAt', 'updatedAt', 'negRisk', 'negRiskMarketID',
    'negRiskAugmented', 'category',
)
MARKET_FIELDS = (
    'id', 'conditionId', 'slug', 'question', 'description', 'resolutionSource',
    'startDate', 'endDate', 'createdAt', 'updatedAt', 'outcomes', 'clobTokenIds',
    'negRisk', 'negRiskMarketID', 'negRiskOther',
)


def metadata_only(event):
    """Exclude prices, outcomes' realised values, volume, and current trading state."""
    result = {key: event.get(key) for key in EVENT_FIELDS}
    result['markets'] = [
        {key: market.get(key) for key in MARKET_FIELDS}
        for market in event.get('markets', [])
    ]
    return result


def bounds(start, end, final_start):
    stamps = [pd.Timestamp(v) for v in (start, end, final_start)]
    if any(t.tzinfo is None for t in stamps):
        raise ValueError('Explicit UTC offsets are required')
    lo, hi, test = [t.tz_convert('UTC') for t in stamps]
    if not lo < test < hi:
        raise ValueError('Require start < final_start < end')
    return lo, hi, test


def fetch_catalogue(folder, *, start, end, final_start, max_pages=2000,
                    listing_min=None, listing_max=None):
    """Resume immutable pages; fail closed if a safety limit or API error is hit."""
    lo, hi, test = bounds(start, end, final_start)
    folder = Path(folder)
    pages = folder / 'pages'
    pages.mkdir(parents=True, exist_ok=True)
    params = dict(start_date_max=hi.isoformat(), end_date_min=lo.isoformat(),
                  order='id', ascending='true', limit=100)
    if listing_min is not None:
        params['start_date_min'] = pd.Timestamp(listing_min).isoformat()
    if listing_max is not None:
        params['start_date_max'] = pd.Timestamp(listing_max).isoformat()
    design = dict(url=URL, params=params, start=lo.isoformat(), end=hi.isoformat(),
                  final_start=test.isoformat(), metadata_mode='frozen',
                  selection='All returned events, open and closed; no volume or return filters.',
                  scope='Current catalogue nominal date overlap; null dates and historical revisions may be missed.',
                  final_test='Metadata discovery only; no prices, labels, P&L, or model selection.',
                  source_sha256=sha256(__file__))
    design_path = folder / 'acquisition.json'
    if design_path.exists():
        previous = json.loads(design_path.read_text())
        if any(previous[k] != design[k] for k in ('url', 'params', 'start', 'end', 'final_start')):
            raise ValueError('Resume parameters differ from the frozen acquisition')
    else:
        design['started_at'] = pd.Timestamp.now(tz='UTC').isoformat()
        design_path.write_text(json.dumps(design, indent=2) + '\n')
    completed_path = folder / 'complete.json'
    if completed_path.exists():
        completion = json.loads(completed_path.read_text())
        if completion['acquisition_sha256'] != sha256(design_path):
            raise ValueError('Completed acquisition manifest hash mismatch')
        for name, expected in completion['page_hashes'].items():
            if sha256(pages / name) != expected:
                raise ValueError('Completed acquisition page hash mismatch')
        return completion
    session = requests.Session()
    seen = set()
    total_markets = 0
    cursor = None
    for page in range(max_pages):
        path = pages / f'{page:06d}.json.gz'
        if path.exists():
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                saved = json.load(f)
            if saved['cursor'] != cursor:
                raise ValueError('Cached cursor mismatch')
        else:
            query = dict(params)
            if cursor is not None:
                query['after_cursor'] = cursor
            for attempt in range(5):
                response = session.get(URL, params=query,
                                       timeout=45, allow_redirects=False)
                if response.status_code not in (429, 500, 502, 503, 504):
                    break
                delay = response.headers.get('Retry-After', str(2 ** attempt))
                try:
                    delay = float(delay)
                except ValueError:
                    delay = 2 ** attempt
                if delay > 60:
                    raise RuntimeError('Long server backoff requested; resume later')
                time.sleep(max(0, delay))
            if response.status_code != 200:
                raise RuntimeError(f'Catalogue HTTP {response.status_code} at page {page}')
            body = response.json()
            if not isinstance(body, dict) or not isinstance(body.get('events'), list):
                raise ValueError('Expected a cursor envelope containing events')
            raw = body['events']
            saved = dict(cursor=cursor, next_cursor=body.get('next_cursor'),
                         fetched_at=pd.Timestamp.now(tz='UTC').isoformat(),
                         response_sha256=hashlib.sha256(response.content).hexdigest(),
                         events=[metadata_only(e) for e in raw])
            temporary = path.with_suffix('.tmp')
            with gzip.open(temporary, 'wt', encoding='utf-8') as f:
                json.dump(saved, f)
            temporary.replace(path)
        rows = saved['events']
        for event in rows:
            eid = str(event['id'])
            if eid in seen:
                raise ValueError(f'Duplicate event across pages: {eid}; pagination may have shifted')
            seen.add(eid)
            total_markets += len(event['markets'])
        next_cursor = saved['next_cursor']
        if next_cursor is not None and next_cursor == cursor:
            raise ValueError('Repeated cursor; catalogue incomplete')
        if page % 10 == 0 or not rows:
            print(f'{folder.name} page {page}: {len(seen):,} events, {total_markets:,} markets', flush=True)
        # A short page is not exhaustion when the server supplies another cursor.
        if not next_cursor:
            completion = dict(events=len(seen), markets=total_markets, pages=page + 1,
                              completed_at=pd.Timestamp.now(tz='UTC').isoformat(),
                              acquisition_sha256=sha256(design_path),
                              page_hashes={p.name: sha256(p) for p in sorted(pages.glob('*.json.gz'))})
            (folder / 'complete.json').write_text(json.dumps(completion, indent=2) + '\n')
            return completion
        cursor = next_cursor
    raise RuntimeError('Page budget reached; catalogue incomplete, resume with a larger budget')


def fetch_partitions(folder, *, start, end, final_start, max_pages=2000):
    """Disjoint listing-date slices allow four independent metadata requests."""
    lo, hi, _ = bounds(start, end, final_start)
    folder = Path(folder)
    if lo.day != 1 or hi.day != 1:
        raise ValueError('Monthly discovery requires month-start boundaries')
    edges = list(pd.date_range(lo, hi, freq='MS'))
    slices = [('earlier_listings', None, lo - pd.Timedelta(microseconds=1))]
    slices += [(a.strftime('%Y_%m'), a, b - pd.Timedelta(microseconds=1))
               for a, b in zip(edges, edges[1:])]

    def fetch_one(item):
        name, first, last = item
        return fetch_catalogue(folder / name, start=start, end=end, final_start=final_start,
                               max_pages=max_pages, listing_min=first, listing_max=last)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(fetch_one, slices))
    manifest = dict(start=str(lo), end=str(hi), final_start=final_start,
                    partitions=[s[0] for s in slices], results=results,
                    note='Partitioned by current listing date, not by profits; earlier listings included.')
    (folder / 'partitions.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


def analyse_partitions(folder, *, allow_partial=False):
    folder = Path(folder)
    if (folder / 'partitions.json').exists():
        manifest = json.loads((folder / 'partitions.json').read_text())
    elif allow_partial:
        names = sorted(p.name for p in folder.iterdir() if (p / 'acquisition.json').exists())
        manifest = dict(partitions=names)
    else:
        raise ValueError('Acquisition is incomplete; explicit --allow-partial is required')
    incomplete = [name for name in manifest['partitions'] if not (folder / name / 'complete.json').exists()]
    counts, reasons = Counter(), Counter()
    frames = {}
    for name in manifest['partitions']:
        child = folder / name
        child_output = child / ('discovery_partial' if name in incomplete else 'discovery')
        saved_summary = child_output / 'summary.json'
        evidence = child / ('partial_acquisition.json' if name in incomplete else 'complete.json')
        if saved_summary.exists():
            summary = json.loads(saved_summary.read_text())
            if summary.get('completion_sha256') != sha256(evidence):
                raise ValueError('Discovery input fingerprint changed')
            if summary.get('detector_sha256') != sha256(Path(__file__).with_name('universe_detection.py')):
                raise ValueError('Discovery detector changed')
            if summary.get('templates_sha256') != sha256(Path(__file__).with_name('contract_templates.py')):
                raise ValueError('Discovery templates changed')
        else:
            summary = analyse_catalogue(child, allow_partial=allow_partial)
        counts.update(summary['counts'])
        reasons.update(summary['reasons'])
        for path in child_output.glob('*.parquet'):
            frames.setdefault(path.name, []).append(pd.read_parquet(path))
    output = folder / ('discovery_partial' if incomplete else 'discovery')
    output.mkdir(exist_ok=False)
    for name, pieces in frames.items():
        result = pd.concat(pieces, ignore_index=True)
        if name == 'events.parquet' and not result.empty and result.event_id.duplicated().any():
            raise ValueError('Events overlap between listing-date partitions')
        result.to_parquet(output / name, index=False)
        if name in ('events.parquet', 'ladder_pairs.parquet'):
            result.to_csv(output / name.replace('.parquet', '.csv'), index=False)
    summary.update(counts=dict(counts), reasons=dict(reasons),
                   partitions=manifest['partitions'], catalogue_complete=not incomplete,
                   incomplete_partitions=incomplete, source_sha256=sha256(__file__))
    summary.pop('completion_sha256', None)
    summary['outputs_sha256'] = {p.name: sha256(p) for p in output.glob('*.parquet')}
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print('Combined discovery:', json.dumps(summary, indent=2), flush=True)
    return summary


def _prepare_event(event):
    market_rows = []
    for market in event['markets']:
        row = normalise_market(dict(market, event_id=str(event['id']), event_slug=event['slug']))
        market_rows.append(row)
    if not market_rows:
        return market_rows, False
    if event.get('negRisk') is not True and not any(r['neg_risk'] is True for r in market_rows):
        if len(market_rows) < 2:
            return market_rows, False
        matches = 0
        for row in market_rows:
            try:
                parse_contract(row)
                matches += 1
            except (ValueError, TypeError, OverflowError):
                pass
        if matches < 2:
            return market_rows, False
    return market_rows, True


def classify_event(event, start, end):
    """Reuse the strict detector; never attest exhaustive membership automatically."""
    market_rows, candidate = _prepare_event(event)
    if not candidate:
        return None, market_rows
    inventory = normalise_event(event)
    result = detect_universe(pd.DataFrame(market_rows), pd.DataFrame([inventory]),
                             window_start=start, window_end=end, metadata_mode='frozen')
    return result, market_rows


def classify_events(events, start, end):
    """Batch independent event groups; isolate malformed records on fallback."""
    output, prepared, inventory = {}, {}, []
    for event in events:
        eid = str(event['id'])
        try:
            rows, candidate = _prepare_event(event)
            if candidate:
                prepared[eid] = rows
                inventory.append(normalise_event(event))
            else:
                output[eid] = (None, rows)
        except (ValueError, TypeError, OverflowError) as error:
            output[eid] = error
    if not prepared:
        return output
    try:
        result = detect_universe(pd.DataFrame([r for rows in prepared.values() for r in rows]),
                                 pd.DataFrame(inventory), window_start=start, window_end=end,
                                 metadata_mode='frozen')
        event_groups = dict(tuple(result.events.groupby('event_id')))
        pair_groups = dict(tuple(result.pairs.groupby('event_id')))
        for eid, rows in prepared.items():
            output[eid] = (SimpleNamespace(events=event_groups[eid],
                                          pairs=pair_groups.get(eid, result.pairs.iloc[:0])), rows)
    except (ValueError, TypeError, OverflowError):
        # Cross-event identity conflicts must not silently remove an entire page.
        for event in events:
            eid = str(event['id'])
            if eid not in prepared:
                continue
            try:
                output[eid] = classify_event(event, start, end)
            except (ValueError, TypeError, OverflowError) as error:
                output[eid] = error
    return output


def analyse_catalogue(folder, *, allow_partial=False):
    folder = Path(folder)
    design = json.loads((folder / 'acquisition.json').read_text())
    complete = (folder / 'complete.json').exists()
    evidence = folder / ('complete.json' if complete else 'partial_acquisition.json')
    if not complete:
        if not allow_partial:
            raise ValueError('Acquisition is incomplete; explicit --allow-partial is required')
        partial = dict(acquisition_sha256=sha256(folder / 'acquisition.json'),
                       page_hashes={p.name: sha256(p) for p in sorted((folder / 'pages').glob('*.json.gz'))},
                       catalogue_complete=False, reason='Pagination interrupted before exhaustion')
        evidence.write_text(json.dumps(partial, indent=2) + '\n')
    completion = json.loads(evidence.read_text())
    if completion['acquisition_sha256'] != sha256(folder / 'acquisition.json'):
        raise ValueError('Acquisition manifest hash mismatch')
    start, end, final = bounds(design['start'], design['end'], design['final_start'])
    output = folder / ('discovery' if complete else 'discovery_partial')
    output.mkdir(exist_ok=False)
    summaries, pair_frames, canonical, inventories, tokens = [], [], [], [], []
    counts = Counter()
    reasons = Counter()
    for name, expected_hash in completion['page_hashes'].items():
        path = folder / 'pages' / name
        if sha256(path) != expected_hash:
            raise ValueError(f'Page hash mismatch: {name}')
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            events = json.load(f)['events']
        classified = classify_events(events, start, end)
        for event in events:
            counts['events_scanned'] += 1
            counts['markets_scanned'] += len(event['markets'])
            if len(event['markets']) == 1 and not (
                event.get('negRisk') is True or event['markets'][0].get('negRisk') is True
            ):
                counts['unclassified_events'] += 1
                reasons['single_non_negrisk_market'] += 1
                continue
            try:
                item = classified[str(event['id'])]
                if isinstance(item, Exception):
                    raise item
                result, rows = item
            except (ValueError, TypeError, OverflowError) as error:
                counts['invalid_events'] += 1
                reasons[str(error)] += 1
                summaries.append(dict(event_id=str(event['id']), event_slug=event['slug'],
                                      classification='invalid', reason=str(error)))
                continue
            if result is None:
                counts['unclassified_events' if rows else 'empty_events'] += 1
                reasons['no_eligible_supported_pairs' if rows else 'empty_event'] += 1
                continue
            audit = result.events.iloc[0].to_dict()
            counts[audit['classification'] + '_events'] += 1
            reasons[audit['reason']] += 1
            if audit['classification'] not in ('negrisk', 'ladder'):
                continue
            audit.update(title=event['title'], nominal_start=event['startDate'],
                         nominal_end=event['endDate'], augmented=event['negRiskAugmented'],
                         historical_metadata_verified=False, book_coverage_verified=False)
            summaries.append(audit)
            canonical.extend(rows)
            inventories.append(normalise_event(event))
            if audit['classification'] == 'ladder':
                pair_frames.append(result.pairs)
                counts['ladder_pairs'] += len(result.pairs)
                needed = set(result.pairs.broad_yes_token) | set(result.pairs.narrow_no_token)
            else:
                # This is an acquisition candidate, never an accepted trading basket.
                needed = {r['yes_token'] for r in rows if r['yes_token']}
                if event['negRiskAugmented'] is True:
                    counts['augmented_negrisk_events'] += 1
                elif event['negRiskAugmented'] is False:
                    counts['explicit_nonaugmented_negrisk_events'] += 1
                else:
                    counts['unknown_augmentation_negrisk_events'] += 1
            for row in rows:
                for side in ('yes', 'no'):
                    token = row[side + '_token']
                    if token in needed:
                        tokens.append(dict(event_id=row['event_id'], event_slug=row['event_slug'],
                                           classification=audit['classification'], market_id=row['market_id'],
                                           token_id=token, side=side,
                                           basket_verified=audit['classification'] == 'ladder'))
        if counts['events_scanned'] % 5000 == 0:
            print('Discovery:', dict(counts), flush=True)
    pd.DataFrame(summaries).to_parquet(output / 'events.parquet', index=False)
    pd.DataFrame(summaries).to_csv(output / 'events.csv', index=False)
    pd.DataFrame(canonical).to_parquet(output / 'markets.parquet', index=False)
    pd.DataFrame(inventories).to_parquet(output / 'event_inventory.parquet', index=False)
    pd.DataFrame(tokens).to_parquet(output / 'request_tokens.parquet', index=False)
    pairs = pd.concat(pair_frames, ignore_index=True) if pair_frames else pd.DataFrame()
    pairs.to_parquet(output / 'ladder_pairs.parquet', index=False)
    summary = dict(counts=dict(counts), reasons=dict(reasons), start=str(start), end=str(end),
                   final_start=str(final), discovery_source='Polymarket Gamma public catalogue',
                   orderbook_source='FinData planned, coverage and provenance unverified',
                   source_sha256=sha256(__file__),
                   completion_sha256=sha256(evidence), catalogue_complete=complete,
                   detector_sha256=sha256(Path(__file__).with_name('universe_detection.py')),
                   templates_sha256=sha256(Path(__file__).with_name('contract_templates.py')),
                   limitations=[
                       'Current metadata, not a historical point-in-time inventory.',
                       'Date filtering uses nominal event dates; not verified trading lifetimes.',
                       'negRisk flags identify candidates; exhaustive membership remains unverified.',
                       'No order books, opportunity labels, returns, or final-test performance inspected.',
                       'No historical price or volume fields retained in metadata pages.',
                       'Recording continuity and source-specific quality require a separate coverage audit.',
                   ])
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['fetch', 'analyse', 'fetch-partitions', 'analyse-partitions'])
    parser.add_argument('--output', required=True)
    parser.add_argument('--start', default='2026-03-01T00:00:00Z')
    parser.add_argument('--end', default='2026-09-01T00:00:00Z')
    parser.add_argument('--final-start', default='2026-08-01T00:00:00Z')
    parser.add_argument('--max-pages', type=int, default=2000)
    parser.add_argument('--allow-partial', action='store_true')
    args = parser.parse_args()
    if args.action in ('fetch', 'fetch-partitions'):
        action = fetch_catalogue if args.action == 'fetch' else fetch_partitions
        action(args.output, start=args.start, end=args.end,
               final_start=args.final_start, max_pages=args.max_pages)
    elif args.action == 'analyse-partitions':
        analyse_partitions(args.output, allow_partial=args.allow_partial)
    else:
        analyse_catalogue(args.output, allow_partial=args.allow_partial)


if __name__ == '__main__':
    main()
