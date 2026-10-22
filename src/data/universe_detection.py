"""Discover supported economic relationships from a complete market export.

Classification and eligibility are separate: negRisk metadata can identify an
event even when the export cannot establish an exhaustive tradable basket.
This module does not load books, inspect returns or change the frozen study.
"""
from __future__ import annotations
import argparse
from dataclasses import dataclass
import json
from itertools import combinations
from pathlib import Path
import pandas as pd
from src.data.contract_templates import VERSION, fingerprint, parse_contract
from src.common import sha256


def missing(value):
    if isinstance(value, (list,dict,tuple)) or getattr(value,'ndim',0) > 0:
        return False
    return value is None or bool(pd.isna(value))


def text(value):
    return '' if missing(value) else str(value).strip()


def flag(value):
    if missing(value):
        return None
    if isinstance(value, bool) or type(value).__name__ == 'bool':
        return bool(value)
    if isinstance(value, str) and value.lower() in {'true', 'false'}:
        return value.lower() == 'true'
    raise ValueError('Boolean metadata must be true, false or missing')


def timestamp(value):
    if missing(value) or value == '':
        return None
    result = pd.Timestamp(value)
    if result.tzinfo is None:
        raise ValueError('Timestamps must have an explicit timezone')
    return result.tz_convert('UTC')


def array(value):
    if missing(value):
        return []
    result = json.loads(value) if isinstance(value, str) else value
    if hasattr(result, 'tolist'):
        result = result.tolist()
    if not isinstance(result, (list, tuple)):
        raise ValueError('Expected an array')
    return list(result)


def normalise_market(record):
    """Adapt canonical rows or Gamma-shaped market records; discard outcomes/prices.

    Multiple event memberships require an explicit event_id. Provider indexing
    timestamps are not silently treated as historical metadata availability.
    """
    events = array(record.get('events'))
    event = events[0] if len(events) == 1 else {}
    outcomes = array(record.get('outcomes', record.get('outcomes_raw')))
    tokens = array(record.get('clobTokenIds'))
    token_map = {text(side).upper(): text(token) for side,token in zip(outcomes,tokens)} if len(outcomes) == len(tokens) == 2 and {text(s).upper() for s in outcomes} == {'YES','NO'} else {}
    fields = ('contract_year', 'valid_until', 'metadata_available_at')
    result = {key: record.get(key) for key in fields}
    result.update(
        market_id=text(record.get('market_id', record.get('id'))),
        event_id=text(record.get('event_id', event.get('id'))),
        event_slug=text(record.get('event_slug', event.get('slug'))),
        question=text(record.get('question')), description=text(record.get('description')),
        resolution_source=text(record.get('resolution_source', record.get('resolutionSource'))),
        listed_at=record.get('listed_at', record.get('start_date', record.get('startDate'))),
        yes_token=text(record.get('yes_token', token_map.get('YES'))),
        no_token=text(record.get('no_token', token_map.get('NO'))),
        neg_risk=record.get('neg_risk', record.get('negRisk')),
        neg_risk_market_id=text(record.get('neg_risk_market_id', record.get('negRiskMarketID', event.get('negRiskMarketID')))))
    return result


def normalise_event(record):
    members = record.get('member_market_ids')
    if missing(members) and isinstance(record.get('markets'), list):
        members = [m.get('market_id',m.get('id')) for m in record['markets']]
    return dict(event_id=text(record.get('event_id',record.get('id'))),
        event_slug=text(record.get('event_slug',record.get('slug'))),
        neg_risk=record.get('neg_risk',record.get('negRisk')),
        neg_risk_augmented=record.get('neg_risk_augmented',record.get('negRiskAugmented')),
        neg_risk_market_id=text(record.get('neg_risk_market_id',record.get('negRiskMarketID'))),
        member_market_ids=array(members), complete_set_verified=record.get('complete_set_verified'),
        metadata_available_at=record.get('metadata_available_at'), valid_until=record.get('valid_until'))


PAIR_COLUMNS = ['pair_id','event_id','event_slug','kind','template','signature',
    'broad_market_id','narrow_market_id','broad_yes_token','narrow_no_token','window_start','window_end']
PACKAGE_COLUMNS = ['package_id','event_id','event_slug','stratum','market_ids','token_ids','window_start','window_end']


@dataclass
class DetectionResult:
    events: pd.DataFrame
    markets: pd.DataFrame
    pairs: pd.DataFrame
    packages: pd.DataFrame
    design: dict

    def write(self, output):
        output = Path(output)
        output.mkdir(parents=True, exist_ok=False)
        for name in ('events','markets','pairs','packages'):
            getattr(self,name).to_parquet(output/f'{name}.parquet',index=False)
        design = dict(self.design, files_sha256={name:sha256(output/f'{name}.parquet')
            for name in ('events','markets','pairs','packages')})
        (output/'design.json').write_text(json.dumps(design,indent=2)+'\n',encoding='utf-8')

    @classmethod
    def read(cls, output):
        output = Path(output)
        design = json.loads((output/'design.json').read_text(encoding='utf-8'))
        names = ('events','markets','pairs','packages')
        for name in names:
            if design['files_sha256'][name] != sha256(output/f'{name}.parquet'):
                raise ValueError(f'Detection artifact changed: {name}')
        return cls(*(pd.read_parquet(output/f'{name}.parquet') for name in names),design)


def detect_universe(markets, events=None, *, window_start, window_end, metadata_mode='point_in_time'):
    """Build event classifications, supported pairs and replay-compatible baskets.

    Each market/event must have one metadata version. For historical revisions,
    call separately on version intervals. Point-in-time mode uses the recorded
    metadata availability as the earliest allowed start. Frozen mode explicitly
    assumes retrospective rules/membership; it never uses winners or volume.
    """
    if metadata_mode not in {'point_in_time','frozen'}:
        raise ValueError('Unknown metadata_mode')
    start, end = timestamp(window_start), timestamp(window_end)
    if start is None or end is None or start >= end:
        raise ValueError('A nonempty timezone-aware analysis window is required')
    rows = [normalise_market(r) for r in markets.to_dict('records')]
    event_rows = [normalise_event(r) for r in events.to_dict('records')] if events is not None else []
    for records, key in ((rows,'market_id'),(event_rows,'event_id')):
        ids = [r[key] for r in records]
        if any(not value for value in ids) or len(ids) != len(set(ids)):
            raise ValueError(f'Missing or duplicate {key}; supply a single metadata version')
    registry = {r['event_id']:r for r in event_rows}
    # Slugs form downstream ranking keys: never collapse distinct parent events.
    slugs = {}
    for row in rows:
        slug = row['event_slug'] or row['event_id']
        if row['event_id'] and slug in slugs and slugs[slug] != row['event_id']:
            raise ValueError('Different event IDs share a ranking slug')
        if row['event_id']:
            slugs[slug] = row['event_id']
        row['event_slug'] = slug
    token_owners = {}
    for row in rows:
        for token in (row['yes_token'],row['no_token']):
            if token:
                token_owners.setdefault(token,[]).append(row['market_id'])
    invalid_tokens = {token for token, owners in token_owners.items() if len(owners) > 1}
    audits, groups = [], {}
    for row in rows:
        audit = {key:row[key] for key in ('market_id','event_id','event_slug')}
        audit.update(status='review',reason='',template=None,signature=None,threshold=None,direction=None,
            description_sha256=fingerprint(row['description']),window_start=None,window_end=None)
        audits.append(audit)
        row['_audit'] = audit
        if not row['event_id']:
            audit['reason'] = 'missing_or_ambiguous_event_membership'
            continue
        groups.setdefault(row['event_id'],[]).append(row)
        try:
            row['neg_risk'] = flag(row['neg_risk'])
            listed = timestamp(row['listed_at'])
            available = timestamp(row['metadata_available_at'])
            until = timestamp(row['valid_until'])
            if listed is None:
                raise ValueError('missing_listing_time')
            if metadata_mode == 'point_in_time' and available is None:
                raise ValueError('missing_metadata_availability')
            row['_start'] = max(t for t in (start,listed,available) if t is not None)
            row['_end'] = min(end,until) if until is not None else end
            if row['_start'] >= row['_end']:
                raise ValueError('outside_requested_window')
            if not row['yes_token'] or not row['no_token'] or any(t in invalid_tokens for t in (row['yes_token'],row['no_token'])):
                raise ValueError('missing_or_ambiguous_outcome_tokens')
            row['_valid'] = True
        except (ValueError,TypeError,OverflowError) as error:
            audit['reason'] = str(error)
    event_output, pairs, packages = [], [], []
    for event_id, members in sorted(groups.items()):
        event = registry.get(event_id,{})
        slug_set = {r['event_slug'] for r in members}
        if len(slug_set) != 1:
            raise ValueError(f'Inconsistent event slugs: {event_id}')
        slug = next(iter(slug_set))
        flagged = any(r['neg_risk'] is True for r in members) or flag(event.get('neg_risk')) is True
        summary = dict(event_id=event_id,event_slug=slug,classification='negrisk' if flagged else 'unclassified',
            status='review',reason='',markets=len(members),packages=0)
        event_output.append(summary)
        if flagged:
            reason = ''
            ids = {r['market_id'] for r in members}
            expected = list(map(str,event.get('member_market_ids',[])))
            if any(not r.get('_valid') for r in members):
                reason = 'ineligible_or_unavailable_member'
            elif any(r['neg_risk'] is not True for r in members) or flag(event.get('neg_risk')) is not True:
                reason = 'missing_or_inconsistent_negrisk_flags'
            elif flag(event.get('neg_risk_augmented')) is not False:
                reason = 'augmented_or_unknown_negrisk_structure'
            elif flag(event.get('complete_set_verified')) is not True:
                reason = 'exhaustive_outcome_set_not_verified'
            elif len(ids) < 2 or set(expected) != ids or len(expected) != len(ids):
                reason = 'incomplete_or_inconsistent_event_inventory'
            elif not event.get('neg_risk_market_id') or any(r['neg_risk_market_id'] != event['neg_risk_market_id'] for r in members):
                reason = 'missing_or_inconsistent_negrisk_contract_id'
            else:
                try:
                    available = timestamp(event.get('metadata_available_at'))
                    until = timestamp(event.get('valid_until'))
                    if metadata_mode == 'point_in_time' and available is None:
                        raise ValueError('missing_event_metadata_availability')
                    lo = max([r['_start'] for r in members]+([available] if available is not None else []))
                    hi = min([r['_end'] for r in members]+([until] if until is not None else []))
                    if lo >= hi:
                        raise ValueError('no_shared_trading_window')
                except (ValueError,TypeError) as error:
                    reason = str(error)
            summary.update(status='review' if reason else 'accepted',reason=reason or 'verified_complete_negrisk_set')
            for row in members:
                row['_audit'].update(status=summary['status'],reason=row['_audit']['reason'] or summary['reason'])
            if not reason:
                ordered = sorted(members,key=lambda r:r['market_id'])
                packages.append(dict(package_id=f'negrisk:{event_id}',event_id=event_id,event_slug=slug,stratum='negrisk',
                    market_ids=[r['market_id'] for r in ordered],token_ids=[r['yes_token'] for r in ordered],window_start=lo,window_end=hi))
                summary['packages'] = 1
            continue
        compatible = {}
        for row in members:
            if not row.get('_valid'):
                continue
            try:
                contract = parse_contract(row)
                lo = max(row['_start'],contract.start) if contract.start is not None else row['_start']
                hi = min(row['_end'],contract.end) if contract.end is not None else row['_end']
                if lo >= hi:
                    raise ValueError('outside_contract_window')
                row['_start'],row['_end'],row['_contract'] = lo,hi,contract
                row['_audit'].update(status='unpaired',reason='no_compatible_partner',template=contract.template,
                    signature=contract.signature,threshold=str(contract.threshold),direction=contract.direction,
                    window_start=lo,window_end=hi)
                compatible.setdefault((contract.template,contract.signature),[]).append(row)
            except (ValueError,TypeError,OverflowError) as error:
                row['_audit']['reason'] = str(error)
        for (_,signature), group in sorted(compatible.items()):
            for a,b in combinations(sorted(group,key=lambda r:r['market_id']),2):
                ca,cb = a['_contract'],b['_contract']
                if ca.threshold == cb.threshold:
                    continue
                broad_first = ca.threshold < cb.threshold if ca.direction == 'upper' else ca.threshold > cb.threshold
                broad,narrow = (a,b) if broad_first else (b,a)
                lo,hi = max(a['_start'],b['_start']),min(a['_end'],b['_end'])
                if lo >= hi:
                    continue
                pid = f"{broad['market_id']}_{narrow['market_id']}"
                pairs.append(dict(pair_id=pid,event_id=event_id,event_slug=slug,kind=ca.kind,template=ca.template,signature=signature,
                    broad_market_id=broad['market_id'],narrow_market_id=narrow['market_id'],
                    broad_yes_token=broad['yes_token'],narrow_no_token=narrow['no_token'],window_start=lo,window_end=hi))
                packages.append(dict(package_id=pid,event_id=event_id,event_slug=slug,stratum='ladder',
                    market_ids=[broad['market_id'],narrow['market_id']],token_ids=[broad['yes_token'],narrow['no_token']],window_start=lo,window_end=hi))
                for row in (a,b):
                    row['_audit'].update(status='accepted',reason='validated_monotone_pair')
                summary['packages'] += 1
        if summary['packages']:
            summary.update(classification='ladder',status='accepted',reason='supported_ladder_pairs')
        else:
            summary.update(reason='no_eligible_supported_pairs')
    audit_frame = pd.DataFrame(audits,columns=['market_id','event_id','event_slug','status','reason','template','signature',
        'threshold','direction','description_sha256','window_start','window_end'])
    design = dict(detector_version=VERSION,window_start=str(start),window_end=str(end),metadata_mode=metadata_mode,
        membership='Within supplied parent events; no cross-event semantic matching.',
        timing='Only declared listing/metadata availability and grammar dates; no realised closing times.',
        negrisk='Requires explicit exhaustive-set verification, exact member inventory and non-augmented contract identity.',
        inputs_sha256=fingerprint(json.dumps([{k:v for k,v in r.items() if not k.startswith('_')} for r in rows],default=str,sort_keys=True),
            json.dumps(event_rows,default=str,sort_keys=True)),
        source_sha256=sha256(__file__),templates_sha256=sha256(Path(__file__).with_name('contract_templates.py')),
        market_count=len(rows),event_count=len(event_output),pair_count=len(pairs),package_count=len(packages),
        market_reasons={str(k):int(v) for k,v in audit_frame.reason.value_counts().items()},
        limitation='Frozen mode does not establish historical metadata availability. Detection establishes relationships, not profitability or book coverage.')
    return DetectionResult(pd.DataFrame(event_output,columns=['event_id','event_slug','classification','status','reason','markets','packages']),
        audit_frame,pd.DataFrame(pairs,columns=PAIR_COLUMNS),pd.DataFrame(packages,columns=PACKAGE_COLUMNS),design)


def detection_plans(result, path_for_token, *, require_files=True):
    """Adapt accepted baskets to the existing simulator plan shape.

    Missing books exclude a complete negRisk basket or the affected ladder pair;
    never drop a leg to make an incomplete basket appear tradable.
    """
    plans, excluded = [], []
    for (event_id,slug,stratum), group in result.packages.groupby(['event_id','event_slug','stratum'],sort=True):
        paths, packages = {}, []
        for row in group.itertuples():
            candidate = {str(token):Path(path_for_token(str(token))) for token in row.token_ids}
            if require_files and not all(p.is_file() for p in candidate.values()):
                excluded.append(dict(event_slug=slug,package_id=row.package_id,reason='missing_member_history'))
                continue
            packages.append((list(map(str,row.token_ids)),row.window_start,row.window_end))
            paths.update(candidate)
        if packages:
            plans.append(dict(event_id=event_id,event_slug=slug,stratum=stratum,paths=paths,packages=packages))
    return plans,excluded


def read_export(path, normalizer=None):
    path = Path(path)
    if path.suffix == '.parquet':
        frame = pd.read_parquet(path)
        return pd.DataFrame(normalizer(r) for r in frame.to_dict('records')) if normalizer else frame
    if path.suffix == '.jsonl':
        with path.open(encoding='utf-8') as stream:
            return pd.DataFrame((normalizer(json.loads(line)) if normalizer else json.loads(line)) for line in stream if line.strip())
    if path.suffix == '.json':
        rows = json.loads(path.read_text(encoding='utf-8'))
        return pd.DataFrame(normalizer(r) for r in rows) if normalizer else pd.DataFrame(rows)
    raise ValueError('Use a Parquet, JSON array or JSONL export')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--markets',required=True)
    parser.add_argument('--events')
    parser.add_argument('--start',required=True)
    parser.add_argument('--end',required=True)
    parser.add_argument('--metadata-mode',choices=['point_in_time','frozen'],default='point_in_time')
    parser.add_argument('--output',required=True)
    args = parser.parse_args()
    result = detect_universe(read_export(args.markets,normalise_market),read_export(args.events,normalise_event) if args.events else None,
        window_start=args.start,window_end=args.end,metadata_mode=args.metadata_mode)
    result.write(args.output)
    print(json.dumps(result.design,indent=2))


if __name__ == '__main__':
    main()
