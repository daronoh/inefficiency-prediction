"""Final-period recording audit using timestamps, never prices or outcomes.

This is a separate access path: development loaders and book readers retain
their existing split guards. Timestamp extents do not certify usable labels.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.common import sha256
from src.config import SPEC
from src.data.universe import build_plan
from src.paths import (PROJECT_ROOT, PROCESSED_DIR, EVENT_UNIVERSE,
                       MARKETS_PARQUET, TOKENS_PARQUET, data_relative)


def recording_extent(path):
    """Read only parquet footer statistics or the single timestamp column."""
    file = pq.ParquetFile(path)
    index = file.schema_arrow.get_field_index('snapshot_time')
    if index < 0:
        raise ValueError('Missing source timestamp column')
    stats = [file.metadata.row_group(i).column(index).statistics
             for i in range(file.num_row_groups)]
    if stats and all(s is not None and s.has_min_max and s.null_count == 0 for s in stats):
        first = min(pd.Timestamp(s.min) for s in stats)
        last = max(pd.Timestamp(s.max) for s in stats)
        method = 'timestamp_footer'
    else:
        values = pd.to_datetime(pd.read_parquet(path, columns=['snapshot_time']).snapshot_time, utc=True)
        if values.isna().any():
            raise ValueError('Missing source timestamp')
        first, last = values.min(), values.max()
        method = 'timestamp_column'
    return dict(first=first, last=last, raw_rows=file.metadata.num_rows, extent_method=method)


def timestamp_history(path):
    """The audit's only row-data projection; book columns are never requested."""
    values = pd.to_datetime(pd.read_parquet(path, columns=['snapshot_time']).snapshot_time, utc=True)
    if values.isna().any():
        raise ValueError('Missing source timestamp')
    return pd.DatetimeIndex(values).sort_values().unique()


def endpoint(extents, start, frequency):
    """Latest whole UTC boundary supported by any frozen token recording.

    No label counts, price states or model results enter the stopping rule.
    The fractional final hour is excluded; incomplete earlier hours remain.
    """
    values = pd.to_datetime(extents, utc=True).dropna()
    stop = values.max().floor(frequency) if len(values) else pd.NaT
    if pd.isna(stop) or stop <= start:
        raise ValueError('No whole final-period monitoring window is recorded')
    return stop


def event_availability(plan, extents, windows, horizon, development_events):
    """Separate as-of seen membership from retrospective recording evidence.

    The caller supplies independently justified package windows. Extending a
    loader cutoff does not extend a contract's audited validity.
    """
    rows = []
    for when in windows:
        packages = [(ids, a, b) for ids, a, b in plan['packages'] if a <= when < b]
        if not packages:
            continue
        tokens = sorted({t for ids, _, _ in packages for t in ids})
        seen = all(pd.notna(extents[t]['first']) and extents[t]['first'] <= when for t in tokens)
        # Match the historical feature grid's event-level observation extent.
        # This is an offline mask, not an input or live deactivation signal.
        event_last = max((extents[t]['last'] for t in tokens), default=pd.NaT)
        in_extent = pd.notna(event_last) and when <= event_last
        bracketed = all(extents[t]['first'] <= when and
                        min(when+horizon, b) <= extents[t]['last']
                        for ids, _, b in packages for t in ids)
        rows.append(dict(event_slug=plan['event_slug'], stratum=plan['stratum'], time=when,
                         development_event=plan['event_slug'] in development_events,
                         active_packages=len(packages), active_tokens=len(tokens),
                         all_members_seen=seen, in_event_recording_extent=in_extent,
                         recording_eligible=seen and in_extent,
                         all_member_extent_bracketed=seen and in_extent and bracketed,
                         members_ended_before_scan=sum(extents[t]['last'] < when for t in tokens)))
    return pd.DataFrame(rows)


def pool_summary(frame, windows):
    pieces = []
    for scope in ('frozen_supported_universe', 'development_events_only'):
        part = frame if scope == 'frozen_supported_universe' else frame.loc[frame.development_event]
        eligible = part.loc[part.recording_eligible]
        counts = eligible.groupby('time').agg(eligible_events=('event_slug', 'size'),
            extent_bracketed_events=('all_member_extent_bracketed', 'sum'),
            required_token_snapshots=('active_tokens', 'sum'))
        counts = counts.reindex(windows, fill_value=0).rename_axis('time').reset_index()
        counts['scope'] = scope
        for n in (3, 5):
            counts[f'selected_at_{n}'] = counts.eligible_events.clip(upper=n)
            counts[f'selective_at_{n}'] = counts.eligible_events.gt(n)
            counts[f'monitor_all_at_{n}'] = counts.eligible_events.gt(0) & counts.eligible_events.le(n)
        pieces.append(counts)
    return pd.concat(pieces, ignore_index=True)


def audit(output):
    output = Path(output)
    if output.exists():
        raise FileExistsError('Use a fresh audit directory')
    plans, excluded = build_plan('all')
    # Existing development artifact: keys only, no labels, values or predictors.
    parent = PROCESSED_DIR/'opportunity_value/examples.parquet'
    development_events = set(pd.read_parquet(parent, columns=['event_slug']).event_slug)
    cutoff = pd.Timestamp(SPEC.lockbox_start)
    extents, paths = {}, {}
    for plan in plans:
        for token, path in plan['paths'].items():
            if token not in extents:
                extents[token] = recording_extent(path)
                paths[token] = Path(path)
    stop = endpoint(pd.Series([e['last'] for e in extents.values()]), cutoff, SPEC.frequency)
    windows = pd.date_range(cutoff, stop, freq=SPEC.frequency, inclusive='left')
    output.mkdir(parents=True)
    # No audited ladder relationship extends beyond this cutoff. Do not invent
    # final-period contracts by changing a development-clipped end date.
    if any(p['stratum'] == 'ladder' and any(b > cutoff for _, _, b in p['packages']) for p in plans):
        raise ValueError('New final-period ladder validity requires an explicit audit revision')
    expanded, inventory, members = [], [], []
    for plan in plans:
        for token in plan['paths']:
            members.append(dict(event_slug=plan['event_slug'], stratum=plan['stratum'], token_id=token))
        later = dict(plan)
        if plan['stratum'] == 'negrisk':
            later['packages'] = [(ids, a, stop) for ids, a, _ in plan['packages']]
        expanded.append(event_availability(later, extents, windows, SPEC.horizon, development_events))
    availability = pd.concat(expanded, ignore_index=True)
    budget = pool_summary(availability, windows)
    for i, (token, item) in enumerate(extents.items(), 1):
        path = paths[token]
        row = dict(token_id=token, data_path=data_relative(path), file_bytes=path.stat().st_size, **item)
        row['has_final_observations'] = pd.notna(item['last']) and item['last'] >= cutoff
        if row['has_final_observations']:
            times = timestamp_history(path)
            later = times[times >= cutoff]
            after = times[times > cutoff]
            seed = times[times <= cutoff][-1:]
            evidence = seed.append(after)
            gaps = np.diff(evidence.as_unit('ns').asi8)/1e9
            row.update(final_updates=len(later), first_final=str(later[0]), last_final=str(later[-1]),
                       max_final_interupdate_gap_s=float(gaps.max()) if len(gaps) else None,
                       gaps_over_one_hour=int((gaps > SPEC.horizon.total_seconds()).sum()),
                       timestamp_sha256=hashlib.sha256(times.as_unit('ns').asi8.tobytes()).hexdigest())
            print(f'Timestamp audit {i}/{len(extents)}: {len(later)} final observations', flush=True)
        else:
            row['final_updates'] = 0
        inventory.append(row)
    tokens = pd.DataFrame(inventory)
    membership = pd.DataFrame(members)
    events = []
    for plan in plans:
        slug = plan['event_slug']
        token = tokens.loc[tokens.token_id.isin(plan['paths'])]
        part = availability.loc[availability.event_slug.eq(slug)]
        candidates = part.loc[part.recording_eligible] if len(part) else part
        events.append(dict(event_slug=slug, stratum=plan['stratum'],
            development_event=slug in development_events, tokens=len(token),
            tokens_with_final_observations=int(token.has_final_observations.sum()),
            first_member_start=token['first'].min(), last_member_start=token['first'].max(),
            first_member_end=token['last'].min(), last_member_end=token['last'].max(),
            final_eligible_windows=len(candidates),
            all_member_extent_bracketed_windows=int(candidates.all_member_extent_bracketed.sum()) if len(candidates) else 0,
            first_final_scan=candidates.time.min() if len(candidates) else pd.NaT,
            last_final_scan=candidates.time.max() if len(candidates) else pd.NaT,
            status=('no_audited_final_contract_window' if plan['stratum'] == 'ladder'
                    else 'no_post_cutoff_recording' if not token.has_final_observations.any()
                    else 'members_missing_at_all_scans' if not len(candidates)
                    else 'ranking_recordings_available')))
    events = pd.DataFrame(events)
    summary = dict(final_start=str(cutoff), final_end_exclusive=str(stop),
        endpoint_rule='floor(max last source timestamp among frozen supported token files, cadence); exclude incomplete terminal hour',
        scheduled_hours=len(windows), supported_events=len(plans), development_events=len(development_events),
        supported_negrisk=sum(p['stratum'] == 'negrisk' for p in plans),
        supported_ladder=sum(p['stratum'] == 'ladder' for p in plans),
        token_files=len(tokens), tokens_with_final_observations=int(tokens.has_final_observations.sum()),
        events_with_final_observations=int(events.tokens_with_final_observations.gt(0).sum()),
        events_with_final_ranking_rows=int(events.final_eligible_windows.gt(0).sum()),
        final_eligible_event_windows=int(events.final_eligible_windows.sum()),
        extent_bracketed_event_windows=int(events.all_member_extent_bracketed_windows.sum()),
        scopes={})
    for scope, part in budget.groupby('scope'):
        summary['scopes'][scope] = dict(hours_with_events=int(part.eligible_events.gt(0).sum()),
            min_events=int(part.eligible_events.min()), max_events=int(part.eligible_events.max()),
            median_events=float(part.eligible_events.median()),
            selective_hours_n3=int(part.selective_at_3.sum()), selective_hours_n5=int(part.selective_at_5.sum()),
            monitor_all_hours_n3=int(part.monitor_all_at_3.sum()), monitor_all_hours_n5=int(part.monitor_all_at_5.sum()),
            hours_with_more_than_three_extent_bracketed_events=int(part.extent_bracketed_events.gt(3).sum()))
    frozen = PROJECT_ROOT/'results/final_preparation'
    design = dict(version='final_timestamp_audit_v1', window=asdict(SPEC),
        purpose='Recording coverage and protocol preparation only',
        row_columns_read=['snapshot_time'], development_columns_read=['event_slug'],
        book_columns_read=[], opportunity_labels_read=False, model_predictions_computed=False,
        execution_run=False, final_outcomes_opened=False, final_timestamp_access=True,
        universe='Original frozen supported 46 negRisk + 5 audited ladder events; no new discovery',
        evidence='First/last timestamps and update-gap diagnostics only. Full-member extents are not usable-label guarantees; observed blockers may make some incomplete packages decidable.',
        ranking_mask='All required tokens seen by scan, active frozen package and scan within event-level recording extent. Recording extent is an offline observation mask, not a predictor or live deactivation signal.',
        no_price_lookahead=True,
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in ['src/data/final_coverage.py','src/data/universe.py','src/data/rules.py','src/config.py']},
        metadata_sha256={p.name:sha256(p) for p in [EVENT_UNIVERSE,MARKETS_PARQUET,TOKENS_PARQUET]},
        final_model_completion_sha256=sha256(frozen/'complete.json'),
        final_bundle_sha256=sha256(frozen/'final_refit_bundle.json'),
        development_keys_sha256=sha256(parent),
        input_identity='Timestamp extent inventory and timestamp hashes for files with final observations; full price-file hashes must be verified separately before economic evaluation.')
    for name, frame in [('token_inventory',tokens),('event_inventory',events),('membership',membership),
                        ('hourly_pool',budget),('excluded_events',pd.DataFrame(excluded))]:
        frame.to_csv(output/f'{name}.csv', index=False)
    availability.to_parquet(output/'event_windows.parquet', index=False)
    for name, value in [('summary',summary),('design',design)]:
        (output/f'{name}.json').write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')
    files = {p.name:sha256(p) for p in output.iterdir() if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(files_sha256=files,
        final_timestamp_access=True, final_outcomes_opened=False, opportunity_labels_built=False), indent=2)+'\n', encoding='utf-8')
    return summary


def verify(output):
    output = Path(output)
    complete = json.loads((output/'complete.json').read_text())
    design = json.loads((output/'design.json').read_text())
    if complete['final_outcomes_opened'] or complete['opportunity_labels_built'] or design['book_columns_read']:
        raise ValueError('Expected a timestamp-only audit')
    for name, digest in complete['files_sha256'].items():
        if sha256(output/name) != digest:
            raise ValueError(f'Changed audit artifact: {name}')
    for path, digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/path) != digest:
            raise ValueError(f'Changed audit source: {path}')
    for path in [EVENT_UNIVERSE,MARKETS_PARQUET,TOKENS_PARQUET]:
        if sha256(path) != design['metadata_sha256'][path.name]:
            raise ValueError('Frozen metadata changed')
    if sha256(PROJECT_ROOT/'results/final_preparation/complete.json') != design['final_model_completion_sha256']:
        raise ValueError('Prepared models changed')
    return json.loads((output/'summary.json').read_text())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.output), indent=2))
