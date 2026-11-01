"""Aggregate the predeclared multi-day pilot without model selection or P&L."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import isolated, write_json
from src.data.pmxt_multiday import STUDY_ROOT
from src.data.pmxt_diagnostics import audit, REASONS


def describe(group):
    return dict(event_hours=len(group), known_hours=int(group.label_occurrence.notna().sum()),
        positive_hours=int((group.label_occurrence==1).sum()),
        negative_hours=int((group.label_occurrence==0).sum()),
        unknown_hours=int(group.label_occurrence.isna().sum()),
        known_fraction=float(group.label_occurrence.notna().mean()),
        full_ask_hours=int((group.full_ask_seconds>=3600-1e-8).sum()),
        full_book_hours=int((group.fully_observed_seconds>=3600-1e-8).sum()),
        full_ask_time_fraction=float(group.full_ask_seconds.sum()/(len(group)*3600)),
        full_book_time_fraction=float(group.fully_observed_seconds.sum()/(len(group)*3600)),
        determined_time_fraction=float(group.occurrence_determined_seconds.sum()/(len(group)*3600)),
        exact_value_hours=int(group.label_peak_value_usd.notna().sum()),
        maximum_hourly_value_lower_usd=float(group.peak_value_lower_usd.max()),
        positive_seconds=float(group.positive_seconds.sum()))


def assess(root=STUDY_ROOT, *, run_audits=True):
    root = isolated(root)
    # Seeding coverage is useful for real multi-day inputs. Synthetic tests can
    # provide aggregate evidence without an entire archive layout.
    if all((root/b['name']/'raw').exists() for b in json.loads((root/'study_design.json').read_text())['blocks']):
        from src.data.pmxt_bootstrap import audit_seeds
        audit_seeds(root)
    design_path = root/'study_design.json'
    design = json.loads(design_path.read_text())
    all_hours, features, arrivals, audits = [], [], [], []
    evidence = {str(design_path.relative_to(PROJECT_ROOT)): sha256(design_path)}
    for block in design['blocks']:
        folder = root/block['name']
        results = PROJECT_ROOT/'results/pmxt_pilot'/folder.name/'source_validation_v3'
        summary_path = results/'summary.json'
        summary = json.loads(summary_path.read_text())
        if pd.Timestamp(summary['source_start'])!=pd.Timestamp(block['start']) or pd.Timestamp(summary['source_end'])!=pd.Timestamp(block['analysis_end']):
            raise ValueError('Completed replay differs from predeclared block')
        for name, digest in summary['derived_sha256'].items():
            if sha256(folder/'source_validation_v3'/name)!=digest:
                raise ValueError('Derived replay evidence changed')
        for name, digest in summary['results_sha256'].items():
            if sha256(results/name)!=digest:
                raise ValueError('Replay results changed')
        evidence[str(summary_path.relative_to(PROJECT_ROOT))] = sha256(summary_path)
        joined = pd.read_csv(results/'event_hours.csv', dtype={'event_id':str})
        joined['hour'] = pd.to_datetime(joined.hour,utc=True)
        joined['block'] = block['name']
        all_hours.append(joined)
        feature = pd.read_parquet(folder/'source_validation_v3/hourly_features.parquet')
        feature['block'] = block['name']
        features.append(feature)
        arrival = pd.read_csv(results/'arrival_checks.csv',dtype={'event_id':str})
        arrival['block'] = block['name']
        arrivals.append(arrival)
        if run_audits:
            audit(folder)
        audit_path = PROJECT_ROOT/'results/pmxt_pilot'/folder.name/'unknown_audit_v1/summary.json'
        audited = json.loads(audit_path.read_text())
        if audited['replay_summary_sha256']!=sha256(summary_path):
            raise ValueError('Unknown-hour audit is stale')
        evidence[str(audit_path.relative_to(PROJECT_ROOT))] = sha256(audit_path)
        audits.append(audited)
    frame = pd.concat(all_hours,ignore_index=True)
    scans = pd.concat(features,ignore_index=True)
    arrival_frame = pd.concat(arrivals,ignore_index=True)
    if frame.duplicated(['event_id','hour']).any():
        raise ValueError('Analysis blocks overlap')
    event_count = len(pd.read_parquet(root/design['blocks'][0]['name']/'events.parquet'))
    if len(frame)!=design['analysis_hours']*event_count:
        raise ValueError('Incomplete planned sample')
    if not ((frame.label_occurrence!=0)|(frame.ruled_out_seconds>=3600-1e-8)).all():
        raise AssertionError('Unsupported negative label')
    if not ((frame.label_occurrence!=1)|(frame.peak_value_lower_usd>0)).all():
        raise AssertionError('Unsupported positive label')
    by_kind = pd.DataFrame([dict(classification=kind,**describe(group)) for kind,group in frame.groupby('classification')])
    by_block = pd.DataFrame([dict(block=name,**describe(group)) for name,group in frame.groupby('block')])
    by_event = pd.DataFrame([dict(event_id=eid,event_slug=group.event_slug.iloc[0],classification=group.classification.iloc[0],**describe(group)) for eid,group in frame.groupby('event_id')])
    scan_counts = scans.groupby(['block','hour']).agg(ask_complete_events=('full_asks','sum'),book_complete_events=('full_books','sum')).reset_index()
    reasons = pd.DataFrame([dict(reason=r,primary_unknown_seconds=sum(a['primary_reason_seconds'][r] for a in audits),
        overlapping_unknown_seconds=sum(a['overlapping_reason_seconds'][r] for a in audits)) for r in REASONS])
    totals = describe(frame)
    targets = design['acceptance_targets']
    gates = dict(ask_time=totals['full_ask_time_fraction']>=targets['full_ask_time_fraction'],
        occurrence_labels=totals['known_fraction']>=targets['certified_occurrence_fraction'],
        typical_scan_count=float(scan_counts.ask_complete_events.median())>=targets['typical_ask_complete_events_per_scan'])
    ledger_path=root/'download_ledger.json'
    ledger = json.loads(ledger_path.read_text())
    evidence[str(ledger_path.relative_to(PROJECT_ROOT))]=sha256(ledger_path)
    for filename in ['object_inventory.json','transport_diagnostics_allowance.json']:
        path=root/filename
        if path.exists():
            evidence[str(path.relative_to(PROJECT_ROOT))]=sha256(path)
    result = dict(**totals,events=event_count,analysis_hours=design['analysis_hours'],
        receipt_archive_hours=sum(len(list((root/b['name']/'raw').glob('*.parquet'))) for b in design['blocks']),
        positive_event_count=int(frame.loc[frame.label_occurrence==1,'event_id'].nunique()),
        candidate_episode_starts=int((arrival_frame.latency_ms==0).sum()) if len(arrival_frame) else 0,
        median_ask_complete_events_per_scan=float(scan_counts.ask_complete_events.median()),
        minimum_ask_complete_events_per_scan=int(scan_counts.ask_complete_events.min()),
        primary_targets=targets,engineering_gates=gates,engineering_targets_pass=all(gates.values()),
        maximum_new_range_bytes=design['maximum_new_range_bytes'],
        charged_new_range_bytes=ledger['charged_range_bytes'],
        retained_raw_cache_bytes=sum(p.stat().st_size for p in root.glob('*/raw/*.parquet')),
        selection_bias='The fixed universe was identifier-selected from May04; it is not a representative market sample. May04 01-04 was previously inspected.',
        source_clock='Hypothetical timely native feed; finite receipt padding; not a PMXT delivery latency guarantee.',
        opportunity_definition='Robust displayed margin above the $1 minimum payout, buy baskets only, min5 and zero fees; historical parameters unverified.',
        value_definition='Peak qualifying single-basket displayed margin. Repeated quotes and hourly peaks are not summed into trading profit.',
        model_schema_ready=False,historical_parameters_verified=False,models_trained=False,
        new_final_test_accessed=False,net_pnl_estimated=False,evidence_sha256=evidence,
        implementation_sha256={str(Path(__file__).relative_to(PROJECT_ROOT)):sha256(Path(__file__)),
            'src/data/pmxt_multiday.py':sha256(PROJECT_ROOT/'src/data/pmxt_multiday.py')})
    output = isolated(PROJECT_ROOT/'results/pmxt_pilot'/root.name,'results')
    output.mkdir(parents=True,exist_ok=True)
    for name,table in [('event_hours',frame),('by_classification',by_kind),('by_block',by_block),
        ('by_event',by_event),('scan_counts',scan_counts),('unknown_reasons',reasons),('arrival_checks',arrival_frame)]:
        table.to_csv(output/f'{name}.csv',index=False)
    result['results_sha256']={p.name:sha256(p) for p in output.iterdir() if p.is_file() and p.name!='summary.json'}
    write_json(output/'summary.json',result)
    print(json.dumps(result,indent=2,default=str))
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=STUDY_ROOT)
    parser.add_argument('--reuse-audits',action='store_true')
    args=parser.parse_args()
    assess(args.root,run_audits=not args.reuse_audits)


if __name__=='__main__':
    main()
