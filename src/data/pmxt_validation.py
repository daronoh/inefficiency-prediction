"""Source-time acceptance tests, isolated from both Dome and the initial pilot.

Reads a frozen pilot universe and hash-checked raw prefix. Extra receipt hours
capture delayed native-time records; they are not future feature observations.
Full-book comparisons can exclude suspect prior intervals, never impute prices.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
import heapq
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import isolated,write_json
from src.data.pmxt_pilot import DEFAULT_ROOT,load_plan
from src.data.pmxt_books import milliseconds
from src.data.pmxt_source import SourceBook,QualityMask,source_batches,fixed_arrival_check
from src.data.pmxt_audit import EventState,conditional_quote


class SourceEventState(EventState):
    """Avoid rebuilding cumulative depth for baskets with no top-price gap."""
    def recompute(self,changed,stamp):
        for package in self.packages:
            tokens=list(map(str,package['token_ids']))
            if not set(tokens)&changed: continue
            members=[self.books[t] for t in tokens]
            known=all(b.usable for b in members)
            clean=known and all(b.bids and b.asks and max(b.bids)<=min(b.asks) for b in members)
            gap=1-sum(min(b.asks)/10000 for b in members) if clean else None
            quote=conditional_quote([b.raw_book(stamp) for b in members]) if clean and gap>0 else None
            self.package_states[package['package_id']]=dict(known=known,clean=bool(clean),gap=gap,quote=quote)
        values=list(self.package_states.values())
        self.known=bool(values) and all(v['known'] for v in values)
        self.known_fraction=sum(v['known'] for v in values)/len(values) if values else 0
        self.clean_fraction=sum(v['clean'] for v in values)/len(values) if values else 0
        self.gap=max((v['gap'] for v in values if v['gap'] is not None),default=None)
        self.last_quote=max((v['quote'] for v in values if v['quote']),key=lambda q:q['quoted_gross_usd'],default=None)


def read_prefix(root,plan):
    frames,inputs = [],[]
    for clock in pd.date_range(plan['warmup_start'],plan['end'],freq='h',inclusive='left'):
        path = root/'raw'/f'{clock:%Y-%m-%dT%H}.parquet'
        if not path.with_suffix('.json').exists(): break
        manifest = json.loads(path.with_suffix('.json').read_text())
        if sha256(path)!=manifest['sha256'] or set(manifest['assets'])!=set(plan['assets']):
            raise ValueError('Selected raw artifact changed')
        f = pd.read_parquet(path)
        if not f.empty and (f.timestamp_received.min()<clock or f.timestamp_received.max()>=clock+pd.Timedelta(hours=1)):
            raise ValueError('Receipt partition mismatch')
        f['archive_hour'] = clock
        frames.append(f)
        inputs.append(dict(path=str(path.relative_to(root)),sha256=manifest['sha256'],
            transfer_bytes=manifest['transfer_bytes'],cache_bytes=path.stat().st_size))
    if not frames: raise ValueError('No cached archive prefix')
    return pd.concat(frames,ignore_index=True),inputs


def validate(root=DEFAULT_ROOT,*,analysis_end='2026-05-04T04:00:00Z',name='source_validation_v2',
             checkpoint_policy='authoritative_snapshot'):
    root,plan = load_plan(root)
    if not name or Path(name).name!=name or name in {'.','..'}:
        raise ValueError('Validation name must be a simple directory name')
    output = isolated(root/name)
    result = isolated(PROJECT_ROOT/'results/pmxt_pilot'/root.name/name,'results')
    output.mkdir(exist_ok=True);result.mkdir(parents=True,exist_ok=True)
    if checkpoint_policy not in ('authoritative_snapshot','strict_equality'):
        raise ValueError('Unknown checkpoint policy')
    strict = checkpoint_policy=='strict_equality'
    implementation_paths=[Path(__file__),PROJECT_ROOT/'src/data/pmxt_source.py',
        PROJECT_ROOT/'src/data/pmxt_audit.py',PROJECT_ROOT/'src/data/pmxt_books.py',
        PROJECT_ROOT/'src/data/pmxt_archive.py',PROJECT_ROOT/'src/data/pmxt_pilot.py',
        PROJECT_ROOT/'src/simulator/sizing.py',PROJECT_ROOT/'src/data/books.py']
    implementation_hashes={p.relative_to(PROJECT_ROOT).as_posix():sha256(p) for p in implementation_paths}
    frame,inputs = read_prefix(root,plan)
    start,end,capture = pd.Timestamp(plan['start']),pd.Timestamp(analysis_end),pd.Timestamp(plan['warmup_start'])
    receipt_end = max(frame.archive_hour)+pd.Timedelta(hours=1)
    if end.tzinfo is None or end!=end.floor('h') or not start<end or end>pd.Timestamp(plan['end']):
        raise ValueError('Nonempty, whole-hour development window required')
    if receipt_end<end+pd.Timedelta(hours=1):
        raise ValueError('Cache at least one extra receipt hour after the source analysis window')
    design_path = output/'design.json'
    design = dict(version=2,clock='source_time',start=start,end=end,capture_start=capture,
        universe_sha256=sha256(root/'plan.json'),
        execution='100ms total source-update-to-order-arrival; uncapped capital/shares',
        metadata_mode=plan['metadata_mode'],historical_parameters_verified=False,
        no_models_trained=True,no_new_final_test_accessed=True,
        checkpoint_policy=checkpoint_policy,
        quality='Full snapshots are state updates. Contradictions/conflicts exclude suspect periods; depth differences alone are diagnostic. Never backfill feature prices.',
        acceptance_targets={'complete_event_time_fraction':.95,'fully_covered_label_fraction':.90,
            'minimum_typical_complete_events_per_scan':10,
            'unresolved_quote_contradictions_allowed':0},
        acceptance_note='Engineering targets, not industry standards; recorded before running this source validation.')
    serialized = json.loads(json.dumps(design,default=str))
    if design_path.exists():
        if json.loads(design_path.read_text())!=serialized:
            raise ValueError('Validation design already frozen differently; choose a new name')
    else: write_json(design_path,design)
    # No source-time state from before capture is accepted as a complete seed.
    work = frame.loc[frame.timestamp>=capture].copy()
    books = {t:SourceBook(milliseconds(capture),strict_checkpoints=strict) for t in plan['assets']}
    groups = 0
    for (stamp,token),rows in source_batches(work):
        books[token].apply_batch(stamp,rows);groups += 1
        if groups%100_000==0: print(f'Source validation: {groups:,} groups checked',flush=True)
    checkpoints = pd.DataFrame([dict(asset_id=t,**r) for t,b in books.items() for r in b.checkpoint_rows])
    checkpoints.to_parquet(output/'checkpoint_comparisons.parquet',index=False)
    quality = pd.DataFrame([dict(asset_id=t,**dict(b.counters)) for t,b in books.items()])
    quality.to_csv(result/'token_quality.csv',index=False)
    masks = {t:QualityMask(b.bad_intervals) for t,b in books.items()}
    write_json(output/'suspect_intervals.json',{t:m.intervals for t,m in masks.items() if m.intervals})
    write_json(result/'checkpoint_examples.json',{t:b.examples for t,b in books.items() if b.examples})
    write_json(result/'contradiction_examples.json',{t:b.contradiction_examples for t,b in books.items() if b.contradiction_examples})
    print(f'Checkpoint pass complete: {len(checkpoints):,} comparisons',flush=True)
    # Second pass evaluates original causal states with explicit quality masks.
    # A future checkpoint may make a label unknown, but cannot change a quote.
    books = {t:SourceBook(milliseconds(capture),strict_checkpoints=strict,record_diagnostics=False) for t in plan['assets']}
    events = pd.read_parquet(root/'events.parquet').to_dict('records')
    packages = pd.read_parquet(root/'packages.parquet').to_dict('records')
    states,by_token = {},defaultdict(set)
    for event in events:
        eid = event['event_id']
        states[eid] = SourceEventState(event,[p for p in packages if p['event_id']==eid],books)
        for token in states[eid].tokens: by_token[token].add(eid)
    cuts = {milliseconds(t) for t in pd.date_range(capture,end,freq='h')}
    for mask in masks.values():
        cuts.update(x for pair in mask.intervals for x in pair if milliseconds(capture)<=x<=milliseconds(end))
    cuts = iter(sorted(cuts));next_cut = next(cuts,None)
    previous = capture
    features,labels,arrivals = [],{},[]
    pending = []
    previous_signals = set()
    sequence = 0

    def recompute(tokens,stamp):
        for eid in {eid for token in tokens for eid in by_token[token]}:
            states[eid].recompute(tokens,stamp)

    def integrate(target):
        nonlocal previous
        if target<=previous: return
        hour = previous.floor('h')
        if hour>=start and hour<end:
            duration = (target-previous).total_seconds()
            for eid,state in states.items():
                row = labels[(eid,hour)]
                row['fully_observed_seconds'] += duration*state.known
                row['any_package_observed_seconds'] += duration*(state.known_fraction>0)
                if state.last_quote:
                    row['conditional_positive_seconds'] += duration
                    if state.last_quote['quoted_gross_usd']>row['conditional_peak_gross_usd']:
                        row.update(conditional_peak_gross_usd=state.last_quote['quoted_gross_usd'],peak_source_at=previous)
        previous = target

    def move(target):
        nonlocal next_cut,previous_signals
        while next_cut is not None and next_cut<=milliseconds(target):
            stamp = pd.Timestamp(next_cut,unit='ms',tz='UTC')
            integrate(stamp)
            changed = set()
            for token,book in books.items():
                blocked = masks[token].contains(next_cut)
                if book.blocked!=blocked:
                    book.blocked=blocked;changed.add(token)
            recompute(changed,stamp)
            affected={eid for token in changed for eid in by_token[token]}
            previous_signals={key for key in previous_signals if key[0] not in affected}
            if stamp==stamp.floor('h') and start<=stamp<end:
                for eid,state in states.items():
                    row = state.features(stamp)
                    row['max_source_age_seconds'] = row.pop('max_receive_age_seconds')
                    features.append(row)
                    labels[(eid,stamp)] = dict(event_id=eid,hour=stamp,fully_observed_seconds=0.,
                        any_package_observed_seconds=0.,conditional_positive_seconds=0.,
                        conditional_peak_gross_usd=0.,peak_source_at=None)
            next_cut = next(cuts,None)
        integrate(target)

    def check_arrival(item):
        at,_,signal,delay = item
        raw = [books[t].raw_book(at) for t in signal['token_ids']]
        arrivals.append(dict(event_id=signal['event_id'],package_id=signal['package_id'],
            decision_at=signal['decision_at'],arrival_at=at,latency_ms=delay,
            quantity=signal['quantity'],quoted_gross_usd=signal['quoted_gross_usd'],
            **fixed_arrival_check(signal,raw)))

    def process(stamp,batches):
        nonlocal sequence,previous_signals
        while pending and pending[0][0]<stamp:
            item=heapq.heappop(pending);move(item[0]);check_arrival(item)
        move(stamp)
        changed = set()
        for (_,token),rows in batches:
            book=books[token];book.apply_batch(stamp,rows)
            book.blocked=masks[token].contains(milliseconds(stamp));changed.add(token)
        recompute(changed,stamp)
        # Same-time market updates are applied BEFORE checking order arrival.
        while pending and pending[0][0]==stamp: check_arrival(heapq.heappop(pending))
        if start<=stamp<end:
            # Only events whose books changed can start/end an episode here.
            affected = {eid for token in changed for eid in by_token[token]}
            active={key for key in previous_signals if key[0] not in affected}
            for eid in affected:
                state=states[eid]
                row=labels[(eid,stamp.floor('h'))]
                if state.last_quote and state.last_quote['quoted_gross_usd']>row['conditional_peak_gross_usd']:
                    row.update(conditional_peak_gross_usd=state.last_quote['quoted_gross_usd'],peak_source_at=stamp)
                for package in state.packages:
                    pid=package['package_id'];q=state.package_states[pid]['quote']
                    if not q: continue
                    key=eid,pid;active.add(key)
                    if key not in previous_signals:
                        signal=dict(q,event_id=eid,package_id=pid,token_ids=list(map(str,package['token_ids'])),decision_at=stamp)
                        for delay in [0,50,100,250,500]:
                            sequence+=1;at=stamp+pd.Timedelta(milliseconds=delay)
                            if at<end: heapq.heappush(pending,(at,sequence,signal,delay))
            previous_signals=active
            while pending and pending[0][0]==stamp: check_arrival(heapq.heappop(pending))

    grouped,stamp=[],None
    for key,rows in source_batches(work.loc[work.timestamp<end]):
        if stamp is not None and key[0]!=stamp:
            process(stamp,grouped);grouped=[]
        stamp=key[0];grouped.append((key,rows))
    if grouped: process(stamp,grouped)
    while pending:
        item=heapq.heappop(pending);move(item[0]);check_arrival(item)
    move(end)
    features=pd.DataFrame(features);labels=pd.DataFrame(labels.values())
    labels['label_conditional_gross']=pd.array([1 if r.conditional_peak_gross_usd>0 else 0 if r.fully_observed_seconds>=3600-1e-8 else None
        for r in labels.itertuples()],dtype='Int64')
    labels['fee_net_executable_status']='unknown_historical_parameters'
    features.to_parquet(output/'hourly_features.parquet',index=False)
    labels.to_parquet(output/'hourly_quote_labels.parquet',index=False)
    features.merge(labels,on=['event_id','hour'],validate='one_to_one').to_csv(result/'event_hours.csv',index=False)
    arrival_frame=pd.DataFrame(arrivals,columns=['event_id','package_id','decision_at','arrival_at','latency_ms','quantity',
        'quoted_gross_usd','status','supported_legs','conditional_gross_usd'])
    arrival_frame.to_csv(result/'arrival_checks.csv',index=False)
    concurrency=features.assign(complete=features.usable_token_fraction==1).groupby('hour').complete.sum()
    summary=dict(source_start=start,source_end=end,receipt_end=receipt_end,receipt_context_hours=(receipt_end-end).total_seconds()/3600,
        checkpoint_policy=checkpoint_policy,
        events=len(events),tokens=len(books),source_messages=int(len(work)),archive_hours=len(inputs),
        checkpoint_comparisons=len(checkpoints),
        checkpoint_exact_fraction=float(checkpoints.exact.mean()) if len(checkpoints) else None,
        checkpoint_top_price_fraction=float(checkpoints.top_match.mean()) if len(checkpoints) else None,
        checkpoint_best_ask_size_fraction=float(checkpoints.best_ask_size_match.mean()) if len(checkpoints) else None,
        checkpoint_ask_exact_fraction=float(checkpoints.ask_exact.mean()) if len(checkpoints) else None,
        event_hours=len(labels),fully_observed_event_hours=int((labels.fully_observed_seconds>=3600-1e-8).sum()),
        observed_event_time_fraction=float(labels.fully_observed_seconds.sum()/(3600*len(labels))),
        complete_events_per_scan=concurrency.to_dict(),
        conditional_positive_event_hours=int((labels.conditional_peak_gross_usd>0).sum()),
        maximum_conditional_one_basket_gross_usd=float(labels.conditional_peak_gross_usd.max()),
        arrival_checks=len(arrival_frame),arrival_status_counts=arrival_frame.groupby(['latency_ms','status']).size().to_dict(),
        late_records_for_analysis=int(((frame.timestamp<end)&(frame.timestamp>=start)&(frame.timestamp_received>=end)).sum()),
        recorded_transfer_bytes=sum(i['transfer_bytes'] for i in inputs),raw_cache_bytes=sum(i['cache_bytes'] for i in inputs),
        acceptance_targets=design['acceptance_targets'],historical_parameters_verified=False,model_schema_ready=False,
        models_trained=False,net_pnl_estimated=False,
        limitations=['Native full snapshot clocks assumed to identify state version; disagreement may be stream gaps or timestamp semantics.',
            'Finite receipt buffer does not prove all delayed messages were captured.',
            'Depth disagreement alone is not proof of missing data: full-book messages can supply new state.',
            'Quality masks exclude contradictions; never backfill feature prices.',
            'Quoted positives and arrival support use zero fees, minimum 5 and initially .0001 tick assumptions.',
            'Arrival checks sample episode starts; no reservations, monitoring policy or partial unwind P&L.'],
        inputs=inputs,design_sha256=sha256(design_path),
        derived_sha256={n:sha256(output/n) for n in ['hourly_features.parquet','hourly_quote_labels.parquet',
            'checkpoint_comparisons.parquet','suspect_intervals.json']},
        results_sha256={n:sha256(result/n) for n in ['event_hours.csv','token_quality.csv','arrival_checks.csv',
            'checkpoint_examples.json','contradiction_examples.json']},
        implementation_sha256=implementation_hashes)
    if any(sha256(PROJECT_ROOT/p)!=h for p,h in implementation_hashes.items()):
        raise ValueError('Validation implementation changed during execution; rerun before publishing artifacts')
    # JSON keys must be strings, not timestamps or latency/status tuples.
    summary['complete_events_per_scan']={str(k):int(v) for k,v in summary['complete_events_per_scan'].items()}
    summary['arrival_status_counts']={f'{k[0]}ms:{k[1]}':int(v) for k,v in summary['arrival_status_counts'].items()}
    write_json(result/'summary.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ['inputs','implementation_sha256']},indent=2,default=str))
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=DEFAULT_ROOT)
    parser.add_argument('--analysis-end',default='2026-05-04T04:00:00Z')
    parser.add_argument('--name',default='source_validation_v2')
    parser.add_argument('--checkpoint-policy',choices=['authoritative_snapshot','strict_equality'],default='authoritative_snapshot')
    args=parser.parse_args();validate(args.root,analysis_end=args.analysis_end,name=args.name,checkpoint_policy=args.checkpoint_policy)
