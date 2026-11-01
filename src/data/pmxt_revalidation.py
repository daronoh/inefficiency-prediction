"""Rerun the frozen PMXT pilot with bounded uncertainty and label certification.

Version 3 is separate from v1/v2. No new downloads, model fits, final test access
or Dome mutations. Values remain conditional zero-fee displayed quote margins.
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
from src.data.pmxt_source import source_batches,QualityMask
from src.data.pmxt_validation import read_prefix
from src.data.pmxt_uncertainty import BoundedBook,basket_bounds,arrival_bounds,SIDES


class BoundedEvent:
    def __init__(self,event,packages,books):
        self.event,self.packages,self.books=event,packages,books
        self.tokens={str(t) for p in packages for t in p['token_ids']}
        self.bounds={}
        self.recompute(self.tokens)

    def recompute(self,changed):
        for p in self.packages:
            if set(p['token_ids'])&changed:
                self.bounds[p['package_id']]=basket_bounds([self.books[str(t)] for t in p['token_ids']])
        values=list(self.bounds.values())
        self.status='positive' if any(v['status']=='positive' for v in values) else 'blocked' if all(v['status']=='blocked' for v in values) else 'unknown'
        self.lower=max((v['lower'] for v in values),default=0.)
        self.upper=None if any(v['upper'] is None for v in values) else max((v['upper'] for v in values),default=0.)
        self.full_asks=all(self.books[t].exact('SELL') for t in self.tokens)
        self.full_books=self.full_asks and all(self.books[t].exact('BUY') for t in self.tokens)

    def features(self,stamp):
        n=len(self.tokens);members=[self.books[t] for t in self.tokens]
        ages=[(milliseconds(stamp)-b.last_source)/1000 for b in members if b.last_source is not None]
        return dict(event_id=self.event['event_id'],event_slug=self.event['event_slug'],
            classification=self.event['classification'],hour=stamp,token_count=n,package_count=len(self.packages),
            ask_available_fraction=sum(b.available('SELL') for b in members)/n,
            bid_available_fraction=sum(b.available('BUY') for b in members)/n,
            ask_exact_fraction=sum(b.exact('SELL') for b in members)/n,
            bid_exact_fraction=sum(b.exact('BUY') for b in members)/n,
            full_books=self.full_books,full_asks=self.full_asks,
            snapshot_occurrence_status=self.status,snapshot_value_lower_usd=self.lower,
            snapshot_value_upper_usd=self.upper,max_source_age_seconds=max(ages,default=None))


def finalize_labels(rows):
    result=[]
    for original in rows:
        row=dict(original)
        lower=row['peak_value_lower_usd'];upper=row['peak_value_upper_usd']
        label=1 if lower>1e-10 else 0 if row['ruled_out_seconds']>=3600-1e-8 else None
        exact=upper is not None and abs(upper-lower)<=1e-8
        row.update(label_occurrence=label,label_peak_value_usd=lower if exact else None,
            value_target_status='exact' if exact else 'lower_bound' if lower>0 else 'unknown_interval',
            fee_net_executable_status='unknown_historical_parameters')
        result.append(row)
    frame=pd.DataFrame(result)
    frame['label_occurrence']=pd.array(frame.label_occurrence,dtype='Int64')
    return frame


def causal_features(states,books,stamp):
    """Features never include retrospective quality findings from later records.

    The label view is restored afterwards. A causal ambiguity already observed
    at/before the scan remains present; only hindsight masks are removed.
    """
    saved={t:dict(b.past_blocked) for t,b in books.items()}
    try:
        for b in books.values():b.past_blocked={s:False for s in SIDES}
        for state in states.values():state.recompute(state.tokens)
        return [state.features(stamp) for state in states.values()]
    finally:
        for t,b in books.items():b.past_blocked=saved[t]
        for state in states.values():state.recompute(state.tokens)


def rerun(root=DEFAULT_ROOT,*,analysis_end='2026-05-04T04:00:00Z',name='source_validation_v3'):
    root,plan=load_plan(root)
    if not name or Path(name).name!=name or name in {'.','..'}:raise ValueError('Simple isolated run name required')
    output=isolated(root/name);output.mkdir(exist_ok=True)
    results=isolated(PROJECT_ROOT/'results/pmxt_pilot'/root.name/name,'results');results.mkdir(parents=True,exist_ok=True)
    paths=[Path(__file__),PROJECT_ROOT/'src/data/pmxt_uncertainty.py',PROJECT_ROOT/'src/data/pmxt_validation.py',
        PROJECT_ROOT/'src/data/pmxt_source.py',PROJECT_ROOT/'src/data/pmxt_books.py',PROJECT_ROOT/'src/data/pmxt_archive.py',
        PROJECT_ROOT/'src/data/pmxt_pilot.py',PROJECT_ROOT/'src/data/books.py',PROJECT_ROOT/'src/simulator/sizing.py']
    code={p.relative_to(PROJECT_ROOT).as_posix():sha256(p) for p in paths}
    frame,inputs=read_prefix(root,plan)
    start,end,capture=pd.Timestamp(plan['start']),pd.Timestamp(analysis_end),pd.Timestamp(plan['warmup_start'])
    # A valid cached hour can contain zero messages for the selected tokens.
    receipt_end=pd.Timestamp(Path(inputs[-1]['path']).stem,tz='UTC')+pd.Timedelta(hours=1)
    if end.tzinfo is None or end!=end.floor('h') or not start<end<=pd.Timestamp(plan['end']):raise ValueError('Whole-hour development window required')
    if receipt_end<end+pd.Timedelta(hours=1):raise ValueError('Need a receipt buffer after analysis end')
    design=dict(version=3,clock='source_time',source_start=start,source_end=end,capture_start=capture,
        universe_sha256=sha256(root/'plan.json'),
        reconstruction='Atomic per-token/source-ms group; compatible closing snapshots; level alternatives; per-side drift recovery; no synthetic liquidity.',
        exclusions='Local level ambiguity begins at its timestamp. Unknown-onset best-price drift looks back to that SIDE snapshot anchor.',
        snapshot_close_assumption='A single same-source full snapshot closes the batch only where its level values match observed alternatives and its best prices are compatible.',
        labels='Witnessed robust positive; negative only if every basket ruled out for full hour; peak bounds; exact regression target only when bounds coincide.',
        trading='100ms total source-update-to-order-arrival primary; 0/50/250/500ms diagnostics; uncapped shares/capital; no PnL.',
        parameters='Zero-fee, min5, initial .0001 tick diagnostic; historical parameters unverified.',
        metadata_mode=plan['metadata_mode'],models_trained=False,new_final_test_accessed=False)
    serialized=json.loads(json.dumps(design,default=str));design_path=output/'design.json'
    if design_path.exists():
        if json.loads(design_path.read_text())!=serialized:raise ValueError('Frozen v3 design differs; choose a new name')
    else:write_json(design_path,design)
    work=frame.loc[frame.timestamp>=capture].copy()
    books={t:BoundedBook(milliseconds(capture)) for t in plan['assets']}
    groups=0
    for (stamp,token),rows in source_batches(work):
        books[token].apply_batch(stamp,rows);groups+=1
        if groups%200_000==0:print(f'Bounded validation: {groups:,} source groups',flush=True)
    pd.DataFrame([dict(asset_id=t,**dict(b.counters)) for t,b in books.items()]).to_csv(results/'token_quality.csv',index=False)
    write_json(results/'issue_examples.json',{t:b.issues for t,b in books.items() if b.issues})
    masks={(t,s):QualityMask([(a,z) for side,a,z in b.drift_intervals if side==s]) for t,b in books.items() for s in SIDES}
    write_json(output/'side_drift_intervals.json',{f'{t}:{s}':m.intervals for (t,s),m in masks.items() if m.intervals})
    counters=defaultdict(int)
    for b in books.values():
        for key,value in b.counters.items():counters[key]+=int(value)
    print('Issue classification finished; replaying event bounds.',flush=True)
    books={t:BoundedBook(milliseconds(capture)) for t in plan['assets']}
    events=pd.read_parquet(root/'events.parquet').to_dict('records')
    packages=pd.read_parquet(root/'packages.parquet').to_dict('records')
    states={};by_token=defaultdict(set)
    for event in events:
        eid=event['event_id'];states[eid]=BoundedEvent(event,[p for p in packages if p['event_id']==eid],books)
        for token in states[eid].tokens:by_token[token].add(eid)
    cuts={milliseconds(t) for t in pd.date_range(capture,end,freq='h')}
    for mask in masks.values():cuts.update(x for pair in mask.intervals for x in pair if milliseconds(capture)<=x<=milliseconds(end))
    cuts=iter(sorted(cuts));next_cut=next(cuts,None)
    previous=capture;features=[];labels={};arrivals=[];pending=[];sequence=0;active=set()

    def recompute(tokens):
        affected={eid for token in tokens for eid in by_token[token]}
        for eid in affected:states[eid].recompute(tokens)
        return affected

    def peak(row,state,stamp):
        if state.lower>row['peak_value_lower_usd']:
            row['peak_value_lower_usd']=state.lower;row['peak_lower_source_at']=stamp
        if state.upper is None:row['peak_value_upper_usd']=None
        elif row['peak_value_upper_usd'] is not None:row['peak_value_upper_usd']=max(row['peak_value_upper_usd'],state.upper)

    def integrate(target):
        nonlocal previous
        if target<=previous:return
        hour=previous.floor('h')
        if start<=hour<end:
            seconds=(target-previous).total_seconds()
            for eid,state in states.items():
                row=labels[eid,hour]
                row['fully_observed_seconds']+=seconds*state.full_books
                row['full_ask_seconds']+=seconds*state.full_asks
                row['occurrence_determined_seconds']+=seconds*(state.status!='unknown')
                row['ruled_out_seconds']+=seconds*(state.status=='blocked')
                row['positive_seconds']+=seconds*(state.status=='positive')
                row['blocked_with_incomplete_books_seconds']+=seconds*(state.status=='blocked' and not state.full_books)
                peak(row,state,previous)
        previous=target

    def move(target):
        nonlocal next_cut,active
        while next_cut is not None and next_cut<=milliseconds(target):
            stamp=pd.Timestamp(next_cut,unit='ms',tz='UTC');integrate(stamp)
            changed=set()
            for (token,side),mask in masks.items():
                blocked=mask.contains(next_cut)
                if books[token].past_blocked[side]!=blocked:
                    books[token].past_blocked[side]=blocked;changed.add(token)
            affected=recompute(changed);active={key for key in active if key[0] not in affected}
            if stamp==stamp.floor('h') and start<=stamp<end:
                features.extend(causal_features(states,books,stamp))
                for eid,state in states.items():
                    labels[eid,stamp]=dict(event_id=eid,hour=stamp,fully_observed_seconds=0.,full_ask_seconds=0.,
                        occurrence_determined_seconds=0.,ruled_out_seconds=0.,positive_seconds=0.,
                        blocked_with_incomplete_books_seconds=0.,peak_value_lower_usd=0.,peak_value_upper_usd=0.,peak_lower_source_at=None)
            next_cut=next(cuts,None)
        integrate(target)

    def check(item):
        at,_,signal,delay=item
        members=[books[t] for t in signal['token_ids']]
        arrivals.append(dict(event_id=signal['event_id'],package_id=signal['package_id'],decision_at=signal['decision_at'],
            arrival_at=at,latency_ms=delay,quantity=signal['quantity'],quoted_gross_usd=signal['quoted_gross_usd'],**arrival_bounds(signal,members)))

    def process(stamp,batches):
        nonlocal active,sequence
        while pending and pending[0][0]<stamp:
            item=heapq.heappop(pending);move(item[0]);check(item)
        move(stamp);prior_active=set(active);changed=set()
        for (_,token),rows in batches:
            b=books[token];b.apply_batch(stamp,rows)
            for side in SIDES:b.past_blocked[side]=masks[token,side].contains(milliseconds(stamp))
            changed.add(token)
        affected=recompute(changed)
        while pending and pending[0][0]==stamp:check(heapq.heappop(pending))
        if start<=stamp<end:
            active={key for key in active if key[0] not in affected}
            # Candidate episode starts only; repeated displayed liquidity is not
            # summed into P&L. Keep the preceding active set for affected events.
            for eid in affected:
                state=states[eid];peak(labels[eid,stamp.floor('h')],state,stamp)
                for p in state.packages:
                    bounds=state.bounds[p['package_id']]
                    if bounds['candidate'] is None:continue
                    key=eid,p['package_id'];active.add(key)
                    if key not in prior_active:
                        signal=dict(bounds['candidate'],event_id=eid,package_id=p['package_id'],token_ids=list(p['token_ids']),decision_at=stamp)
                        for delay in [0,50,100,250,500]:
                            at=stamp+pd.Timedelta(milliseconds=delay)
                            if at<end:sequence+=1;heapq.heappush(pending,(at,sequence,signal,delay))
            while pending and pending[0][0]==stamp:check(heapq.heappop(pending))

    batch=[];stamp=None
    for key,rows in source_batches(work.loc[work.timestamp<end]):
        if stamp is not None and key[0]!=stamp:
            process(stamp,batch);batch=[]
        stamp=key[0];batch.append((key,rows))
    if batch:process(stamp,batch)
    while pending:
        item=heapq.heappop(pending);move(item[0]);check(item)
    move(end)
    features=pd.DataFrame(features);labels=finalize_labels(labels.values())
    features.to_parquet(output/'hourly_features.parquet',index=False)
    labels.to_parquet(output/'hourly_label_bounds.parquet',index=False)
    joined=features.merge(labels,on=['event_id','hour'],validate='one_to_one');joined.to_csv(results/'event_hours.csv',index=False)
    arrival_frame=pd.DataFrame(arrivals,columns=['event_id','package_id','decision_at','arrival_at','latency_ms','quantity','quoted_gross_usd','status','conditional_gross_usd'])
    arrival_frame.to_csv(results/'arrival_checks.csv',index=False)
    # Paired comparison, with unchanged inputs and hourly endpoints.
    old_path=PROJECT_ROOT/'results/pmxt_pilot'/root.name/'source_validation_v2/event_hours.csv'
    if old_path.exists():
        old=pd.read_csv(old_path,dtype={'event_id':str});old['hour']=pd.to_datetime(old.hour,utc=True)
        old=old.rename(columns={'label_conditional_gross':'v2_occurrence','conditional_peak_gross_usd':'v2_observed_peak'})
        joined.merge(old[['event_id','hour','v2_occurrence','v2_observed_peak']],on=['event_id','hour'],how='left',validate='one_to_one').to_csv(results/'comparison_v2.csv',index=False)
    summary=dict(version=3,source_start=start,source_end=end,receipt_end=receipt_end,archive_hours=len(inputs),events=len(events),tokens=len(books),
        source_records=len(work),event_hours=len(labels),raw_counters=dict(counters),
        full_book_event_hours=int((labels.fully_observed_seconds>=3600-1e-8).sum()),
        full_ask_event_hours=int((labels.full_ask_seconds>=3600-1e-8).sum()),
        occurrence_known_hours=int(labels.label_occurrence.notna().sum()),
        occurrence_positive_hours=int((labels.label_occurrence==1).sum()),occurrence_negative_hours=int((labels.label_occurrence==0).sum()),
        occurrence_unknown_hours=int(labels.label_occurrence.isna().sum()),
        exact_value_hours=int(labels.label_peak_value_usd.notna().sum()),
        value_lower_bound_hours=int((labels.value_target_status=='lower_bound').sum()),
        full_book_time_fraction=float(labels.fully_observed_seconds.sum()/(len(labels)*3600)),
        full_ask_time_fraction=float(labels.full_ask_seconds.sum()/(len(labels)*3600)),
        occurrence_determined_time_fraction=float(labels.occurrence_determined_seconds.sum()/(len(labels)*3600)),
        blocked_with_incomplete_books_seconds=float(labels.blocked_with_incomplete_books_seconds.sum()),
        maximum_observed_value_lower_usd=float(labels.peak_value_lower_usd.max()),
        arrival_checks=len(arrival_frame),arrival_status_counts={f'{a}ms:{b}':int(n) for (a,b),n in arrival_frame.groupby(['latency_ms','status']).size().items()},
        ask_complete_events_per_scan={str(t):int(n) for t,n in features.groupby('hour').full_asks.sum().items()},
        recorded_transfer_bytes=sum(i['transfer_bytes'] for i in inputs),raw_cache_bytes=sum(i['cache_bytes'] for i in inputs),
        historical_parameters_verified=False,models_trained=False,new_final_test_accessed=False,net_pnl_estimated=False,inputs=inputs,
        feature_availability='Causal source-time states before scan; later retrospective masks excluded from model inputs.',
        design_sha256=sha256(design_path),implementation_sha256=code,
        derived_sha256={n:sha256(output/n) for n in ['hourly_features.parquet','hourly_label_bounds.parquet','side_drift_intervals.json']},
        results_sha256={p.name:sha256(p) for p in results.iterdir() if p.is_file() and p.name!='summary.json'},
        limitations=['Source-ms batch completion and compatible closing snapshot are explicit timestamp-semantics assumptions.',
            'No exchange sequence or original message hash in this archive schema; matching best quotes do not prove full integrity.',
            'Finite receipt buffer; missing deep-level updates can remain undetected.',
            'Level alternatives yield outer bounds; their joint combinations need not all be real states.',
            'Current metadata used retrospectively; historical fees/minima/ticks unverified.',
            'Arrival checks are conditional displayed support, not fills or P&L; no reservations/unwinding.'])
    if any(sha256(PROJECT_ROOT/p)!=h for p,h in code.items()):raise ValueError('Implementation changed during execution')
    write_json(results/'summary.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ['inputs','implementation_sha256','results_sha256','derived_sha256']},indent=2,default=str))
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=DEFAULT_ROOT)
    parser.add_argument('--analysis-end',default='2026-05-04T04:00:00Z');parser.add_argument('--name',default='source_validation_v3')
    args=parser.parse_args();rerun(args.root,analysis_end=args.analysis_end,name=args.name)
