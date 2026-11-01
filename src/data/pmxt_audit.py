"""Streaming coverage/features and conditional quote audit for the PMXT pilot.

This is a data-adapter acceptance audit, not a trading backtest. Repeated quotes
are not summed as collectible profit. Historical fees remain unverified.
"""
from __future__ import annotations
from collections import defaultdict
import json
import math

import pandas as pd

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import isolated, write_json
from src.data.pmxt_books import TokenBook,milliseconds,receive_batches
from src.data.pmxt_pilot import DEFAULT_ROOT,load_plan
from src.simulator.sizing import choose_order


def conditional_quote(books):
    """Zero-fee diagnostic under 5-share / .0001 initial tick assumptions.

    Callers must NOT identify it as executable fee-net arbitrage. Only observed
    tick changes replace the initial tick scenario; current Gamma ticks are not
    backdated. Capital/shares are uncapped; displayed depth bounds quantity.
    """
    if any(b is None or not b.clean for b in books): return None
    if sum(b.asks.prices[0] for b in books) >= 1-1e-12: return None
    return choose_order(books,min_quantity=5,max_quantity=None,cash_cap=None,
        min_limit_margin=0)


class EventState:
    def __init__(self,event,packages,books):
        self.event,self.packages,self.books = event,packages,books
        self.package_states = {}
        self.tokens = {str(t) for p in packages for t in p['token_ids']}
        self.last_quote = None
        self.recompute(self.tokens,pd.Timestamp('1970-01-01',tz='UTC'))

    def recompute(self, changed, stamp):
        for package in self.packages:
            tokens = list(map(str,package['token_ids']))
            if not set(tokens)&changed: continue
            states = [self.books[t] for t in tokens]
            known = all(b.usable for b in states)
            raw = [b.raw_book(stamp) for b in states] if known else []
            clean = known and all(b.clean for b in raw)
            gap = 1-sum(b.asks.prices[0] for b in raw) if clean else None
            quote = conditional_quote(raw) if clean and gap>0 else None
            self.package_states[package['package_id']] = dict(known=known,clean=clean,
                gap=gap,quote=quote)
        values = list(self.package_states.values())
        self.known = bool(values) and all(v['known'] for v in values)
        self.known_fraction = sum(v['known'] for v in values)/len(values) if values else 0
        self.clean_fraction = sum(v['clean'] for v in values)/len(values) if values else 0
        self.gap = max((v['gap'] for v in values if v['gap'] is not None),default=None)
        quotes = [v['quote'] for v in values if v['quote']]
        self.last_quote = max(quotes,key=lambda q:q['quoted_gross_usd'],default=None)

    def features(self,stamp):
        states = [self.books[t] for t in self.tokens]
        ages = [(milliseconds(stamp)-b.last_received)/1000 for b in states if b.last_received is not None]
        spreads,depths = [],[]
        for b in states:
            raw = b.raw_book(stamp)
            if raw is not None and raw.clean:
                spreads.append(raw.asks.prices[0]-raw.bids.prices[0])
                depths.append(float(raw.asks.cumulative_size[-1]))
        return dict(event_id=self.event['event_id'],event_slug=self.event['event_slug'],
            classification=self.event['classification'],hour=stamp,
            package_count=len(self.packages),token_count=len(self.tokens),
            usable_token_fraction=sum(b.usable for b in states)/len(states),
            known_package_fraction=self.known_fraction,clean_package_fraction=self.clean_fraction,
            quoted_gap=self.gap,mean_spread=sum(spreads)/len(spreads) if spreads else None,
            minimum_ask_depth_shares=min(depths,default=None),
            max_receive_age_seconds=max(ages,default=None),
            snapshot_conditional_gross_usd=self.last_quote['quoted_gross_usd'] if self.last_quote else 0)


def audit(root=DEFAULT_ROOT):
    root,plan = load_plan(root)
    start,end,warmup = (pd.Timestamp(plan[k]) for k in ['start','end','warmup_start'])
    # Replay ONLY a contiguous, hash-checked prefix. A missing hour is unknown,
    # not an empty stream and not permission to carry a book across a gap.
    paths = []
    for stamp in pd.date_range(warmup,end,freq='h',inclusive='left'):
        path = root/'raw'/f'{stamp:%Y-%m-%dT%H}.parquet'
        manifest_path = path.with_suffix('.json')
        if not manifest_path.exists(): break
        manifest = json.loads(manifest_path.read_text())
        if sha256(path) != manifest['sha256'] or set(manifest['assets']) != set(plan['assets']):
            raise ValueError('Selected archive cache changed')
        paths.append((stamp,path,manifest))
    if len(paths)<2:
        raise ValueError('Need the warmup hour and at least one analysis hour')
    actual_end = paths[-1][0]+pd.Timedelta(hours=1)
    books = {t:TokenBook(milliseconds(warmup)) for t in plan['assets']}
    events,packages = (pd.read_parquet(root/f'{n}.parquet').to_dict('records') for n in ['events','packages'])
    state = {}
    by_token = defaultdict(set)
    for event in events:
        eid = event['event_id']
        state[eid] = EventState(event,[p for p in packages if p['event_id']==eid],books)
        for token in state[eid].tokens: by_token[token].add(eid)
    features,labels = [],{}
    previous = warmup
    current_hour = None
    messages = 0
    snapshot_delays = []
    change_delays = []

    def begin_hour(hour):
        nonlocal current_hour
        for eid,event in state.items():
            features.append(event.features(hour))
            labels[(eid,hour)] = dict(event_id=eid,hour=hour,
                fully_observed_seconds=0.,any_package_observed_seconds=0.,
                conditional_positive_seconds=0.,conditional_peak_gross_usd=0.,
                peak_quantity=0,peak_received_at=None)
        current_hour = hour

    def advance(target):
        nonlocal previous,current_hour
        while previous < target:
            hour = previous.floor('h')
            stop = min(target,hour+pd.Timedelta(hours=1))
            if hour>=start:
                if hour != current_hour:
                    begin_hour(hour)
                duration = (stop-previous).total_seconds()
                for eid,event in state.items():
                    row = labels[(eid,hour)]
                    row['fully_observed_seconds'] += duration*event.known
                    row['any_package_observed_seconds'] += duration*(event.known_fraction>0)
                    if event.last_quote:
                        row['conditional_positive_seconds'] += duration
                        quote = event.last_quote
                        if quote['quoted_gross_usd'] > row['conditional_peak_gross_usd']:
                            row.update(conditional_peak_gross_usd=quote['quoted_gross_usd'],
                                peak_quantity=quote['quantity'],peak_received_at=previous)
            previous = stop
        # Freeze boundary features BEFORE processing messages arriving there.
        if target == target.floor('h') and start<=target<actual_end and current_hour!=target:
            begin_hour(target)

    def process(stamp, batches):
        nonlocal messages
        advance(stamp)
        changed = set()
        for (_,token),rows in batches:
            book = books[token]
            for row in rows:
                book.apply(row);messages += 1
            book.validate_batch()
            changed.add(token)
        affected = {eid for token in changed for eid in by_token[token]}
        for eid in affected: state[eid].recompute(changed,stamp)
        # Quotes that last <1ms still count as observed positive evidence.
        hour = stamp.floor('h')
        if hour>=start:
            for eid in affected:
                event = state[eid]
                if event.last_quote and (eid,hour) in labels:
                    row,quote = labels[(eid,hour)],event.last_quote
                    if quote['quoted_gross_usd']>row['conditional_peak_gross_usd']:
                        row.update(conditional_peak_gross_usd=quote['quoted_gross_usd'],
                            peak_quantity=quote['quantity'],peak_received_at=stamp)

    for clock,path,manifest in paths:
        frame = pd.read_parquet(path)
        if not frame.empty and (frame.timestamp_received.min()<clock or frame.timestamp_received.max()>=clock+pd.Timedelta(hours=1)):
            raise ValueError('Receipt timestamps do not match archive hour')
        full = frame.loc[frame.event_type=='book']
        snapshot_delays.extend((full.timestamp_received-full.timestamp).dt.total_seconds().tolist())
        delta = frame.loc[frame.event_type=='price_change']
        change_delays.extend((delta.timestamp_received-delta.timestamp).dt.total_seconds().tolist())
        pending,stamp = [],None
        for key,rows in receive_batches(frame):
            if stamp is not None and key[0] != stamp:
                process(stamp,pending);pending = []
            stamp = key[0]
            pending.append((key,rows))
        if pending: process(stamp,pending)
        advance(clock+pd.Timedelta(hours=1))
        print(f'Replayed {clock:%Y-%m-%dT%H}: {len(frame):,} messages',flush=True)
    feature_frame = pd.DataFrame(features)
    label_frame = pd.DataFrame(labels.values())
    label_frame['label_conditional_gross'] = pd.array([
        1 if r['conditional_peak_gross_usd']>0 else 0 if r['fully_observed_seconds']>=3600-1e-8 else None
        for r in label_frame.to_dict('records')],dtype='Int64')
    label_frame['fee_net_executable_status'] = 'unknown_historical_fees_and_metadata'
    quality = pd.DataFrame([dict(asset_id=t,usable_at_end=b.usable,
        source_baseline_ms=b.baseline,receive_ms=b.last_received,**dict(b.counters)) for t,b in books.items()])
    folder = isolated(PROJECT_ROOT/'results/pmxt_pilot'/root.name,'results')
    folder.mkdir(parents=True,exist_ok=True)
    feature_frame.to_parquet(root/'hourly_features.parquet',index=False)
    label_frame.to_parquet(root/'hourly_quote_audit.parquet',index=False)
    quality.to_csv(folder/'token_quality.csv',index=False)
    feature_frame.merge(label_frame,on=['event_id','hour'],validate='one_to_one').to_csv(folder/'event_hours.csv',index=False)
    checkpoint = {t:dict(baseline=b.baseline,last_received=b.last_received,
        capture_start=b.capture_start,bids=b.bids,asks=b.asks,revisions=[[*k,*v] for k,v in b.revisions.items()],
        usable=b.usable,tick_size=b.tick_size,tick_source=b.tick_source,expected=b.expected,mismatch=b.mismatch,malformed=b.malformed)
        for t,b in books.items()}
    write_json(root/'replay_checkpoint.json',dict(at=actual_end,books=checkpoint,
        note='Audit checkpoint, raw messages are the authoritative replay source'))
    summary = dict(planned_start=start,planned_end=end,replayed_start=start,replayed_end=actual_end,
        full_week_complete=actual_end==end,archive_hours=len(paths),analysis_hours=(actual_end-start).total_seconds()/3600,
        events=len(events),event_counts=plan['selected'],tokens=len(books),baskets=len(packages),messages=messages,
        event_hours=len(label_frame),fully_observed_event_hours=int((label_frame.fully_observed_seconds>=3600-1e-8).sum()),
        observed_event_hour_fraction=float(label_frame.fully_observed_seconds.sum()/(3600*len(label_frame))),
        conditional_positive_event_hours=int((label_frame.conditional_peak_gross_usd>0).sum()),
        maximum_conditional_one_basket_gross_usd=float(label_frame.conditional_peak_gross_usd.max()),
        usable_tokens_at_end=sum(b.usable for b in books.values()),
        top_mismatch_batches=sum(b.counters['top_mismatch_batches'] for b in books.values()),
        malformed_messages=sum(b.counters['malformed'] for b in books.values()),
        full_snapshot_delay_seconds=pd.Series(snapshot_delays,dtype=float).describe(percentiles=[.5,.9,.99]).to_dict(),
        price_change_delay_seconds=pd.Series(change_delays,dtype=float).describe(percentiles=[.5,.9,.99]).to_dict(),
        historical_fees_verified=False,net_profit_estimated=False,models_trained=False,
        semantics='Conditional zero-fee displayed margin, NOT P&L. Max per event-hour; never sum repeated displayed depth.',
        transfer_bytes=sum(m['transfer_bytes'] for _,_,m in paths),raw_cache_bytes=sum(p.stat().st_size for _,p,_ in paths),
        assumptions=['current rules/membership retrospectively fixed','minimum 5 shares','initial tick .0001 scenario until observed change',
            'no age/persistence cutoff','receive-time causal replay','uncapped capital/shares','no simulated fills'],
        quality_limits=['best-quote consistency is necessary, not proof of recording completeness',
            'no exchange sequence numbers; missing deep-level updates cannot always be detected',
            'equal source milliseconds use received update precedence; true exchange ordering can be ambiguous',
            'full snapshots older than capture start remain left-censored until a fresh baseline'],
        files_sha256={n:sha256(root/n) for n in ['plan.json','events.parquet','markets.parquet','packages.parquet',
            'hourly_features.parquet','hourly_quote_audit.parquet']},
        implementation_sha256={p.relative_to(PROJECT_ROOT).as_posix():sha256(p)
            for p in sorted((PROJECT_ROOT/'src/data').glob('pmxt_*.py'))})
    write_json(folder/'summary.json',summary)
    return summary
