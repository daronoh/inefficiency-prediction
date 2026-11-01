"""Source-clock PMXT reconstruction and independent full-depth checks.

This is a hypothetical timely-feed research clock, not availability through
PMXT. Native full-book timestamps are assumed to identify the state version;
checkpoint disagreement explicitly tests, rather than proves, that assumption.
"""
from __future__ import annotations
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass,field
import heapq
import math

import pandas as pd

from src.data.books import Ladder,RawBook
from src.data.pmxt_books import levels,milliseconds,price_key


def source_batches(frame):
    """Group all same-token/source-millisecond records, independent of receipt."""
    ordered = frame.sort_values(['timestamp','asset_id','timestamp_received','archive_hour','archive_row'],kind='stable')
    columns = list(ordered.columns)
    key,batch = None,[]
    for values in ordered.itertuples(index=False,name=None):
        row = dict(zip(columns,values))
        current = row['timestamp'],row['asset_id']
        if key is not None and current!=key:
            yield key,batch
            batch = []
        key = current;batch.append(row)
    if key is not None: yield key,batch


def differences(a,b):
    return {p for p in a.keys()|b.keys() if abs(a.get(p,0)-b.get(p,0))>1e-5}


def merge_intervals(intervals):
    result = []
    for start,end in sorted(intervals):
        if end<=start: continue
        if result and start<=result[-1][1]: result[-1] = result[-1][0],max(end,result[-1][1])
        else: result.append((start,end))
    return result


@dataclass
class SourceBook:
    capture_start: int
    strict_checkpoints: bool = False
    record_diagnostics: bool = True
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    baseline: int | None = None
    last_received: int | None = None  # compatibility with EventState; SOURCE clock here
    uncertain: bool = False
    blocked: bool = False
    tick_size: float | None = None
    counters: Counter = field(default_factory=Counter)
    bad_intervals: list = field(default_factory=list)
    checkpoint_rows: list = field(default_factory=list)
    examples: list = field(default_factory=list)
    contradiction_examples: list = field(default_factory=list)
    _cached: RawBook | None = None

    @property
    def usable(self):
        return self.baseline is not None and not self.uncertain and not self.blocked

    def apply_batch(self,stamp,rows):
        source = milliseconds(stamp)
        if source<self.capture_start: return
        if self.last_received is not None and source<=self.last_received:
            raise ValueError('Apply exactly one complete group per source millisecond')
        self.last_received = source
        self._cached = None
        for row in rows: self.counters[row['event_type']] += 1
        prior_baseline = self.baseline
        try:
            revisions = {}
            snapshots = []
            best_bids,best_asks,ticks = set(),set(),set()
            for row in rows:
                kind = row['event_type']
                if kind=='book':
                    snapshots.append((levels(row['bids']),levels(row['asks'])))
                elif kind=='price_change':
                    side = row['side']
                    if side not in ('BUY','SELL'): raise ValueError('Invalid side')
                    p,s = price_key(row['price']),float(row['size'])
                    if not math.isfinite(s) or s<0: raise ValueError('Invalid quantity')
                    revisions.setdefault((side,p),set()).add(s)
                    if not pd.isna(row.get('best_bid')): best_bids.add(price_key(row['best_bid']))
                    if not pd.isna(row.get('best_ask')): best_asks.add(price_key(row['best_ask']))
                elif kind=='tick_size_change':
                    tick = float(row['new_tick_size'])
                    if not math.isfinite(tick) or not 0<tick<=1: raise ValueError('Invalid tick')
                    ticks.add(tick)
                elif kind!='last_trade_price': raise ValueError('Unknown type')
            ambiguous = any(len(v)>1 for v in revisions.values()) or len(ticks)>1
            if ambiguous:
                self.counters['ambiguous_revision_groups'] += 1
                self.uncertain = True
                self.bad_intervals.append((prior_baseline or source,source+1))
            for (side,price),sizes in revisions.items():
                if len(sizes)!=1: continue  # Never invent an order for conflicting sizes.
                size = next(iter(sizes))
                target = self.bids if side=='BUY' else self.asks
                if size: target[price] = size
                else: target.pop(price,None)
            if ticks: self.tick_size = min(ticks)
            if snapshots:
                unique = {(tuple(sorted(b.items())),tuple(sorted(a.items()))) for b,a in snapshots}
                self.counters['repeated_snapshot_rows'] += len(snapshots)-len(unique)
                if len(unique)>1:
                    self.uncertain = True
                    self.counters['conflicting_snapshot_groups'] += 1
                    self.bad_intervals.append((prior_baseline or source,source+1))
                else:
                    bids,asks = snapshots[0]
                    if prior_baseline is not None and self.record_diagnostics:
                        db,da = differences(self.bids,bids),differences(self.asks,asks)
                        top_match = max(self.bids,default=0)==max(bids,default=0) and min(self.asks,default=10000)==min(asks,default=10000)
                        touched = {p for _,p in revisions}
                        row = dict(source_ms=source,previous_snapshot_ms=prior_baseline,
                            exact=not db and not da,top_match=top_match,
                            ask_exact=not da,bid_exact=not db,
                            best_ask_size_match=(min(self.asks,default=10000)==min(asks,default=10000)
                                and abs(self.asks.get(min(asks,default=10000),0)-asks.get(min(asks,default=10000),0))<=1e-5),
                            different_levels=len(db)+len(da),
                            same_source_changes=bool(revisions),
                            difference_touches_same_source_update=bool((db|da)&touched))
                        self.checkpoint_rows.append(row)
                        self.counters['checkpoint_comparisons'] += 1
                        self.counters['checkpoint_exact'] += row['exact']
                        self.counters['checkpoint_top_match'] += top_match
                        if db or da:
                            self.counters['checkpoint_disagreements'] += 1
                            # A full book can itself supply a new state. Equality
                            # is a diagnostic, not a mandatory delta-stream
                            # invariant. Keep the stricter hypothesis optional.
                            if self.strict_checkpoints:
                                self.bad_intervals.append((prior_baseline,source))
                            if len(self.examples)<3:
                                self.examples.append(dict(row,bid_differences=sorted(db)[:8],ask_differences=sorted(da)[:8]))
                    self.bids,self.asks,self.baseline = bids,asks,source
                    self.uncertain = ambiguous
                    if any(len(sizes)==1 and abs((bids if side=='BUY' else asks).get(p,0)-next(iter(sizes)))>1e-5
                        for (side,p),sizes in revisions.items()):
                        self.uncertain = True
                        self.counters['snapshot_delta_tie_conflicts'] += 1
                        self.bad_intervals.append((source,source+1))
            if self.baseline is not None and revisions:
                bad = ((best_bids and max(self.bids,default=0) not in best_bids)
                    or (best_asks and min(self.asks,default=10000) not in best_asks))
                if bad:
                    self.uncertain = True
                    self.counters['top_mismatch_groups'] += 1
                    self.bad_intervals.append((prior_baseline or source,source+1))
                    if self.record_diagnostics and len(self.contradiction_examples)<3:
                        self.contradiction_examples.append(dict(source_ms=source,
                            baseline_ms=self.baseline,actual_best_bid=max(self.bids,default=0)/10000,
                            actual_best_ask=min(self.asks,default=10000)/10000,
                            reported_bids=[p/10000 for p in sorted(best_bids)],
                            reported_asks=[p/10000 for p in sorted(best_asks)],
                            same_time_full_snapshots=len(snapshots),source_group_rows=len(rows)))
            if self.baseline is not None and self.bids and self.asks and max(self.bids)>min(self.asks):
                self.uncertain = True
                self.counters['crossed_groups'] += 1
                self.bad_intervals.append((prior_baseline or source,source+1))
        except (ValueError,TypeError,KeyError,OverflowError):
            self.counters['malformed_groups'] += 1
            self.uncertain = True
            self.bad_intervals.append((prior_baseline or source,source+1))

    def raw_book(self,stamp,*,minimum=5,assumed_tick=.0001):
        if not self.usable: return None
        if self._cached is None:
            conv = lambda v:[dict(price=p/10000,size=s) for p,s in v.items()]
            self._cached = RawBook(pd.Timestamp(stamp),Ladder(conv(self.asks),True),
                Ladder(conv(self.bids),False),minimum,self.tick_size or assumed_tick)
        return self._cached


class QualityMask:
    def __init__(self,intervals):
        self.intervals = merge_intervals(intervals)
        self.starts = [a for a,_ in self.intervals]

    def contains(self,stamp):
        i = bisect_right(self.starts,stamp)-1
        return i>=0 and stamp<self.intervals[i][1]


def fixed_arrival_check(signal,books):
    """Diagnostic at an externally scheduled arrival; no quantity/limit revision.

    This checks displayed support, not actual fills, persistent liquidity
    reservations, net fees, unwind P&L or a complete strategy backtest.
    """
    if len(books)!=len(signal['limits']) or len(books)<2 or signal['quantity']<=0:
        raise ValueError('An arrival needs the fixed positive size and every basket leg')
    if any(b is None or not b.clean for b in books): return dict(status='unknown',supported_legs=None)
    quotes = [b.asks.quote(signal['quantity'],limit) for b,limit in zip(books,signal['limits'])]
    complete = all(q is not None for q in quotes)
    return dict(status='display_supported' if complete else 'insufficient_displayed_depth',
        supported_legs=sum(q is not None for q in quotes),
        conditional_gross_usd=signal['quantity']-sum(q[0] for q in quotes) if complete else None)
