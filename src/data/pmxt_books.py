"""Causal PMXT depth reconstruction. Does not use any Dome paths or cutoffs.

Source timestamps order revisions; receive timestamps determine availability.
Price-change sizes replace a level, and zero deletes it. Full snapshots are
rebased with newer level revisions that have ALREADY arrived.
"""
from __future__ import annotations
from collections import Counter
from dataclasses import dataclass, field
import json
import math

import pandas as pd

from src.data.books import Ladder, RawBook


def milliseconds(value):
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None or pd.isna(stamp):
        raise ValueError('An explicit, finite UTC clock is required')
    return stamp.value//1_000_000


def price_key(value):
    price = float(value)
    if not math.isfinite(price) or not 0 <= price <= 1:
        raise ValueError('Price outside [0,1]')
    key = round(price*10000)
    if abs(price-key/10000) > 1e-8:
        raise ValueError('Price exceeds archive decimal precision')
    return key


def levels(value):
    rows = json.loads(value) if isinstance(value,str) else value
    if not isinstance(rows,list):
        raise ValueError('Full snapshot depth must be an array')
    result = {}
    for row in rows:
        p,s = (row['price'],row['size']) if isinstance(row,dict) else row
        key,size = price_key(p),float(s)
        if not math.isfinite(size) or size < 0 or key in result:
            raise ValueError('Invalid or duplicate price level')
        if size:
            result[key] = size
    return result


@dataclass
class TokenBook:
    capture_start: int
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    revisions: dict = field(default_factory=dict)
    baseline: int | None = None
    last_received: int | None = None
    last_source: int | None = None
    expected: tuple | None = None
    mismatch: bool = False
    malformed: bool = False
    tick_size: float | None = None
    tick_source: int | None = None
    counters: Counter = field(default_factory=Counter)
    _cached: RawBook | None = None

    @property
    def usable(self):
        return (self.baseline is not None and self.baseline >= self.capture_start
                and not self.mismatch and not self.malformed)

    def invalidate(self, resume_time):
        """A missing archive interval breaks continuity until a fresh snapshot."""
        self.capture_start = milliseconds(resume_time)
        self.bids.clear(); self.asks.clear(); self.revisions.clear()
        self.baseline = self.expected = self.last_source = None
        self.tick_size = self.tick_source = None
        self.mismatch = self.malformed = False
        self._cached = None
        self.counters['archive_gaps'] += 1

    def apply(self, row):
        received,source = milliseconds(row['timestamp_received']),milliseconds(row['timestamp'])
        if self.last_received is not None and received < self.last_received:
            raise ValueError('PMXT messages must arrive in receive-time order')
        self.last_received = received
        self.last_source = source if self.last_source is None else max(source,self.last_source)
        event = row['event_type']
        self.counters[event] += 1
        self._cached = None
        try:
            if event == 'book':
                bids,asks = levels(row['bids']),levels(row['asks'])
                if self.baseline is not None and source < self.baseline:
                    self.counters['older_snapshots_ignored'] += 1
                    return
                # Deletion tombstones are retained too. No future messages enter
                # this journal: it contains only apply() calls already received.
                journal = {k:v for k,v in self.revisions.items() if v[0] >= source}
                for (side,price),(_,size) in journal.items():
                    target = bids if side == 'BUY' else asks
                    if size: target[price] = size
                    else: target.pop(price,None)
                self.bids,self.asks,self.baseline = bids,asks,source
                self.revisions = journal
                self.malformed = False
                self.mismatch = False
                self.counters['rebased_levels'] += len(journal)
                self.counters['equal_source_rebased_levels'] += sum(v[0]==source for v in journal.values())
                if self.expected is not None and self.expected[0] < source:
                    self.expected = None
            elif event == 'price_change':
                side = row['side']
                if side not in ('BUY','SELL'):
                    raise ValueError('Invalid price-change side')
                price,size = price_key(row['price']),float(row['size'])
                if not math.isfinite(size) or size < 0:
                    raise ValueError('Invalid level size')
                key = side,price
                previous = self.revisions.get(key)
                if ((self.baseline is not None and source < self.baseline)
                    or (previous is not None and source < previous[0])):
                    self.counters['older_revisions_ignored'] += 1
                    return
                self.revisions[key] = source,size
                target = self.bids if side == 'BUY' else self.asks
                if size: target[price] = size
                else: target.pop(price,None)
                if (self.expected is None or source >= self.expected[0]):
                    bid,ask = row.get('best_bid'),row.get('best_ask')
                    bid = None if pd.isna(bid) else price_key(bid)
                    ask = None if pd.isna(ask) else price_key(ask)
                    if self.expected is None or source>self.expected[0]:
                        self.expected = source,(() if bid is None else (bid,)),(() if ask is None else (ask,))
                    else:
                        _,bids,asks = self.expected
                        self.expected = source,tuple(sorted(set(bids)|({bid} if bid is not None else set()))),tuple(sorted(set(asks)|({ask} if ask is not None else set())))
            elif event == 'tick_size_change':
                tick = float(row['new_tick_size'])
                if not math.isfinite(tick) or not 0 < tick <= 1:
                    raise ValueError('Invalid tick size')
                if self.tick_source is None or source>=self.tick_source:
                    self.tick_size,self.tick_source = tick,source
            elif event != 'last_trade_price':
                raise ValueError('Unrecognised event type')
            # Trades are observations, never book-level decrements.
        except (ValueError,TypeError,KeyError,OverflowError):
            self.malformed = True
            self.counters['malformed'] += 1

    def validate_batch(self):
        """Validate after all updates at a token's same receipt millisecond.

        Missing levels cannot be repaired using best_bid/ask: those fields have
        no depth. A mismatch remains unknown until a new valid full snapshot.
        """
        if self.baseline is None or self.expected is None:
            return
        _,bids,asks = self.expected
        # One source millisecond can report conflicting intermediate best
        # quotes. Archive row order cannot establish the final exchange quote.
        # A compatible reported best is necessary; we never invent depth from it.
        bad = ((bids and max(self.bids,default=0) not in bids)
            or (asks and min(self.asks,default=10000) not in asks))
        if len(bids)>1 or len(asks)>1:
            self.counters['ambiguous_source_best_batches'] += 1
        if bad:
            self.counters['top_mismatch_batches'] += 1
            self.mismatch = True

    def raw_book(self, stamp, *, minimum=5, assumed_tick=.0001):
        if not self.usable:
            return None
        if self._cached is None:
            convert = lambda side:[{'price':p/10000,'size':s} for p,s in side.items()]
            self._cached = RawBook(pd.Timestamp(stamp),Ladder(convert(self.asks),True),
                Ladder(convert(self.bids),False),minimum,self.tick_size or assumed_tick)
        return self._cached


def receive_batches(frame):
    """Stable causal ordering across tokens; the source clock never sorts arrivals."""
    if frame.empty:
        return
    ordered = frame.sort_values(['timestamp_received','asset_id','archive_row'],kind='stable')
    # itertuples avoids allocating a pandas Series for every delta.
    columns = list(ordered.columns)
    batch,key = [],None
    for values in ordered.itertuples(index=False,name=None):
        row = dict(zip(columns,values))
        current = row['timestamp_received'],row['asset_id']
        if key is not None and current != key:
            yield key,batch
            batch = []
        key = current
        batch.append(row)
    if key is not None:
        yield key,batch
