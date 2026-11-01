"""Version 3: bounded, side/level-specific uncertainty on the source clock.

No quantities are inferred from best-price fields. Full snapshots close a
same-source batch only when their sizes and best prices are compatible with
the batch. Conflicting alternatives supply bounds, never a selected ordering.
"""
from __future__ import annotations
from collections import Counter
from dataclasses import dataclass,field
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd

from src.data.books import Ladder
from src.data.pmxt_books import levels,milliseconds,price_key
from src.simulator.sizing import depth_quantities


SIDES=('BUY','SELL')


@dataclass
class BoundedBook:
    capture_start:int
    depth:dict=field(default_factory=lambda:{s:{} for s in SIDES})
    alternatives:dict=field(default_factory=lambda:{s:{} for s in SIDES})
    faults:dict=field(default_factory=lambda:{s:False for s in SIDES})
    past_blocked:dict=field(default_factory=lambda:{s:False for s in SIDES})
    baseline:int|None=None
    last_source:int|None=None
    tick_options:tuple=(.0001,)
    counters:Counter=field(default_factory=Counter)
    drift_intervals:list=field(default_factory=list)
    issues:list=field(default_factory=list)
    _cache:dict=field(default_factory=dict)

    def available(self,side):
        return self.baseline is not None and not self.faults[side] and not self.past_blocked[side]

    def exact(self,side):
        return self.available(side) and not self.alternatives[side]

    def ladder(self,side='SELL',bound='lower'):
        if not self.available(side):return None
        key=side,bound
        if key not in self._cache:
            d=dict(self.depth[side])
            for p,options in self.alternatives[side].items():
                size=(min if bound=='lower' else max)(options)
                if size:d[p]=size
                else:d.pop(p,None)
            self._cache[key]=Ladder([dict(price=p/10000,size=s) for p,s in d.items()],side=='SELL')
        return self._cache[key]

    def possible_best(self,side):
        """Outer set of possible best prices; absent asks/bids use 1/0."""
        mandatory=set(self.depth[side])
        optional=set()
        for p,options in self.alternatives[side].items():
            if min(options)>0:mandatory.add(p)
            elif max(options)>0:optional.add(p)
        sentinel=10000 if side=='SELL' else 0
        best=(min(mandatory) if side=='SELL' else max(mandatory)) if mandatory else sentinel
        better={p for p in optional if p<=best} if side=='SELL' else {p for p in optional if p>=best}
        return better|{best}

    def _issue(self,source,reason,side=None,*,drift=False):
        self.counters[reason]+=1
        if len(self.issues)<12:
            self.issues.append(dict(source_ms=source,reason=reason,side=side,baseline_ms=self.baseline))
        for s in ([side] if side else SIDES):
            if drift and self.baseline is not None and self.baseline<source:
                self.drift_intervals.append((s,self.baseline,source))
            self.faults[s]=True

    def apply_batch(self,stamp,rows):
        source=milliseconds(stamp)
        if source<self.capture_start:return
        if self.last_source is not None and source<=self.last_source:
            raise ValueError('A complete native-time group must be applied exactly once')
        self.last_source=source;self._cache.clear()
        snapshots=[];changes={};best={s:set() for s in SIDES};ticks=set()
        try:
            for r in rows:
                kind=r['event_type'];self.counters[kind]+=1
                if kind=='book':snapshots.append({'BUY':levels(r['bids']),'SELL':levels(r['asks'])})
                elif kind=='price_change':
                    side=r['side']
                    if side not in SIDES:raise ValueError('Invalid side')
                    p,size=price_key(r['price']),float(r['size'])
                    if not math.isfinite(size) or size<0:raise ValueError('Invalid size')
                    options=changes.setdefault((side,p),set())
                    if size in options:self.counters['identical_level_replays']+=1
                    options.add(size)
                    for s,col in [('BUY','best_bid'),('SELL','best_ask')]:
                        if not pd.isna(r.get(col)):best[s].add(price_key(r[col]))
                elif kind=='tick_size_change':
                    v=float(r['new_tick_size'])
                    if not math.isfinite(v) or not 0<v<=1:raise ValueError('Invalid tick')
                    ticks.add(v)
                elif kind!='last_trade_price':raise ValueError('Unknown event')
            if ticks:self.tick_options=tuple(sorted(ticks))
            if snapshots:
                unique={tuple(tuple(sorted(b[s].items())) for s in SIDES):b for b in snapshots}
                self.counters['identical_snapshot_replays']+=len(snapshots)-len(unique)
                snapshots=list(unique.values())
                self.depth={s:{} for s in SIDES};self.alternatives={s:{} for s in SIDES}
                recovered=sum(self.faults.values())
                self.faults={s:False for s in SIDES};self.baseline=source
                self.counters['snapshot_side_recoveries']+=recovered
                for s in SIDES:
                    for p in set().union(*(set(b[s]) for b in snapshots)):
                        options={b[s].get(p,0.) for b in snapshots}
                        if len(options)==1:
                            value=next(iter(options))
                            if value:self.depth[s][p]=value
                        else:self.alternatives[s][p]=options
                if len(snapshots)>1:self.counters['ambiguous_full_snapshot_groups']+=1
                # Matching a snapshot's value is required before assuming that
                # the snapshot closes a same-time level-change sequence.
                unresolved=False
                for (s,p),options in changes.items():
                    final={b[s].get(p,0.) for b in snapshots}
                    if len(snapshots)==1 and next(iter(final)) in options:
                        if len(options)>1:self.counters['level_conflicts_resolved_by_snapshot']+=1
                    else:
                        combined=options|final
                        if len(combined)>1:
                            self.depth[s].pop(p,None);self.alternatives[s][p]=combined
                            unresolved=True;self.counters['unresolved_snapshot_level_ties']+=1
                if changes and not unresolved and len(snapshots)==1:
                    self.counters['compatible_snapshot_close_groups']+=1
            else:
                for (s,p),options in changes.items():
                    if len(options)==1:
                        value=next(iter(options))
                        if p in self.alternatives[s]:
                            self.counters['level_conflicts_resolved_by_later_update']+=1
                            del self.alternatives[s][p]
                        if value:self.depth[s][p]=value
                        else:self.depth[s].pop(p,None)
                    else:
                        self.depth[s].pop(p,None);self.alternatives[s][p]=options
                        self.counters['unresolved_level_conflicts']+=1
            # Final-state checks happen only AFTER all source-time rows.
            for s in SIDES:
                if self.baseline is not None and best[s] and not(self.possible_best(s)&best[s]):
                    # The absent level update's onset is unknown: retain the
                    # conservative lookback on THIS SIDE, not the entire token.
                    self._issue(source,'best_price_contradiction',s,drift=not bool(snapshots))
                elif best[s] and self.alternatives[s]:
                    self.counters['best_price_compatible_with_bounded_depth']+=1
            if self.baseline is not None and not any(self.faults.values()):
                if max(self.possible_best('BUY'))>min(self.possible_best('SELL')):
                    # Avoid trading even a possibly crossed alternative. A new
                    # explicit snapshot restores the joint state. No pruning.
                    self._issue(source,'crossed_final_group')
                    self._cache.clear()
        except (ValueError,TypeError,KeyError,OverflowError):
            self._issue(source,'malformed_group')
            self._cache.clear()


def optimise_asks(ladders,ticks,minimum=5):
    """Buy-only depth sizing, without fabricating bids to satisfy a book API.

    Lower-bound calls require limit prices legal for every possible tick.
    No capital/share cap. Both integrated and worst-limit costs must be < payout.
    """
    if len(ladders)<2 or any(a is None or not len(a) for a in ladders):return None
    members=[SimpleNamespace(asks=a,min_order_size=minimum) for a in ladders]
    quantities=depth_quantities(members,minimum,None,None,0)
    if not len(quantities):return None
    costs,limits=zip(*(a._quotes(quantities) for a in ladders))
    costs,limits=np.asarray(costs),np.asarray(limits)
    gross=quantities-costs.sum(axis=0)
    valid=np.isfinite(costs).all(axis=0)&(gross>1e-10)&(quantities*(1-limits.sum(axis=0))>1e-10)
    for i,options in enumerate(ticks):
        for tick in options:
            grid=limits[i]/tick
            valid&=np.isclose(grid,np.rint(grid),rtol=0,atol=1e-7)
    indices=np.flatnonzero(valid)
    if not len(indices):return None
    maximum=float(gross[indices].max())
    chosen=indices[np.flatnonzero(np.isclose(gross[indices],maximum,rtol=0,atol=1e-9))[0]]
    return dict(quantity=int(quantities[chosen]),limits=limits[:,chosen].tolist(),
        ask_costs=costs[:,chosen].tolist(),quoted_gross_usd=float(gross[chosen]))


def basket_bounds(members,minimum=5):
    """Logical occurrence and peak-value bounds across all retained alternatives."""
    # Necessary-price proof comes before expensive cumulative depth construction.
    best_case_cost=sum(min(b.possible_best('SELL'))/10000 for b in members if b.available('SELL'))
    if best_case_cost>=1-1e-12:
        return dict(status='blocked',reason='known_legs_cost_at_least_payout',lower=0.,upper=0.,candidate=None)
    lower=[b.ladder('SELL','lower') for b in members]
    upper=[b.ladder('SELL','upper') for b in members]
    # Every known leg supplies a necessary condition, even if other legs are
    # unknown. Other unknown legs contribute zero to the best-case cost bound.
    if any(a is not None and (not len(a) or a.cumulative_size[-1]<minimum-1e-9) for a in upper):
        return dict(status='blocked',reason='insufficient_possible_depth',lower=0.,upper=0.,candidate=None)
    best_cost=sum(a.prices[0] for a in upper if a is not None and len(a))
    if best_cost>=1-1e-12:
        return dict(status='blocked',reason='known_legs_cost_at_least_payout',lower=0.,upper=0.,candidate=None)
    candidate=optimise_asks(lower,[b.tick_options for b in members],minimum)
    if all(a is not None for a in upper):
        # Least restrictive tick is an optimistic bound, not a selected state.
        potential=optimise_asks(upper,[(.0001,) for b in members],minimum)
        upper_value=potential['quoted_gross_usd'] if potential else 0.
    else:upper_value=None  # Unknown/unbounded potential: never turn into zero.
    lower_value=candidate['quoted_gross_usd'] if candidate else 0.
    if upper_value is not None:upper_value=max(upper_value,lower_value)
    status='positive' if candidate else 'blocked' if upper_value==0 else 'unknown'
    return dict(status=status,reason='all_alternatives_unprofitable' if status=='blocked' else '',
        lower=lower_value,upper=upper_value,candidate=candidate)


def arrival_bounds(signal,members):
    """Fixed quantity/limits: robust support, impossible support, or unknown."""
    if len(members)!=len(signal['limits']) or len(members)<2:raise ValueError('Missing basket leg')
    lower=[b.ladder('SELL','lower') for b in members]
    upper=[b.ladder('SELL','upper') for b in members]
    q=signal['quantity']
    guaranteed=[a.quote(q,p) if a is not None and len(a) else None for a,p in zip(lower,signal['limits'])]
    if all(v is not None for v in guaranteed):
        return dict(status='display_supported',conditional_gross_usd=q-sum(v[0] for v in guaranteed))
    impossible=any(a is not None and (not len(a) or a.quote(q,p) is None) for a,p in zip(upper,signal['limits']))
    return dict(status='insufficient_displayed_depth' if impossible else 'unknown',conditional_gross_usd=None)
