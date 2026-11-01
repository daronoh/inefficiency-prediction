"""Explain unknown PMXT event-hours without changing reconstruction or labels.

The audit replays the frozen v3 policy and attributes unknown intervals to ask
side evidence. Reasons can overlap; primary reasons partition unknown time.
It never repairs quotes, selects profitable orderings, or fits a model.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import isolated, write_json
from src.data.pmxt_books import milliseconds, price_key
from src.data.pmxt_pilot import DEFAULT_ROOT, load_plan
from src.data.pmxt_revalidation import BoundedEvent
from src.data.pmxt_source import QualityMask, source_batches
from src.data.pmxt_uncertainty import BoundedBook, SIDES
from src.data.pmxt_validation import read_prefix


REASONS = ('missing_initial_snapshot', 'retrospective_best_price_drift',
    'best_price_contradiction', 'crossed_final_group', 'malformed_group',
    'unresolved_quantity_alternatives', 'other_unbounded_basket')


class AuditBook(BoundedBook):
    """Observe the unchanged reconstruction policy; diagnostics do not alter it."""
    def __post_init__(self):
        self.fault_reason = {s: None for s in SIDES}
        self.examples = []
        self.example_counts = Counter()
        self.current_rows = []

    def _issue(self, source, reason, side=None, *, drift=False):
        for s in ([side] if side else SIDES):
            self.fault_reason[s] = reason
        key = reason, side
        if self.example_counts[key] < 2:
            detail = dict(source_ms=source, reason=reason, side=side, drift_lookback=drift,
                snapshot_anchor_ms=self.baseline, group_rows=len(self.current_rows),
                group_types=dict(Counter(r['event_type'] for r in self.current_rows)),
                reconstructed_possible_best={s: sorted(self.possible_best(s)) for s in SIDES},
                reported_best={s: sorted({price_key(r[col]) for r in self.current_rows
                    if not pd.isna(r.get(col))}) for s, col in [('BUY','best_bid'),('SELL','best_ask')]})
            self.examples.append(detail)
            self.example_counts[key] += 1
        return super()._issue(source, reason, side, drift=drift)

    def apply_batch(self, stamp, rows):
        self.current_rows = rows
        if any(r['event_type'] == 'book' for r in rows):
            self.fault_reason = {s: None for s in SIDES}
        super().apply_batch(stamp, rows)
        self.current_rows = []


def ask_reason(book):
    if book.baseline is None:
        return 'missing_initial_snapshot'
    if book.past_blocked['SELL']:
        return 'retrospective_best_price_drift'
    if book.faults['SELL']:
        return book.fault_reason['SELL'] or 'other_unbounded_basket'
    if book.alternatives['SELL']:
        return 'unresolved_quantity_alternatives'
    return None


def explain_state(state, books):
    """Only tokens of unresolved baskets contribute opportunity uncertainty."""
    reasons = set()
    tokens = set()
    unknown_packages = 0
    if state.status == 'unknown':
        for package in state.packages:
            if state.bounds[package['package_id']]['status'] != 'unknown':
                continue
            unknown_packages += 1
            for token in package['token_ids']:
                reason = ask_reason(books[token])
                if reason:
                    reasons.add(reason)
                    tokens.add(token)
        if not reasons:
            reasons.add('other_unbounded_basket')
    primary = next((r for r in REASONS if r in reasons), None)
    return reasons, primary, tokens, unknown_packages


def audit(root=DEFAULT_ROOT, *, run='source_validation_v3'):
    root, plan = load_plan(root)
    derived = isolated(root/run)
    summary_path = PROJECT_ROOT/'results/pmxt_pilot'/root.name/run/'summary.json'
    summary = json.loads(summary_path.read_text())
    # Refuse stale derived evidence. The audit must explain exactly this run.
    for name, digest in summary['derived_sha256'].items():
        if sha256(derived/name) != digest:
            raise ValueError('Replay evidence changed before unknown-hour audit')
    frame, inputs = read_prefix(root, plan)
    if inputs != summary['inputs']:
        raise ValueError('Raw input context differs from the completed replay')
    start, end = pd.Timestamp(summary['source_start']), pd.Timestamp(summary['source_end'])
    capture = pd.Timestamp(plan['warmup_start'])
    raw_masks = json.loads((derived/'side_drift_intervals.json').read_text())
    books = {t: AuditBook(milliseconds(capture)) for t in plan['assets']}
    # BoundedBook's dataclass initializer invokes the subclass post-init only
    # when the base defines it; initialize diagnostic fields explicitly.
    for book in books.values():
        book.__post_init__()
    masks = {(t,s): QualityMask(raw_masks.get(f'{t}:{s}', [])) for t in books for s in SIDES}
    events = pd.read_parquet(root/'events.parquet').to_dict('records')
    packages = pd.read_parquet(root/'packages.parquet').to_dict('records')
    states = {e['event_id']: BoundedEvent(e, [p for p in packages if p['event_id']==e['event_id']], books) for e in events}
    by_token = defaultdict(set)
    for eid, state in states.items():
        for token in state.tokens:
            by_token[token].add(eid)
    explanations = {eid: explain_state(state, books) for eid, state in states.items()}
    cuts = {milliseconds(t) for t in pd.date_range(capture,end,freq='h')}
    for mask in masks.values():
        cuts.update(x for pair in mask.intervals for x in pair if milliseconds(capture)<=x<=milliseconds(end))
    cuts = iter(sorted(cuts))
    next_cut = next(cuts, None)
    previous = capture
    hour_reasons = defaultdict(Counter)
    primary_reasons = defaultdict(Counter)
    token_reasons = defaultdict(Counter)
    unknown_seconds = Counter()
    unknown_package_seconds = Counter()
    timing = defaultdict(Counter)

    def recompute(changed):
        for eid in {eid for t in changed for eid in by_token[t]}:
            states[eid].recompute(changed)
            explanations[eid] = explain_state(states[eid], books)

    def integrate(target):
        nonlocal previous
        if target <= previous:
            return
        hour = previous.floor('h')
        if start <= hour < end:
            seconds = (target-previous).total_seconds()
            for eid, state in states.items():
                key = eid, hour
                timing[key]['observed_seconds'] += seconds*state.full_books
                timing[key]['ask_seconds'] += seconds*state.full_asks
                timing[key]['positive_seconds'] += seconds*(state.status=='positive')
                timing[key]['blocked_seconds'] += seconds*(state.status=='blocked')
                reasons, primary, tokens, count = explanations[eid]
                if primary:
                    unknown_seconds[key] += seconds
                    primary_reasons[key][primary] += seconds
                    unknown_package_seconds[key] += seconds*count
                    for reason in reasons:
                        hour_reasons[key][reason] += seconds
                    for token in tokens:
                        token_reasons[eid,token][ask_reason(books[token])] += seconds
        previous = target

    def move(target):
        nonlocal next_cut
        while next_cut is not None and next_cut <= milliseconds(target):
            stamp = pd.Timestamp(next_cut, unit='ms', tz='UTC')
            integrate(stamp)
            changed = set()
            for (token,side), mask in masks.items():
                value = mask.contains(next_cut)
                if books[token].past_blocked[side] != value:
                    books[token].past_blocked[side] = value
                    changed.add(token)
            recompute(changed)
            next_cut = next(cuts,None)
        integrate(target)

    def process(stamp, batches):
        move(stamp)
        changed = set()
        for (_,token), rows in batches:
            books[token].apply_batch(stamp, rows)
            for side in SIDES:
                books[token].past_blocked[side] = masks[token,side].contains(milliseconds(stamp))
            changed.add(token)
        recompute(changed)

    batch = []
    stamp = None
    work = frame.loc[(frame.timestamp>=capture)&(frame.timestamp<end)]
    groups = 0
    for key, rows in source_batches(work):
        if stamp is not None and key[0] != stamp:
            process(stamp,batch)
            batch = []
        stamp = key[0]
        batch.append((key,rows))
        groups += 1
        if groups%200_000 == 0:
            print(f'Unknown-hour audit: {groups:,} source groups', flush=True)
    if batch:
        process(stamp,batch)
    move(end)
    labels = pd.read_parquet(derived/'hourly_label_bounds.parquet')
    rows = []
    for label in labels.to_dict('records'):
        key = label['event_id'],label['hour']
        difference = abs(unknown_seconds[key]-(3600-label['occurrence_determined_seconds']))
        if difference > 1e-6 or abs(timing[key]['blocked_seconds']-label['ruled_out_seconds']) > 1e-6:
            raise AssertionError(f'Audit and frozen v3 replay disagree for {key}: {difference}')
        row = dict(label, unknown_seconds=unknown_seconds[key],
            unknown_package_seconds=unknown_package_seconds[key],
            main_unknown_reason=primary_reasons[key].most_common(1)[0][0] if primary_reasons[key] else None)
        for reason in REASONS:
            row[f'any_{reason}_seconds'] = hour_reasons[key][reason]
            row[f'primary_{reason}_seconds'] = primary_reasons[key][reason]
        rows.append(row)
    audited = pd.DataFrame(rows).merge(pd.DataFrame(events)[['event_id','event_slug','classification']], on='event_id', validate='many_to_one')
    output = isolated(PROJECT_ROOT/'results/pmxt_pilot'/root.name/'unknown_audit_v1','results')
    output.mkdir(parents=True,exist_ok=True)
    audited.to_csv(output/'event_hours.csv', index=False)
    token_frame = pd.DataFrame([dict(event_id=eid, asset_id=t, reason=r, unknown_exposure_seconds=v)
        for (eid,t),counts in token_reasons.items() for r,v in counts.items()])
    token_frame.to_csv(output/'token_reasons.csv',index=False)
    write_json(output/'examples.json',{t:b.examples for t,b in books.items() if b.examples})
    result = dict(event_hours=len(audited),unknown_labels=int(audited.label_occurrence.isna().sum()),
        unknown_seconds=float(audited.unknown_seconds.sum()),
        primary_reason_seconds={r:float(audited[f'primary_{r}_seconds'].sum()) for r in REASONS},
        overlapping_reason_seconds={r:float(audited[f'any_{r}_seconds'].sum()) for r in REASONS},
        caveat='Primary causes partition unknown time; any-cause exposure overlaps. Counters are not independent outages or proof of archive loss.',
        replay_summary_sha256=sha256(summary_path),
        implementation_sha256={str(Path(__file__).relative_to(PROJECT_ROOT)):sha256(Path(__file__))},
        results_sha256={p.name:sha256(p) for p in output.iterdir() if p.is_file() and p.name!='summary.json'})
    write_json(output/'summary.json',result)
    print(json.dumps(result,indent=2),flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=DEFAULT_ROOT)
    args = parser.parse_args()
    audit(args.root)


if __name__ == '__main__':
    main()
