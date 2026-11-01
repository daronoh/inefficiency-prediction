"""Local-only audit of PMXT update bursts and independent full-book checkpoints.

No trade changes depth, no reported best price creates/deletes a level, and no
later observation repairs an earlier state. This diagnostic changes no labels.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.data.pmxt_archive import isolated, write_json
from src.data.pmxt_continuous import ROOT, CONTINUOUS
from src.data.pmxt_source import source_batches
from src.data.pmxt_validation import read_prefix
from src.paths import PROJECT_ROOT

SIDES = ('BUY', 'SELL')
BUFFERS_MS = (0, 10, 50, 100, 250, 1000)


def decimal(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('Nonfinite depth')
    return result


def parse_levels(raw):
    result = {}
    for price, size in json.loads(raw):
        p, q = decimal(price), decimal(size)
        if not 0 <= p <= 1 or q < 0 or p in result:
            raise ValueError('Invalid or duplicate snapshot level')
        if q:
            result[p] = q
    return result


def top(depth, side):
    return (max(depth) if side == 'BUY' else min(depth)) if depth else None


@dataclass
class DiagnosticBook:
    """Independent Decimal replay; ambiguity persists until a clean full book."""
    depth: dict = field(default_factory=lambda: {s: {} for s in SIDES})
    quotes: dict = field(default_factory=lambda: {s: None for s in SIDES})
    ambiguous: dict = field(default_factory=lambda: {s: False for s in SIDES})
    initialized: bool = False
    anchor: int | None = None
    pending: dict = field(default_factory=dict)
    episodes: list = field(default_factory=list)
    checkpoints: list = field(default_factory=list)
    counters: Counter = field(default_factory=Counter)

    def compatible(self, side):
        return self.initialized and not self.ambiguous[side] and (
            self.quotes[side] is None or top(self.depth[side], side) == self.quotes[side])

    def apply(self, ms, rows):
        changes, quotes, snapshots = {}, {s: set() for s in SIDES}, []
        for row in rows:
            kind = row['event_type']
            self.counters[kind] += 1
            if kind == 'book':
                snapshots.append({'BUY': parse_levels(row['bids']), 'SELL': parse_levels(row['asks'])})
            elif kind == 'price_change':
                side = row['side']
                if side not in SIDES:
                    raise ValueError('Unknown side')
                p, q = decimal(row['price']), decimal(row['size'])
                if not 0 <= p <= 1 or q < 0:
                    raise ValueError('Invalid level update')
                changes.setdefault((side, p), set()).add(q)
                for s, column in [('BUY', 'best_bid'), ('SELL', 'best_ask')]:
                    if pd.notna(row.get(column)):
                        quotes[s].add(decimal(row[column]))
            elif kind not in ('last_trade_price', 'tick_size_change'):
                raise ValueError('Unknown message type')
        changed = set()
        for (side, p), sizes in changes.items():
            if len(sizes) != 1:
                self.ambiguous[side] = True
                continue
            q = next(iter(sizes))
            if self.depth[side].get(p, Decimal(0)) != q:
                changed.add(side)
            if q:
                self.depth[side][p] = q
            else:
                self.depth[side].pop(p, None)
        for side in SIDES:
            if len(quotes[side]) > 1:
                self.ambiguous[side] = True
            elif quotes[side]:
                self.quotes[side] = next(iter(quotes[side]))
        signatures = {tuple((s, tuple(sorted(b[s].items()))) for s in SIDES): b for b in snapshots}
        clean_snapshot = len(signatures) == 1
        if clean_snapshot:
            snapshot = next(iter(signatures.values()))
            if self.initialized:
                for side in SIDES:
                    old, fresh = self.depth[side], snapshot[side]
                    a, b = top(old, side), top(fresh, side)
                    differences = [p for p in old.keys() | fresh.keys() if old.get(p, 0) != fresh.get(p, 0)]
                    touched = {p for s, p in changes if s == side}
                    self.checkpoints.append(dict(source_ms=ms, side=side, previous_snapshot_ms=self.anchor,
                        prior_ambiguity=self.ambiguous[side], same_time_level_updates=any(s == side for s, _ in changes),
                        full_depth_match=not differences, top_price_match=a == b,
                        top_price_and_quantity_match=a == b and (a is None or old[a] == fresh[b]),
                        reconstructed_best=str(a), snapshot_best=str(b), different_levels=len(differences),
                        untouched_level_differences=len(set(differences)-touched),
                        untouched_depth_match=not (set(differences)-touched)))
            self.depth = {s: dict(snapshot[s]) for s in SIDES}
            self.initialized, self.anchor = True, ms
            self.ambiguous = {s: False for s in SIDES}
            # An authoritative snapshot closes only compatible same-time updates.
            for (side, p), sizes in changes.items():
                if snapshot[side].get(p, Decimal(0)) not in sizes:
                    self.ambiguous[side] = True
            self.quotes = {s: top(snapshot[s], s) for s in SIDES}
            for side in SIDES:
                if quotes[side] and (len(quotes[side]) != 1 or self.quotes[side] not in quotes[side]):
                    self.ambiguous[side] = True
        elif snapshots:
            self.ambiguous = {s: True for s in SIDES}
        for side in SIDES:
            episode = self.pending.get(side)
            if episode:
                episode['encountered_ambiguity'] |= self.ambiguous[side]
                episode['encountered_snapshot'] |= bool(snapshots)
                episode['intervening_level_change'] |= side in changed
                if self.compatible(side):
                    if clean_snapshot:
                        mode = 'full_snapshot'
                    elif side in changed:
                        mode = 'level_update_original_quote' if top(self.depth[side], side) == episode['original_quote'] else 'level_update_changed_quote'
                    else:
                        mode = 'quote_revision_without_depth_repair'
                    self._close(side, ms, mode)
            if side not in self.pending and self.initialized and not self.ambiguous[side] and not self.compatible(side):
                self.pending[side] = dict(start_ms=ms, side=side, snapshot_ms=self.anchor,
                    original_quote=self.quotes[side], original_reconstructed=top(self.depth[side], side),
                    encountered_ambiguity=False, encountered_snapshot=False, intervening_level_change=False)

    def _close(self, side, ms, mode):
        episode = self.pending.pop(side)
        episode.update(end_ms=ms, duration_ms=ms-episode['start_ms'], resolution=mode)
        self.episodes.append(episode)

    def finish(self, end_ms):
        for side in list(self.pending):
            self._close(side, end_ms, 'right_censored')


def replay(frame, assets, packages, capture, start, end):
    """Coverage is causal consistency only; it is not certified label coverage."""
    books = {t: DiagnosticBook() for t in assets}
    lo, hi = start.value // 1_000_000, end.value // 1_000_000
    previous = lo
    intervals = []
    for (stamp, token), rows in source_batches(frame.loc[frame.timestamp.ge(capture) & frame.timestamp.lt(end)]):
        ms = stamp.value // 1_000_000
        if ms > previous:
            for package in packages:
                members = [books[str(t)] for t in package['token_ids']]
                intervals.append(dict(package_id=package['package_id'], start_ms=previous, end_ms=ms,
                    ask_consistent=all(b.compatible('SELL') for b in members),
                    both_sides_consistent=all(b.compatible(s) for b in members for s in SIDES)))
            previous = ms
        books[token].apply(ms, rows)
    if previous < hi:
        for package in packages:
            members = [books[str(t)] for t in package['token_ids']]
            intervals.append(dict(package_id=package['package_id'], start_ms=previous, end_ms=hi,
                ask_consistent=all(b.compatible('SELL') for b in members),
                both_sides_consistent=all(b.compatible(s) for b in members for s in SIDES)))
    episodes, checkpoints = [], []
    for token, book in books.items():
        book.finish(hi)
        episodes.extend(dict(asset_id=token, **e) for e in book.episodes if e['start_ms'] < hi and e['end_ms'] > lo)
        checkpoints.extend(dict(asset_id=token, **c) for c in book.checkpoints if lo <= c['source_ms'] < hi)
    return pd.DataFrame(episodes), pd.DataFrame(checkpoints), pd.DataFrame(intervals)


def run(root=ROOT):
    root = Path(root)
    folder = root / CONTINUOUS
    output = isolated(PROJECT_ROOT/'results/pmxt_pilot/reconciliation_v1', 'results')
    output.mkdir(parents=True, exist_ok=True)
    plan_path = folder/'plan.json'
    plan = json.loads(plan_path.read_text())
    design_path = root/'study_design.json'
    original = json.loads(design_path.read_text())
    design = dict(version=1, analysis_start=original['start'], analysis_end=original['analysis_end'],
        buffers_ms=BUFFERS_MS, scope=original['scope'],
        purpose='Test whether ordinary depth updates repair disagreements at their original reported quote, and compare with independent later full books.',
        state_policy='Absolute level updates; no trade subtraction, quote pruning or label changes. Conflicting quantities quarantine a side until a compatible full snapshot.',
        checkpoint_policy='Compare before replacement, after unambiguous same-source updates. Report ties and prior ambiguity separately.',
        decision='A simple burst-only explanation fails in this subset if original-quote depth repairs within 1000ms do not explain every analysis-started disagreement, or unambiguous ask checkpoints disagree at levels not updated at that timestamp.',
        retrospective_audit_only=True, models_trained=False, labels_changed=False,
        source_sha256={p: sha256(PROJECT_ROOT/p) for p in ['src/data/pmxt_reconciliation.py','src/data/pmxt_source.py','src/data/pmxt_validation.py']},
        parent_design_sha256=sha256(design_path), plan_sha256=sha256(plan_path))
    write_json(output/'design.json', design)
    frame, inputs = read_prefix(folder, plan)
    expected = len(pd.date_range(plan['warmup_start'], plan['end'], freq='h', inclusive='left'))
    if len(inputs) != expected:
        raise ValueError('Incomplete contiguous input history')
    packages = pd.read_parquet(folder/'packages.parquet').to_dict('records')
    start, end = pd.Timestamp(design['analysis_start']), pd.Timestamp(design['analysis_end'])
    episodes, checkpoints, intervals = replay(frame, plan['assets'], packages, pd.Timestamp(plan['warmup_start']), start, end)
    lo = start.value // 1_000_000
    episodes['starts_in_analysis'] = episodes.start_ms.ge(lo)
    episodes['original_quote'] = episodes.original_quote.map(str)
    episodes['original_reconstructed'] = episodes.original_reconstructed.map(str)
    # Trades are timing evidence only; never inferred order deletions.
    trades = frame.loc[frame.event_type.eq('last_trade_price')]
    episodes['trade_within_100ms'] = [bool((trades.asset_id.eq(e.asset_id) &
        (trades.timestamp.map(lambda t: t.value//1_000_000)-e.start_ms).abs().le(100)).any()) for e in episodes.itertuples()]
    episodes.to_csv(output/'disagreement_episodes.csv', index=False)
    checkpoints.to_csv(output/'full_book_checkpoints.csv', index=False)
    intervals.to_parquet(output/'causal_consistency_intervals.parquet', index=False)
    rows = []
    current = episodes.loc[episodes.starts_in_analysis]
    genuine = current.resolution.eq('level_update_original_quote') & ~current.encountered_ambiguity & ~current.encountered_snapshot
    for side in SIDES:
        subset = current.loc[current.side.eq(side)]
        for delay in BUFFERS_MS:
            count = int((genuine & current.side.eq(side) & current.duration_ms.le(delay)).sum())
            rows.append(dict(side=side, buffer_ms=delay, analysis_started_episodes=len(subset),
                original_quote_depth_repairs=count, unresolved_or_not_depth_repair=len(subset)-count,
                repaired_fraction=count/len(subset) if len(subset) else None))
    pd.DataFrame(rows).to_csv(output/'burst_recovery.csv', index=False)
    coverage = []
    for key, part in intervals.groupby('package_id'):
        duration = part.end_ms-part.start_ms
        coverage.append(dict(package_id=key, seconds=duration.sum()/1000,
            causal_ask_consistent_fraction=float((duration*part.ask_consistent).sum()/duration.sum()),
            causal_both_sides_consistent_fraction=float((duration*part.both_sides_consistent).sum()/duration.sum())))
    pd.DataFrame(coverage).to_csv(output/'coverage.csv', index=False)
    checkpoint_summary = []
    for side, part in checkpoints.groupby('side'):
        for population, selected in [('all', part), ('unambiguous', part.loc[~part.prior_ambiguity]),
                ('unambiguous_no_same_time_updates', part.loc[~part.prior_ambiguity & ~part.same_time_level_updates])]:
            checkpoint_summary.append(dict(side=side, population=population, checkpoints=len(selected),
                full_depth_matches=int(selected.full_depth_match.sum()), top_price_matches=int(selected.top_price_match.sum()),
                top_price_and_quantity_matches=int(selected.top_price_and_quantity_match.sum()),
                untouched_depth_matches=int(selected.untouched_depth_match.sum())))
    pd.DataFrame(checkpoint_summary).to_csv(output/'checkpoint_summary.csv', index=False)
    unexplained = current.loc[~(genuine & current.duration_ms.le(1000))]
    asks = checkpoints.loc[checkpoints.side.eq('SELL') & ~checkpoints.prior_ambiguity]
    summary = dict(inputs=inputs, packages_sha256=sha256(folder/'packages.parquet'),
        analysis_started_episodes=len(current), overlapping_episodes=len(episodes),
        resolutions=current.resolution.value_counts().to_dict(),
        original_quote_repairs_within_1000ms=int((genuine & current.duration_ms.le(1000)).sum()),
        unexplained_by_simple_burst=len(unexplained), analysis_started_ask_episodes=int(current.side.eq('SELL').sum()),
        unambiguous_ask_checkpoints=len(asks), unambiguous_ask_untouched_depth_mismatches=int((~asks.untouched_depth_match).sum()),
        simple_burst_explanation_supported=bool(len(current) and unexplained.empty and len(asks) and asks.untouched_depth_match.all()),
        labels_changed=False, models_trained=False, pnl_estimated=False,
        design_sha256=sha256(output/'design.json'),
        interpretation='Diagnostic sample selected for missing seeds, not representative provider-wide evidence. Matching best prices alone does not certify depth.',
        results_sha256={p.name: sha256(p) for p in output.iterdir() if p.is_file() and p.name not in ('summary.json', 'design.json')})
    write_json(output/'summary.json', summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('inputs','results_sha256')}, indent=2))
    print(pd.DataFrame(checkpoint_summary).to_string(index=False))
    print(pd.DataFrame(coverage).to_string(index=False))
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    run(parser.parse_args().root)
