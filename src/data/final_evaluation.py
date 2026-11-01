"""Explicit final-only features and labels under the frozen protocol."""
import argparse
from collections import OrderedDict
from dataclasses import asdict
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.common import sha256
from src.config import SPEC, ScreeningSpec
from src.data.books import RawGroup, Ladder, RawBook
from src.data.coverage import classify_book, observation_fraction
from src.data.metadata import add_metadata
from src.data.opportunities import Qualification, label_windows
from src.data.profitability import quoted_net
from src.data.snapshots import add_hourly_history
from src.data.universe import build_plan
from src.paths import PROJECT_ROOT, PROCESSED_DIR, EVENT_UNIVERSE, data_relative
from src.reporting.final_protocol import verify as verify_protocol
from src.simulator.capacity import screen_token
from src.simulator.discovery import split_monitoring_hours
from src.simulator.sizing import choose_order


class FinalGroup(RawGroup):
    """Backward reader with explicit context and per-member recording ends.

    Actual endpoints come from the timestamp audit, not the final row in the
    bounded price projection. No future book is read to seed an earlier state.
    """
    def __init__(self, frames, *, context_end):
        if not frames:
            raise ValueError('A group needs members')
        self.context_end = pd.Timestamp(context_end)
        if self.context_end.tzinfo is None:
            raise ValueError('UTC-aware context boundary required')
        self.frames, self.times, self.recording_ends, self.paths = [], [], [], []
        self._cache = OrderedDict()
        for original in frames:
            frame = original.loc[:,self.COLUMNS].copy()
            frame['snapshot_time'] = pd.to_datetime(frame.snapshot_time, utc=True)
            if frame.snapshot_time.isna().any() or frame.snapshot_time.gt(self.context_end).any():
                raise ValueError('Invalid timestamp or book beyond declared exit context')
            frame['indexed_at_ms'] = pd.to_numeric(frame.indexed_at_ms, errors='raise')
            frame = frame.sort_values(['snapshot_time','indexed_at_ms'],kind='stable',na_position='first').drop_duplicates('snapshot_time',keep='last').reset_index(drop=True)
            self.frames.append(frame)
            self.times.append(pd.DatetimeIndex(frame.snapshot_time))
            self.recording_ends.append(pd.Timestamp(original.attrs.get('recording_end',frame.snapshot_time.max())))
        self.last_time = min(self.context_end, max((t for t in self.recording_ends if pd.notna(t)),default=self.context_end))

    @classmethod
    def from_members(cls, group):
        raise ValueError('Final reader requires explicitly bounded frames and context')

    def at(self, when):
        when = pd.Timestamp(when)
        if when > self.context_end or any(pd.isna(end) or when > end for end in self.recording_ends):
            return None
        return super().at(when)


def load_final_frames(plan, protocol, inventory):
    """One as-of seed per token plus bounded current/exit observations."""
    start = pd.Timestamp(protocol['period']['start'])-pd.Timedelta(hours=1)
    end = pd.Timestamp(protocol['period']['end_exclusive'])+pd.Timedelta(seconds=1.1)
    frames = {}
    for token,path in plan['paths'].items():
        stamps = pd.read_parquet(path,columns=['snapshot_time'],filters=[('snapshot_time','<',start)]).snapshot_time
        seed = stamps.max() if len(stamps) else start
        frame = pd.read_parquet(path,columns=RawGroup.COLUMNS,filters=[('snapshot_time','>=',seed),('snapshot_time','<=',end)])
        frame.attrs['recording_end'] = pd.Timestamp(inventory.loc[token,'last'])
        frames[token] = frame
    return frames


def final_plans(protocol):
    plans,_ = build_plan('all')
    if {p['event_slug'] for p in plans} != set(protocol['universe']['events']):
        raise ValueError('Frozen supported universe differs')
    end = pd.Timestamp(protocol['period']['end_exclusive'])
    result = []
    for original in plans:
        plan = dict(original)
        if plan['stratum'] == 'negrisk':
            plan['packages'] = [(ids,a,end) for ids,a,_ in plan['packages']]
        result.append(plan)
    return result


def sample_features(plan, frames, windows, context_end):
    readers = {t:FinalGroup([f],context_end=context_end) for t,f in frames.items()}
    rows = []
    for when in windows:
        packages = [ids for ids,a,b in plan['packages'] if a <= when < b]
        if not packages:
            continue
        ids = sorted({t for p in packages for t in p})
        seen, clean, ages, spreads, depth, books = [], [], [], [], [], {}
        for token in ids:
            reader = readers[token]
            position = reader.times[0].searchsorted(when,side='right')-1
            if position < 0:
                seen.append(0.); clean.append(np.nan); continue
            book = reader._book(0,int(position))
            books[token] = book
            seen.append(1.);clean.append(float(book.clean));ages.append((when-book.stamp).total_seconds())
            if book.clean:
                spreads.append(book.asks.prices[0]-book.bids.prices[0])
            depth.append(book.asks.sizes.sum())
        values = np.asarray([quoted_net([books.get(t) for t in p]) for p in packages])
        rows.append(dict(event_slug=plan['event_slug'],stratum=plan['stratum'],time=when,
            eligible_snapshot=all(seen),x_active_packages=len(packages),x_active_tokens=len(ids),
            x_seen_fraction=np.mean(seen),x_clean_fraction=float(pd.Series(clean).eq(1).mean()),
            x_spread_mean=np.mean(spreads) if spreads else np.nan,x_spread_max=max(spreads) if spreads else np.nan,
            x_ask_depth_sum=float(sum(depth)) if depth else np.nan,x_book_age_max_s=max(ages) if ages else np.nan,
            x_best_quoted_net_usd=float(np.nanmax(values)) if np.isfinite(values).any() else np.nan,
            x_size_feasible_package_fraction=float(np.isfinite(values).mean()),
            x_positive_quoted_net_package_fraction=float((values>0).mean())))
    return pd.DataFrame(rows)


SIGNAL_COLUMNS = ['pair_id','event_slug','start_time','end_time','episode_id','window',
    'monitoring_end_time','start_observed','end_observed','original_spell_start','time','boundary_continuation']


def discover_final(plan, frames, spec, context_end):
    """The frozen integer-depth discovery rule with explicit final boundaries."""
    screens = {t:screen_token(f) for t,f in frames.items()}
    spells = []
    for number,(ids,a,b) in enumerate(plan['packages']):
        start,end = max(a,pd.Timestamp(spec.development_start)),min(b,pd.Timestamp(spec.lockbox_start))
        if start >= end or any(screens[t].empty for t in ids):
            continue
        stop = min(end,*(frames[t].attrs['recording_end'] for t in ids))
        if stop <= start:
            continue
        grid = pd.DatetimeIndex([start,stop])
        for t in ids:
            times = screens[t].index
            grid = grid.union(times[(times>start)&(times<stop)])
        grid = grid.sort_values()
        best,observed = np.zeros(len(grid)),np.ones(len(grid),dtype=bool)
        for t in ids:
            table = screens[t].reindex(grid,method='pad')
            best += table.best_ask.to_numpy()
            observed &= table.observed.eq(True).to_numpy()
        good = np.zeros(len(grid),dtype=bool)
        group = FinalGroup([frames[t] for t in ids],context_end=context_end)
        for i in np.flatnonzero(best<1):
            if i == len(grid)-1:
                continue
            books = group.at(grid[i])
            good[i] = books is not None and choose_order(books,min_quantity=5,max_quantity=None,
                cash_cap=None,min_limit_margin=0.) is not None
        begins = np.flatnonzero(good & ~np.r_[False,good[:-1]])
        ends = np.flatnonzero(~good & np.r_[False,good[:-1]])
        pid = f"{plan['event_slug']}:{number}"
        for i,j in zip(begins,ends,strict=True):
            spells.append(dict(pair_id=pid,event_slug=plan['event_slug'],start_time=grid[i],end_time=grid[j],
                episode_id=f'{pid}:{grid[i].value}',window='final',monitoring_end_time=end,
                start_observed=bool(i>0 and observed[i-1]),end_observed=bool(grid[j]<stop and observed[j])))
    return pd.DataFrame(split_monitoring_hours(spells,spec),columns=SIGNAL_COLUMNS)


def quote_labels(plan, frames, windows, spec, context_end):
    states = {}
    for t,frame in frames.items():
        values = [classify_book(r.asks_json,r.bids_json,r.min_order_size,r.tick_size,quantities=None) for r in frame.itertuples(index=False)]
        state = pd.DataFrame(values,columns=['size_qualified','state','reason']).assign(time=frame.snapshot_time.to_numpy())
        # Coverage endpoint only: carry the last observed state to the audited
        # recording end/context boundary. This is not a synthetic book update.
        stop = min(frame.attrs['recording_end'],context_end)
        if len(state) and state.time.iloc[-1] < stop:
            state = pd.concat([state,state.iloc[[-1]].assign(time=stop)],ignore_index=True)
        states[t] = state
    coverage = observation_fraction(plan,states,pd.DatetimeIndex(windows),spec)
    signals = discover_final(plan,frames,spec,context_end)
    signals = signals.loc[signals.time.isin(windows)].reset_index(drop=True)
    labels = label_windows(windows,signals,coverage,spec)
    peaks = []
    for pair,part in signals.groupby('pair_id'):
        ids = plan['packages'][int(pair.rsplit(':',1)[1])][0]
        group = FinalGroup([frames[t] for t in ids],context_end=context_end)
        updates = pd.DatetimeIndex([],tz='UTC')
        for t in ids:
            updates = updates.union(pd.DatetimeIndex(frames[t].snapshot_time))
        for when,spells in part.groupby('time'):
            best,peak,quantity,checked = 0.,pd.NaT,0,0
            for spell in spells.itertuples():
                grid = pd.DatetimeIndex([spell.start_time]).union(updates[(updates>spell.start_time)&(updates<spell.end_time)])
                for stamp in grid:
                    books = group.at(stamp)
                    order = None if books is None else choose_order(books,min_quantity=5,max_quantity=None,cash_cap=None,min_limit_margin=0.)
                    if order is None:
                        raise ValueError('Qualified interval contains an unqualified quote')
                    checked += 1
                    if order['quoted_gross_usd'] > best:
                        best,peak,quantity = order['quoted_gross_usd'],stamp,order['quantity']
            peaks.append(dict(time=when,pair_id=pair,peak_quoted_usd=best,peak_time=peak,quantity=quantity,states_checked=checked))
    peaks = pd.DataFrame(peaks,columns=['time','pair_id','peak_quoted_usd','peak_time','quantity','states_checked'])
    values = peaks.groupby('time').peak_quoted_usd.max() if len(peaks) else pd.Series(dtype=float)
    labels['witnessed_peak_quoted_usd'] = labels.time.map(values).fillna(0.)
    labels['value_target'] = labels.witnessed_peak_quoted_usd.where(labels.label_known)
    if not labels.value_target.gt(0).equals(labels.target.eq(1)):
        raise ValueError('Occurrence and peak quote labels disagree')
    return labels,signals,peaks


def build(output, *, open_final=False, protocol_dir=None, audit_dir=None):
    if not open_final:
        raise ValueError('Final outcomes require explicit --open-final access')
    output = Path(output)
    protocol_dir = Path(protocol_dir or PROJECT_ROOT/'results/final_protocol')
    audit_dir = Path(audit_dir or PROJECT_ROOT/'results/final_coverage')
    protocol = verify_protocol(protocol_dir,audit_dir)
    start,end = pd.Timestamp(protocol['period']['start']),pd.Timestamp(protocol['period']['end_exclusive'])
    context_end = end+pd.Timedelta(seconds=1.1)
    spec = ScreeningSpec(development_start=str(start-pd.Timedelta(hours=1)),lockbox_start=str(end))
    output.mkdir(parents=True,exist_ok=False)
    (output/'events').mkdir()
    sources = ['src/data/final_evaluation.py']
    frozen = json.loads((PROJECT_ROOT/'results/final_preparation/design.json').read_text())
    source_hashes = dict(frozen['source_sha256'])
    source_hashes.update({p:sha256(PROJECT_ROOT/p) for p in sources})
    design = dict(version='final_dataset_v1',protocol_sha256=sha256(protocol_dir/'protocol.json'),
        protocol_completion_sha256=sha256(protocol_dir/'complete.json'),window=asdict(spec),
        period=protocol['period'],source_sha256=source_hashes,models_retrained=False,
        final_outcomes_opened=True,execution_outcomes_used=False,
        qualification=asdict(Qualification()),context_end=str(context_end),
        access='User authorised continuing from timestamp audit into fixed final evaluation.')
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    audit = pd.read_parquet(audit_dir/'event_windows.parquet')
    eligible = audit.loc[audit.recording_eligible].copy()
    inventory = pd.read_csv(audit_dir/'token_inventory.csv',dtype={'token_id':str}).set_index('token_id')
    expected = pd.read_csv(PROCESSED_DIR/'opportunities/input_inventory.csv',dtype={'token_id':str})
    expected = expected.set_index('token_id').input_sha256.to_dict()
    plans = [p for p in final_plans(protocol) if p['event_slug'] in set(eligible.event_slug)]
    hashes = []
    for plan in plans:
        for token,path in plan['paths'].items():
            digest = sha256(path)
            if token in expected and digest != expected[token]:
                raise ValueError('Raw file differs from frozen development inventory')
            hashes.append(dict(event_slug=plan['event_slug'],token_id=token,path=data_relative(path),
                               input_sha256=digest,verified_development_input=token in expected))
    pd.DataFrame(hashes).to_csv(output/'input_inventory.csv',index=False)
    # Persist all raw hashes before inspecting any final book/label outcomes.
    features,all_labels,all_peaks = [],[],[]
    for plan in plans:
        slug = plan['event_slug']
        print(f'Final features and observed labels: {slug}',flush=True)
        frames = load_final_frames(plan,protocol,inventory)
        frames = {t:FinalGroup([f],context_end=context_end).frames[0].copy() for t,f in frames.items()}
        for t,f in frames.items():
            f.attrs['recording_end'] = pd.Timestamp(inventory.loc[t,'last'])
        selected = eligible.loc[eligible.event_slug.eq(slug),'time'].sort_values()
        # Include the preceding scheduled state even when the first eligible
        # scan occurs days into the test: historical lags were built before
        # excluding snapshot-ineligible rows.
        times = pd.DatetimeIndex(selected).union(pd.DatetimeIndex(selected)-spec.horizon)
        first_grid = pd.to_datetime(inventory.loc[list(plan['paths']),'first'],utc=True).min().ceil(spec.frequency)
        times = times[times>=first_grid]
        f = add_hourly_history(sample_features(plan,frames,times,context_end),spec)
        f = f.loc[f.time.isin(selected)]
        if len(f) != len(selected) or not f.eligible_snapshot.all():
            raise ValueError('Final feature eligibility differs from timestamp audit')
        features.append(f)
        labels,signals,peaks = quote_labels(plan,frames,pd.DatetimeIndex(selected),spec,context_end)
        all_labels.append(labels.assign(event_slug=slug))
        all_peaks.append(peaks.assign(event_slug=slug))
        folder = output/'events'/slug
        folder.mkdir()
        signals.to_parquet(folder/'signals.parquet',index=False)
    feature = add_metadata(pd.concat(features,ignore_index=True)).sort_values(['time','event_slug']).reset_index(drop=True)
    feature.to_parquet(output/'features.parquet',index=False)
    frame = feature.merge(pd.concat(all_labels,ignore_index=True),on=['event_slug','time'],validate='one_to_one')
    frame['development_event'] = frame.event_slug.isin(set(eligible.loc[eligible.development_event,'event_slug']))
    frame.to_parquet(output/'examples.parquet',index=False)
    pd.concat(all_peaks,ignore_index=True).to_parquet(output/'package_peaks.parquet',index=False)
    # New final-only members get the same exact frozen market-flag verification.
    from src.data.rules import frozen_fee_evidence
    universe = pd.read_parquet(EVENT_UNIVERSE,columns=['event_slug','market_id'])
    frozen_fee_evidence(universe.loc[universe.event_slug.isin(frame.event_slug),'market_id'].astype(str).tolist()).to_csv(output/'fee_evidence.csv',index=False)
    summary = dict(rows=len(frame),events=frame.event_slug.nunique(),known=int(frame.label_known.sum()),
        positive=int(frame.target.eq(1).sum()),negative=int(frame.target.eq(0).sum()),unknown=int((~frame.label_known).sum()),
        witnessed_in_unknown=int((frame.witnessed_opportunity & ~frame.label_known).sum()),
        peak_quote_value=float(frame.value_target.sum()),final_outcomes_opened=True)
    (output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    files = {p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(files_sha256=files,final_outcomes_opened=True,
        models_retrained=False,execution_outcomes_used=False),indent=2)+'\n')
    return summary


def verify(output):
    output = Path(output)
    complete = json.loads((output/'complete.json').read_text())
    design = json.loads((output/'design.json').read_text())
    if not complete['final_outcomes_opened'] or complete['models_retrained']:
        raise ValueError('Expected fixed-model final evaluation')
    for name,digest in complete['files_sha256'].items():
        if sha256(output/name) != digest:
            raise ValueError(f'Final dataset artifact changed: {name}')
    for name,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/name) != digest:
            raise ValueError(f'Final evaluation source changed: {name}')
    return design


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--open-final',action='store_true')
    args = parser.parse_args()
    print(json.dumps(build(args.output,open_final=args.open_final),indent=2))
