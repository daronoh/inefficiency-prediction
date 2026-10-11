"""Predictors computed only from current and previous scheduled snapshots."""
from dataclasses import asdict
import json
from pathlib import Path
import numpy as np
import pandas as pd
from src.data.books import RawGroup
from src.data.universe import build_plan
from src.config import SPEC, require_split_access, shortlist_size, spec_hash

def add_hourly_history(frame, spec=SPEC):
    if frame.duplicated(['event_slug', 'time']).any():
        raise ValueError('Duplicate event/decision time')
    result = frame.sort_values(['event_slug', 'time']).copy()
    consecutive = result.time.sub(result.groupby('event_slug').time.shift()).eq(pd.Timedelta(minutes=spec.cadence_minutes))
    for column in [c for c in frame if c.startswith('x_')]:
        previous = result.groupby('event_slug')[column].shift().where(consecutive)
        result[f'x_snapshot_lag1_{column[2:]}'] = previous
        result[f'x_snapshot_change1_{column[2:]}'] = result[column] - previous
    return result


def sample_token(frame, hours, spec=SPEC):
    """Parse only the last recorded book at or before each requested hour."""
    group = RawGroup([frame])
    columns = ['seen', 'clean', 'age_s', 'spread', 'ask_depth', 'min_size']
    columns += [f'cost_{q}' for q in spec.quantities] + [f'limit_{q}' for q in spec.quantities]
    out = pd.DataFrame(np.nan, index=hours, columns=columns)
    times = group.times[0]
    if not len(times):
        return out
    positions = times.get_indexer(hours, method='pad')
    cache = {}
    for i, pos in enumerate(positions):
        if pos < 0:
            continue
        if pos not in cache:
            book = group._book(0, int(pos))
            stats = dict(seen=1., clean=float(book.clean),
                spread=book.asks.prices[0] - book.bids.prices[0] if book.clean else np.nan,
                ask_depth=float(book.asks.sizes.sum()), min_size=book.min_order_size)
            for q in spec.quantities:
                quote = book.asks.quote(q) if book.clean and np.isfinite(book.min_order_size) and q >= book.min_order_size else None
                valid = quote is not None and np.isfinite(book.tick_size) and book.tick_size > 0
                if valid:
                    valid = abs(quote[1] / book.tick_size - round(quote[1] / book.tick_size)) < 1e-7
                stats[f'cost_{q}'] = quote[0] if valid else np.nan
                stats[f'limit_{q}'] = quote[1] if valid else np.nan
            cache[pos] = stats
        out.iloc[i] = pd.Series({**cache[pos], 'age_s': (hours[i] - times[pos]).total_seconds()}).reindex(columns)
    return out


def event_features(plan, spec=SPEC):
    frames = {t: pd.read_parquet(p, columns=RawGroup.COLUMNS, filters=[
        ('snapshot_time', '>=', pd.Timestamp(SPEC.development_start)),
        ('snapshot_time', '<', pd.Timestamp(SPEC.lockbox_start))]) for t, p in plan['paths'].items()}
    if any(f.empty for f in frames.values()):
        return pd.DataFrame()
    # Extent is an offline observation mask, never an input to a live ranker.
    start = min(f.snapshot_time.min() for f in frames.values()).ceil(spec.frequency)
    end = min(max(f.snapshot_time.max() for f in frames.values()), pd.Timestamp(SPEC.lockbox_start))
    hours = pd.date_range(start, end, freq=spec.frequency)
    sampled = {t: sample_token(f, hours, spec) for t, f in frames.items()}
    rows = []
    for when in hours:
        packages = [ids for ids, start, end in plan['packages'] if start <= when < end]
        if not packages:
            continue
        ids = sorted({t for package in packages for t in package})
        books = pd.DataFrame({t: sampled[t].loc[when] for t in ids}).T
        bests = []
        for package in packages:
            members = books.loc[package]
            gross = []
            for q in spec.quantities:
                costs, limits = members[f'cost_{q}'], members[f'limit_{q}']
                if costs.notna().all() and limits.notna().all() and q * limits.sum() <= spec.cash_cap_usd:
                    gross.append(q - costs.sum() - spec.assumed_other_cost_usd)
            bests.append(max(gross) if gross else np.nan)
        values = np.asarray(bests)
        rows.append(dict(event_slug=plan['event_slug'], stratum=plan['stratum'], time=when,
            eligible_snapshot=bool(books.seen.eq(1).all()),
            x_active_packages=len(packages), x_active_tokens=len(ids),
            x_seen_fraction=float(books.seen.eq(1).mean()), x_clean_fraction=float(books.clean.eq(1).mean()),
            x_spread_mean=books.spread.mean(), x_spread_max=books.spread.max(),
            x_ask_depth_sum=books.ask_depth.sum(min_count=1), x_book_age_max_s=books.age_s.max(),
            x_best_quoted_net_usd=float(np.nanmax(values)) if np.isfinite(values).any() else np.nan,
            x_size_feasible_package_fraction=float(np.isfinite(values).mean()),
            x_positive_quoted_net_package_fraction=float((values > 0).mean())))
    return pd.DataFrame(rows)


def prepare(output: Path, top_n: int, scope='all', spec=SPEC, ladder_scope='audited5'):
    require_split_access('development')
    shortlist_size(0, top_n)
    output.mkdir(parents=True, exist_ok=False)
    plans, excluded = build_plan(scope, ladder_scope)
    frames = []
    for index, plan in enumerate(plans, 1):
        print(f"{spec.cadence_minutes}-minute snapshots {index}/{len(plans)}: {plan['event_slug']}", flush=True)
        frame = event_features(plan, spec)
        if frame.empty:
            excluded.append(dict(event_slug=plan['event_slug'], stratum=plan['stratum'], reason='no_complete_development_member_coverage'))
        else:
            frames.append(frame)
    if not frames:
        raise ValueError('No observable event snapshots')
    features = add_hourly_history(pd.concat(frames, ignore_index=True), spec)
    features.to_parquet(output / 'hourly_features.parquet', index=False)
    pd.DataFrame(excluded).to_csv(output / 'excluded_events.csv', index=False)
    budget = features.loc[features.eligible_snapshot].groupby('time').agg(
        eligible_events=('event_slug', 'size'), broad_scan_token_snapshots=('x_active_tokens', 'sum')).reset_index()
    budget['top_n'] = budget.eligible_events.map(lambda n: shortlist_size(n, top_n))
    budget['monitor_all'] = budget.top_n.eq(budget.eligible_events)
    budget.to_csv(output / 'hourly_budget.csv', index=False)
    state = dict(contract=asdict(spec), spec_hash=spec_hash(spec), top_n=top_n, scope=scope, ladder_scope=ladder_scope,
        feature_rows=len(features), events=features.event_slug.nunique(), eligible_rows=int(features.eligible_snapshot.sum()),
        features=sum(c.startswith('x_') for c in features),
        eligible_events=int(features.loc[features.eligible_snapshot].event_slug.nunique()),
        labels_built=False, model_trained=False,
        caveats=['Frozen retrospective membership, not a live registry.',
            f'Quoted net subtracts the configured other cost (${spec.assumed_other_cost_usd:g}); baseline is zero, execution risk is separate.',
            'Exchange-time snapshots; provider availability sensitivity still required.',
            'Historical labels and full model evaluation must be rebuilt for this contract.'])
    (output / 'run.json').write_text(json.dumps(state, indent=2) + '\n', encoding='utf-8')
    return state
