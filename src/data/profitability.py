"""Uncapped hourly net-P&L targets, independent of any model's selections."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import numpy as np
import pandas as pd
from src.common import sha256
from src.config import SPEC, require_split_access
from src.data.books import RawGroup
from src.data.coverage import classify_book, observation_fraction
from src.data.io import load_frames, observation_groups
from src.data.labels import summarize_window
from src.data.snapshots import add_hourly_history
from src.data.universe import build_plan
from src.simulator.capacity import discover_depth_signals
from src.simulator.execution import experiment
from src.simulator.scheduler import run_event
from src.simulator.sizing import depth_quantities


def profit_setup(spec=SPEC):
    return replace(experiment(spec), delay_s=.1, max_quantity=None, cash_cap_usd=None,
                   compute_failure_stress=False)


def quoted_net(books, other_cost=0.):
    """Best integer-depth dollar margin, including negative margins for ranking.

    This is a snapshot feature, not an execution filter. Like the original
    quoted-gap feature it does not require a positive worst-limit margin.
    """
    if len(books) < 2 or any(b is None or not b.clean for b in books):
        return np.nan
    if any(not np.isfinite([b.min_order_size,b.tick_size]).all() or b.min_order_size <= 0
           or not 0 < b.tick_size <= 1 for b in books):
        return np.nan
    q = depth_quantities(books,5,None,None,None)
    if not len(q):
        return np.nan
    costs, limits = zip(*(b.asks._quotes(q) for b in books))
    valid = np.isfinite(costs).all(axis=0)
    for b, limit in zip(books,limits,strict=True):
        valid &= np.isclose(limit/b.tick_size,np.rint(limit/b.tick_size),rtol=0,atol=1e-7)
    return float(np.max((q-np.sum(costs,axis=0)-other_cost)[valid])) if valid.any() else np.nan


def refresh_quoted_features(plan, frames, base, spec):
    """Change only quantity-dependent quote features; use no future book rows."""
    readers = {t:RawGroup([f]) for t,f in frames.items()}
    result = base.copy()
    for index, row in base.iterrows():
        books = {}
        for token, reader in readers.items():
            position = reader.times[0].searchsorted(row.time,side='right')-1
            books[token] = reader._book(0,int(position)) if position >= 0 else None
        values = np.asarray([quoted_net([books[t] for t in ids],spec.assumed_other_cost_usd)
            for ids,start,end in plan['packages'] if start <= row.time < end])
        if len(values):
            result.loc[index,'x_best_quoted_net_usd'] = np.nanmax(values) if np.isfinite(values).any() else np.nan
            result.loc[index,'x_size_feasible_package_fraction'] = np.isfinite(values).mean()
            result.loc[index,'x_positive_quoted_net_package_fraction'] = (values > 0).mean()
    return result


def build_profitability(output, features_path, spec=SPEC):
    """Generate all standalone windows, including training hours and losses."""
    require_split_access('development')
    output, features_path = Path(output), Path(features_path)
    parent_design = features_path.parent/'run.json'
    if parent_design.exists():
        parent = json.loads(parent_design.read_text())['contract']
        if any(parent[key] != getattr(spec,key) for key in ('cadence_minutes','horizon_minutes','development_start','lockbox_start')):
            raise ValueError('Rebuild scheduled inputs for the requested window and development interval')
    base = pd.read_parquet(features_path)
    if not (base.time.ge(pd.Timestamp(spec.development_start)) &
            (base.time+spec.horizon).le(pd.Timestamp(spec.lockbox_start))).all():
        raise ValueError('Every target window must be entirely within development')
    base = base.drop(columns=[c for c in base if c.startswith(('x_snapshot_lag1_','x_snapshot_change1_'))])
    output.mkdir(parents=True,exist_ok=False)
    setup = profit_setup(spec)
    execution = asdict(setup)
    execution['freshness_s'] = None
    design = dict(version=1,contract=asdict(spec),execution=execution,
        parent_features_sha256=sha256(features_path),final_lockbox_opened=False,
        target='Standalone event-window net simulated P&L, including actual partial losses; unknowns missing',
        coverage='Uncapped integer-depth entry observability; every active package decidable for the whole window',
        features='Original snapshot inputs with uncapped quote-profit, feasible and positive-package fractions and rebuilt lags',
        membership='Original retrospective universe and snapshot-eligible population retained',
        fees='Zero exchange fees for the verified historical market types; additional cost from contract',
        source_sha256=sha256(__file__))
    (output/'design.json').write_text(json.dumps(design,indent=2,allow_nan=False)+'\n')
    plans,_ = build_plan('all')
    pieces, inventory = [], []
    for plan in plans:
        slug = plan['event_slug']
        selected = base.loc[base.event_slug.eq(slug)].copy()
        if selected.empty:
            continue
        print(f'Profitability labels and features: {slug}',flush=True)
        frames = {t:RawGroup([f]).frames[0] for t,f in load_frames(plan).items()}
        windows = pd.DatetimeIndex(selected.time.sort_values())
        states = {}
        for token,frame in frames.items():
            values = [classify_book(r.asks_json,r.bids_json,r.min_order_size,r.tick_size,quantities=None)
                      for r in frame.itertuples(index=False)]
            states[token] = pd.DataFrame(values,columns=['size_qualified','state','reason']).assign(time=frame.snapshot_time.to_numpy())
        coverage = observation_fraction(plan,states,windows,spec)
        signals = discover_depth_signals(plan,frames,setup,spec)
        if len(signals):
            signals = signals.loc[signals.time.isin(windows)]
        folder = output/'events'/slug
        folder.mkdir(parents=True)
        signals.to_parquet(folder/'signals.parquet',index=False)
        groups,mapping = observation_groups(plan,frames,set(signals.pair_id)) if len(signals) else ({},{})
        by_time = dict(tuple(signals.groupby('time'))) if len(signals) else {}
        labels, attempts = [], []
        for when,fraction in zip(windows,coverage,strict=True):
            subset = by_time.get(when,pd.DataFrame())
            detail = run_event(subset,groups,mapping,setup)[0] if len(subset) else pd.DataFrame()
            stats = summarize_window(detail,subset,spec)
            known = fraction >= 1-1e-9 and stats['unknown_attempts'] == 0
            labels.append(dict(time=when,coverage_fraction=float(fraction),label_known=known,
                target=float(stats['positive']) if known else np.nan,
                pnl_target=stats['known_pnl_usd'] if known else np.nan,
                activity_target=float(stats['attempts'] > 0) if known else np.nan,**stats))
            if len(detail):
                attempts.append(detail)
        if attempts:
            pd.concat(attempts,ignore_index=True).to_parquet(folder/'standalone_attempts.parquet',index=False)
        selected = refresh_quoted_features(plan,frames,selected,spec)
        pieces.append(selected.merge(pd.DataFrame(labels),on='time',validate='one_to_one'))
        inventory.extend(dict(event_slug=slug,token_id=t,rows=len(f),input_sha256=sha256(plan['paths'][t])) for t,f in frames.items())
        pd.concat(pieces,ignore_index=True).to_parquet(output/'examples.parquet',index=False)
    result = add_hourly_history(pd.concat(pieces,ignore_index=True),spec)
    result['future_positive'] &= result.label_known
    result.to_parquet(output/'examples.parquet',index=False)
    (output/'complete.json').write_text(json.dumps(dict(examples_sha256=sha256(output/'examples.parquet'),
        rows=len(result),events=result.event_slug.nunique(),input_inventory=inventory,
        final_lockbox_opened=False),indent=2)+'\n')
    return result


def load_profitability(folder):
    folder = Path(folder)
    complete = json.loads((folder/'complete.json').read_text())
    if complete['examples_sha256'] != sha256(folder/'examples.parquet'):
        raise ValueError('Profitability dataset completion hash differs')
    frame = pd.read_parquet(folder/'examples.parquet')
    design = json.loads((folder/'design.json').read_text())
    if frame.time.max() >= pd.Timestamp(design['contract']['lockbox_start']):
        raise ValueError('Final test cannot enter profitability development')
    return frame.loc[frame.eligible_snapshot].sort_values(['time','event_slug']).reset_index(drop=True)
