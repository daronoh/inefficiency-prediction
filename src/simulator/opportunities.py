"""Fixed first-layer rankings, persistent replay and arrival-latency sensitivity."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.common import sha256
from src.config import ScreeningSpec, require_split_access
from src.data.io import load_frames, observation_groups
from src.data.universe import build_plan
from src.models.evaluation import ranked
from src.models.settings import SEED
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.reporting.opportunity_value import verify as verify_models
from src.simulator.execution import Experiment
from src.simulator.profitability import economic_summary, SCOPES
from src.simulator.scheduler import run_event

POLICIES=('quoted_gap_rule','fixed_book_logistic','fixed_book_random_forest',
    'fixed_combined_random_forest','tuned_value_two_stage_random_forest','tuned_value_ensemble')
DELAYS=(.1,0.,.05,.25,.5)
BUDGETS=(5,3)


def setup(delay,minutes=60):
    if not np.isfinite(delay) or delay<0:
        raise ValueError('Finite nonnegative latency required')
    return Experiment(delay_s=delay,wait_s=0.,sizing='adaptive',freshness_s=float('inf'),
        max_quantity=None,cash_cap_usd=None,unwind_delay_s=1.,min_quantity=5,
        min_entry_gross_usd=0.,min_limit_margin_usd=0.,monitoring_minutes=minutes,compute_failure_stress=False)


def selections(prediction,random_count=20):
    """Create choices once, without future-label filtering; identical across latencies."""
    pieces=[]
    pool=prediction.loc[prediction.model.eq('quoted_gap_rule')].sort_values(['time','event_slug']).reset_index(drop=True)
    if pool.empty or pool.duplicated(['event_slug','time']).any():
        raise ValueError('Unique reference prediction population required')
    members=[prediction.loc[prediction.model.eq(name)].copy() for name in POLICIES]
    keys=set(zip(pool.event_slug,pool.time))
    for part in members:
        if len(part)!=len(pool) or set(zip(part.event_slug,part.time))!=keys:
            raise ValueError('Policies must rank the same population')
    for seed in range(random_count):
        members.append(pool.assign(model=f'random_{seed:02d}',score=np.random.default_rng(SEED+seed).random(len(pool))))
    scores=pd.concat(members,ignore_index=True)
    for scope,excluded in SCOPES.items():
        available=scores if excluded is None else scores.loc[scores.event_slug.ne(excluded)]
        for n in BUDGETS:
            for name,part in available.groupby('model'):
                pieces.append(ranked(part,part.score,n).drop(columns='_score').assign(scope=scope,top_n=n))
            all_events=pool if excluded is None else pool.loc[pool.event_slug.ne(excluded)]
            pieces.append(all_events.assign(model='monitor_all',scope=scope,top_n=n))
    return pd.concat(pieces,ignore_index=True)


def replay_event(signals,groups,mapping,choices,minutes=60):
    details,audits=[],[]
    available=set(signals.time)
    policies=list(choices.groupby(['scope','top_n','model']))
    for delay in DELAYS:
        cache={}
        for (scope,n,model),selected in policies:
            times=frozenset(set(selected.time)&available)
            if not times:
                continue
            if times not in cache:
                cache[times]=run_event(signals.loc[signals.time.isin(times)],groups,mapping,setup(delay,minutes))
            attempts,audit=cache[times]
            tags=dict(scope=scope,top_n=n,model=model,arrival_delay_s=delay)
            if len(attempts):
                details.append(attempts.assign(**tags))
            if len(audit):
                audits.append(audit.assign(**tags))
    return (pd.concat(details,ignore_index=True) if details else pd.DataFrame(),
            pd.concat(audits,ignore_index=True) if audits else pd.DataFrame())


def funding_metrics(part):
    """Cash scenarios, not realised portfolio capital or turnover-based ROI.

    Pending limit reservations are released at arrival; known purchases consume
    cash. Completed baskets receive no settlement cash within the experiment.
    Known partial unwinds return their proceeds; unknown orders retain full
    reservations and unknown exits return nothing. Simultaneous submissions are
    charged before cash releases, conservatively avoiding intratimestamp netting.
    """
    if part.empty:
        return dict(peak_inflight_reservations_usd=0.,no_settlement_funding_usd=0.)
    events=[]
    for row in part.loc[part.attempted].itertuples():
        reserve=float(row.reserved_cash_usd)
        events.append((row.confirmation_time,0,reserve,reserve))
        events.append((row.unwind_time,1,-reserve,0.))
        if pd.notna(row.entry_cost_usd):
            events.append((row.arrival_time,1,0.,float(row.entry_cost_usd)-reserve))
            if row.status=='partial_unwound' and pd.notna(row.pnl_usd):
                events.append((row.unwind_time,1,0.,-float(row.entry_cost_usd+row.pnl_usd)))
    ledger=pd.DataFrame(events,columns=['time','phase','inflight','funding']).groupby(['time','phase'])[['inflight','funding']].sum().sort_index().cumsum()
    return dict(peak_inflight_reservations_usd=max(0.,float(ledger.inflight.max())),
        no_settlement_funding_usd=max(0.,float(ledger.funding.max())))


def summarize(attempts,choices,folds,window):
    rows=[]
    spec=ScreeningSpec(**window)
    intervals=[('all',folds[0][1],folds[-1][2])]+folds
    groups=attempts.groupby(['scope','top_n','model','arrival_delay_s']) if len(attempts) else None
    for (scope,n,model),selected in choices.groupby(['scope','top_n','model']):
        for delay in DELAYS:
            key=(scope,n,model,delay)
            part=groups.get_group(key) if groups is not None and key in groups.groups else pd.DataFrame()
            for fold,start,end in intervals:
                lo,hi=pd.Timestamp(start,tz='UTC'),pd.Timestamp(end,tz='UTC')
                chosen=selected.loc[selected.time.ge(lo)&selected.time.lt(hi)]
                trades=part.loc[part.decision_time.ge(lo)&part.decision_time.lt(hi)] if len(part) else part
                result=economic_summary(trades,model,spec)
                taken=trades.loc[trades.attempted] if len(trades) else trades
                positives=chosen.loc[chosen.label_known & chosen.target.eq(1)]
                known_keys=set(zip(positives.event_slug,positives.time))
                attempted_keys=set(zip(taken.event_slug,taken.decision_time)) if len(taken) else set()
                completed=taken.loc[taken.status.eq('complete')] if len(taken) else taken
                complete_keys=set(zip(completed.event_slug,completed.decision_time)) if len(completed) else set()
                result.update(scope=scope,top_n=n,fold=fold,scenario=f'arrival_{int(delay*1000)}ms',arrival_delay_s=delay,
                    monitored_event_windows=len(chosen),selected_unknown_windows=int((~chosen.label_known).sum()),
                    known_positive_hours_selected=len(known_keys),positive_hours_with_attempts=len(known_keys&attempted_keys),
                    positive_hours_with_completions=len(known_keys&complete_keys),
                    opportunity_hour_to_attempt_rate=len(known_keys&attempted_keys)/len(known_keys) if known_keys else np.nan,
                    opportunity_hour_to_completion_rate=len(known_keys&complete_keys)/len(known_keys) if known_keys else np.nan,
                    completion_rate=len(completed)/len(taken) if len(taken) else np.nan,
                    no_fill_attempts=int(taken.status.eq('no_fill').sum()) if len(taken) else 0,
                    **funding_metrics(trades))
                rows.append(result)
    return pd.DataFrame(rows)


def replay(output,models,occurrence,data,observation_data):
    require_split_access('development')
    output,models,occurrence,data,observation_data=map(Path,(output,models,occurrence,data,observation_data))
    trained=verify_models(models,data,occurrence,observation_data)
    prediction=pd.read_parquet(models/'predictions.parquet')
    start,end=pd.Timestamp(trained['window']['development_start']),pd.Timestamp(trained['window']['lockbox_start'])
    horizon=pd.Timedelta(minutes=trained['window']['horizon_minutes'])
    if not (prediction.time.ge(start)&(prediction.time+horizon).le(end)).all():
        raise ValueError('Replay must remain within development')
    fees=pd.read_csv(observation_data/'fee_evidence.csv')
    if fees.empty or not fees.fees_enabled_gamma.eq(False).all():
        raise ValueError('Zero-fee evidence required')
    chosen=selections(prediction)
    source=['src/simulator/opportunities.py','src/simulator/scheduler.py','src/simulator/execution.py',
        'src/simulator/sizing.py','src/simulator/profitability.py','src/simulator/capacity.py',
        'src/simulator/sensitivity.py','src/data/books.py','src/data/io.py','src/data/universe.py']
    execution=asdict(setup(.1,trained['window']['horizon_minutes']))
    execution['freshness_s']=None
    design=dict(version='opportunity_economic_replay_v1',window=trained['window'],folds=trained['folds'],
        predictions_sha256=sha256(models/'predictions.parquet'),model_completion_sha256=sha256(models/'complete.json'),
        observation_completion_sha256=sha256(observation_data/'complete.json'),
        value_completion_sha256=sha256(data/'complete.json'),policies=list(POLICIES),random_count=20,random_seed=SEED,
        budgets=list(BUDGETS),arrival_delays_s=list(DELAYS),primary_arrival_s=.1,execution=execution,scopes=SCOPES,
        models_retrained=False,labels_rebuilt=False,final_lockbox_opened=False,
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in source},
        interpretation='Fixed selections; fresh independent ledger per policy/scenario, persistent within each event across all hours/folds. One-second hold/unwind fixed, including zero arrival. No imposed failure probability or artificial capital/share cap. Known contractual basket margins and unwound partial P&L; not live fills.',
        funding='Peak in-flight reservations and no-settlement cash requirement scenario; completed baskets held without cash release, known partial proceeds returned, unknowns retained conservatively. Not measured portfolio capital or ROI.')
    output.mkdir(parents=True,exist_ok=False)
    (output/'events').mkdir()
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    chosen.to_parquet(output/'selections.parquet',index=False)
    inventory=pd.read_csv(observation_data/'input_inventory.csv',dtype={'token_id':str})
    owners=inventory.groupby('token_id').event_slug.nunique()
    if owners.gt(1).any():
        raise ValueError('Shared tokens across events require a portfolio reservation ledger')
    expected={(r.event_slug,r.token_id):r.input_sha256 for r in inventory.itertuples()}
    plans,_=build_plan('all')
    details,audits=[],[]
    for plan in plans:
        slug=plan['event_slug']
        selected=chosen.loc[chosen.event_slug.eq(slug)]
        path=observation_data/'events'/slug/'signals.parquet'
        if selected.empty or not path.exists():
            continue
        signals=pd.read_parquet(path)
        if signals.empty:
            continue
        signals=signals.loc[signals.time.isin(selected.time)]
        if signals.empty:
            continue
        print(f'Fixed-ranking economic replay: {slug}',flush=True)
        for token,path in plan['paths'].items():
            if sha256(path)!=expected[(slug,token)]:
                raise ValueError('Raw books changed since opportunity labels')
        groups,mapping=observation_groups(plan,load_frames(plan),set(signals.pair_id))
        trades,audit=replay_event(signals,groups,mapping,selected,trained['window']['horizon_minutes'])
        if len(trades):
            trades.to_parquet(output/'events'/f'{slug}.parquet',index=False)
            details.append(trades)
        if len(audit):
            audits.append(audit)
    attempts=pd.concat(details,ignore_index=True) if details else pd.DataFrame()
    attempts.to_parquet(output/'attempts.parquet',index=False)
    (pd.concat(audits,ignore_index=True) if audits else pd.DataFrame()).to_parquet(output/'scheduler_audit.parquet',index=False)
    summarize(attempts,chosen,trained['folds'],trained['window']).to_csv(output/'summary.csv',index=False)
    files={p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(files_sha256=files,final_lockbox_opened=False,
        models_retrained=False,labels_rebuilt=False,selections_sha256=sha256(output/'selections.parquet')),
        indent=2)+'\n')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--models',type=Path,default=PROJECT_ROOT/'results/opportunity_value')
    parser.add_argument('--occurrence',type=Path,default=PROJECT_ROOT/'results/opportunities')
    parser.add_argument('--data',type=Path,default=PROCESSED_DIR/'opportunity_value')
    parser.add_argument('--observation-data',type=Path,default=PROCESSED_DIR/'opportunities')
    args=parser.parse_args()
    replay(args.output,args.models,args.occurrence,args.data,args.observation_data)
