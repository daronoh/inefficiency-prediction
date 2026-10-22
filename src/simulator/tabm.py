"""Replay the declared TabM policies with the unchanged opportunity simulator."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.config import require_split_access
from src.data.io import load_frames, observation_groups
from src.data.universe import build_plan
from src.models.evaluation import ranked
from src.models.tabm_study import verify, write_json, REPLAY_POLICIES
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.simulator.opportunities import replay_event, setup, summarize, DELAYS, BUDGETS
from src.simulator.profitability import SCOPES


def selections(prediction):
    """Rank every eligible row before consulting labels, including unknowns."""
    pieces=[]
    reference=prediction.loc[prediction.model.eq('quoted_gap_rule')]
    keys=set(zip(reference.event_slug,reference.time))
    for name in REPLAY_POLICIES:
        part=prediction.loc[prediction.model.eq(name)]
        if len(part)!=len(keys) or set(zip(part.event_slug,part.time))!=keys:
            raise ValueError(f'TabM evaluation population differs: {name}')
        for scope,excluded in SCOPES.items():
            pool=part if excluded is None else part.loc[part.event_slug.ne(excluded)]
            for n in BUDGETS:
                pieces.append(ranked(pool,pool.score,n).drop(columns='_score').assign(scope=scope,top_n=n))
    return pd.concat(pieces,ignore_index=True)


def replay(output,models=PROJECT_ROOT/'results/tabm',baseline=PROJECT_ROOT/'results/opportunity_replay'):
    from src.reporting.opportunity_replay import verify as verify_baseline
    require_split_access('development')
    output,models,baseline=map(Path,(output,models,baseline))
    trained=verify(models)
    base=verify_baseline(baseline,PROJECT_ROOT/'results/opportunity_value')
    if base['window']!=trained['window'] or base['folds']!=trained['folds']:
        raise ValueError('TabM and baseline replay windows differ')
    observation=PROCESSED_DIR/'opportunities'
    # Check the original signal/coverage artifacts as well as the raw books below.
    complete=json.loads((observation/'complete.json').read_text())
    for path,digest in complete['files_sha256'].items():
        if sha256(observation/path)!=digest:
            raise ValueError(f'Observation input changed: {path}')
    fees=pd.read_csv(observation/'fee_evidence.csv')
    if fees.empty or not fees.fees_enabled_gamma.eq(False).all():
        raise ValueError('Zero-fee evidence required')
    chosen=selections(pd.read_parquet(models/'predictions.parquet'))
    source=list(base['source_sha256'])+['src/simulator/tabm.py']
    execution=asdict(setup(.1,trained['window']['horizon_minutes']))
    execution['freshness_s']=None
    if execution!=base['execution']:
        raise ValueError('Execution settings differ from frozen comparator')
    design=dict(version='tabm_replay_v1',window=trained['window'],folds=trained['folds'],
        policies=list(REPLAY_POLICIES),budgets=list(BUDGETS),arrival_delays_s=list(DELAYS),scopes=SCOPES,
        primary_arrival_s=.1,execution=execution,
        predictions_sha256=sha256(models/'predictions.parquet'),model_completion_sha256=sha256(models/'complete.json'),
        baseline_completion_sha256=sha256(baseline/'complete.json'),
        observation_completion_sha256=sha256(observation/'complete.json'),
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in source},
        models_retrained=False,labels_rebuilt=False,final_lockbox_opened=False,
        interpretation=base['interpretation'],funding=base['funding'])
    output.mkdir(parents=True,exist_ok=False)
    (output/'events').mkdir()
    write_json(output/'design.json',design)
    chosen.to_parquet(output/'selections.parquet',index=False)
    inventory=pd.read_csv(observation/'input_inventory.csv',dtype={'token_id':str})
    if inventory.groupby('token_id').event_slug.nunique().gt(1).any():
        raise ValueError('Shared token inventory needs a cross-event ledger')
    expected={(r.event_slug,r.token_id):r.input_sha256 for r in inventory.itertuples()}
    plans,_=build_plan('all')
    details,audits=[],[]
    for plan in plans:
        slug=plan['event_slug']
        selected=chosen.loc[chosen.event_slug.eq(slug)]
        path=observation/'events'/slug/'signals.parquet'
        if selected.empty or not path.exists():
            continue
        signals=pd.read_parquet(path)
        if signals.empty:
            continue
        signals=signals.loc[signals.time.isin(selected.time)]
        if signals.empty:
            continue
        print(f'TabM economic replay: {slug}',flush=True)
        for token,path in plan['paths'].items():
            if sha256(path)!=expected[(slug,token)]:
                raise ValueError('Raw books changed')
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
    write_json(output/'complete.json',dict(files_sha256=files,models_retrained=False,labels_rebuilt=False,final_lockbox_opened=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--models',type=Path,default=PROJECT_ROOT/'results/tabm')
    args=parser.parse_args()
    replay(args.output,args.models)
