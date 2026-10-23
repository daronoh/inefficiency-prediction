"""Development-only economic comparison for the three N=3-retuned policies."""
import argparse
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.data.io import load_frames, observation_groups
from src.data.universe import build_plan
from src.models.evaluation import ranked
from src.models.final_preparation import verify, NEW_POLICIES
from src.models.tabm_study import write_json
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.simulator.opportunities import replay_event, summarize, DELAYS
from src.simulator.profitability import SCOPES


def selections(prediction):
    reference=prediction.loc[prediction.model.eq('quoted_gap_rule')]
    keys=set(zip(reference.event_slug,reference.time))
    pieces=[]
    for model in NEW_POLICIES:
        part=prediction.loc[prediction.model.eq(model)]
        if len(part)!=len(keys) or set(zip(part.event_slug,part.time))!=keys:
            raise ValueError('Retuned policies must rank the same eligible population')
        for scope,excluded in SCOPES.items():
            pool=part if excluded is None else part.loc[part.event_slug.ne(excluded)]
            for n in (3,5):
                pieces.append(ranked(pool,pool.score,n).drop(columns='_score').assign(scope=scope,top_n=n))
    return pd.concat(pieces,ignore_index=True)


def replay(output,models):
    from src.reporting.opportunity_replay import verify as verify_reference
    output,models=map(Path,(output,models))
    trained=verify(models)
    baseline=PROJECT_ROOT/'results/opportunity_replay'
    old=verify_reference(baseline,PROJECT_ROOT/'results/opportunity_value')
    if trained['execution']!=old['execution'] or trained['window']!=old['window']:
        raise ValueError('Execution or window mismatch')
    observation=PROCESSED_DIR/'opportunities'
    complete=json.loads((observation/'complete.json').read_text())
    for path,digest in complete['files_sha256'].items():
        if sha256(observation/path)!=digest:
            raise ValueError(f'Observation input changed: {path}')
    chosen=selections(pd.read_parquet(models/'predictions.parquet'))
    sources=list(old['source_sha256'])+['src/simulator/final_preparation.py']
    design=dict(version='n3_development_replay_v1',window=trained['window'],folds=trained['folds'],
        policies=list(NEW_POLICIES),budgets=[3,5],primary_budget=3,primary_arrival_s=.1,
        arrival_delays_s=list(DELAYS),execution=old['execution'],scopes=SCOPES,
        predictions_sha256=sha256(models/'predictions.parquet'),model_completion_sha256=sha256(models/'complete.json'),
        baseline_completion_sha256=sha256(baseline/'complete.json'),
        observation_completion_sha256=sha256(observation/'complete.json'),
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in sources},
        interpretation='Earlier-trained outer models only; final refits are NOT evaluated on their training data. Same persistent scheduler and execution assumptions as the frozen references.',
        models_retrained=False,labels_rebuilt=False,final_lockbox_opened=False)
    output.mkdir(parents=True,exist_ok=False)
    (output/'events').mkdir()
    write_json(output/'design.json',design)
    chosen.to_parquet(output/'selections.parquet',index=False)
    inventory=pd.read_csv(observation/'input_inventory.csv',dtype={'token_id':str})
    if inventory.groupby('token_id').event_slug.nunique().gt(1).any():
        raise ValueError('Shared tokens require a cross-event ledger')
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
        print(f'N=3 development replay: {slug}',flush=True)
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
    parser.add_argument('--models',type=Path,default=PROJECT_ROOT/'results/final_preparation')
    args=parser.parse_args()
    replay(args.output,args.models)
