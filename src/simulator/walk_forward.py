"""Persistent full-period economics for the weekly Dome screening study."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.common import sha256
from src.config import ScreeningSpec
from src.data.books import RawGroup
from src.data.final_evaluation import FinalGroup, final_plans
from src.data.walk_forward import FIRST, BOUNDARY, END, write_json
from src.models.evaluation import ranked
from src.models.settings import SEED
from src.models.walk_forward import OUTPUT as MODELS, folds
from src.paths import PROJECT_ROOT, PROCESSED_DIR
from src.simulator.final_evaluation import ATTEMPT_COLUMNS
from src.simulator.final_execution import run_event
from src.simulator.opportunities import setup, funding_metrics
from src.simulator.profitability import economic_summary

OUTPUT=PROJECT_ROOT/'results/walk_forward_replay'
DELAYS=(.1,0.,.05,.25,.5)
CONTEXT_END=END+pd.Timedelta(seconds=1.5)


def verify_models(folder):
    folder=Path(folder)
    design=json.loads((folder/'design.json').read_text())
    complete=json.loads((folder/'complete.json').read_text())
    for name,digest in complete['files_sha256'].items():
        if sha256(folder/name)!=digest:
            raise ValueError(f'Changed weekly model artifact: {name}')
    for name,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/name)!=digest:
            raise ValueError('Weekly model source changed')
    return design


def selections(prediction,random_count=20):
    pool=prediction.loc[prediction.model.eq('quoted_gap_rule')].sort_values(['time','event_slug']).reset_index(drop=True)
    keys=set(zip(pool.event_slug,pool.time))
    for _,part in prediction.groupby('model'):
        if len(part)!=len(pool) or set(zip(part.event_slug,part.time))!=keys:
            raise ValueError('All policies must rank the same eligible pool')
    pieces=[prediction,pool.assign(model='monitor_all',score=0.)]
    for index in range(random_count):
        pieces.append(pool.assign(model=f'random_{index:02d}',score=np.random.default_rng(SEED+index).random(len(pool))))
    scores=pd.concat(pieces,ignore_index=True)
    chosen=[]
    for scope,excluded,budgets in [('all_events',None,(3,5)),('exclude_december_fed','fed-decision-in-december',(3,))]:
        available=scores if excluded is None else scores.loc[scores.event_slug.ne(excluded)]
        for n in budgets:
            for name,part in available.groupby('model'):
                selected=part if name=='monitor_all' else ranked(part,part.score,n).drop(columns='_score')
                chosen.append(selected.assign(scope=scope,top_n=n))
    return pd.concat(chosen,ignore_index=True)


def delays_for(scope,model):
    return (.1,) if scope!='all_events' or model.startswith('random_') else DELAYS


def load_signals(paths):
    pieces=[]
    for path in paths:
        if Path(path).exists():
            part=pd.read_parquet(path)
            if part.empty:continue
            if 'time' not in part:
                raise ValueError('Nonempty source signals lack their monitoring hour')
            pieces.append(part)
    return pd.concat(pieces,ignore_index=True) if pieces else pd.DataFrame()


def event_frames(plan,tokens,inventory):
    """One prior as-of seed, all later states, and bounded terminal exit context."""
    frames={}
    for token in tokens:
        path=plan['paths'][token]
        earlier=pd.read_parquet(path,columns=['snapshot_time'],filters=[('snapshot_time','<',FIRST)])
        seed=earlier.snapshot_time.max() if len(earlier) else FIRST
        frame=pd.read_parquet(path,columns=RawGroup.COLUMNS,filters=[('snapshot_time','>=',seed),('snapshot_time','<=',CONTEXT_END)])
        frame.attrs['recording_end']=pd.Timestamp(inventory.loc[token,'last'])
        frames[token]=frame
    return frames


def event_replay(signals,groups,mapping,choices):
    """One independent persistent ledger per policy/scenario, never per week."""
    available=set(signals.time)
    cache={}
    details,audits,statistics=[],[],[]
    for (scope,n,model),part in choices.groupby(['scope','top_n','model']):
        times=frozenset(set(part.time)&available)
        if not times:
            continue
        for delay in delays_for(scope,model):
            key=(delay,times)
            hit=key in cache
            if not hit:
                cache[key]=run_event(signals.loc[signals.time.isin(times)],groups,mapping,setup(delay),entry_end=END,context_end=CONTEXT_END)
            trades,audit=cache[key]
            tags=dict(scope=scope,top_n=n,model=model,arrival_delay_s=delay)
            if len(trades):details.append(trades.assign(**tags))
            if len(audit):audits.append(audit.assign(**tags))
            statistics.append(dict(**tags,cache_hit=hit,selected_signal_hours=len(times)))
    return (pd.concat(details,ignore_index=True) if details else pd.DataFrame(columns=ATTEMPT_COLUMNS),
        pd.concat(audits,ignore_index=True) if audits else pd.DataFrame(),pd.DataFrame(statistics))


def summarize(attempts,chosen):
    spec=ScreeningSpec(development_start=str(FIRST),lockbox_start=str(END))
    intervals=[('all',FIRST,END),('earlier_segment',FIRST,BOUNDARY),('later_segment',BOUNDARY,END)]+folds()
    rows=[]
    groups=attempts.groupby(['scope','top_n','model','arrival_delay_s']) if len(attempts) else None
    for (scope,n,model),selected in chosen.groupby(['scope','top_n','model']):
        for delay in delays_for(scope,model):
            key=(scope,n,model,delay)
            part=groups.get_group(key) if groups is not None and key in groups.groups else attempts.iloc[:0]
            for fold,start,end in intervals:
                picked=selected.loc[selected.time.ge(start)&selected.time.lt(end)]
                trades=part.loc[part.decision_time.ge(start)&part.decision_time.lt(end)] if len(part) else part
                result=economic_summary(trades,model,spec)
                taken=trades.loc[trades.attempted] if len(trades) else trades
                completed=taken.loc[taken.status.eq('complete')] if len(taken) else taken
                positives=picked.loc[picked.label_known & picked.target.eq(1)]
                positive_keys=set(zip(positives.event_slug,positives.time))
                attempt_keys=set(zip(taken.event_slug,taken.decision_time)) if len(taken) else set()
                result.update(scope=scope,top_n=n,fold=fold,arrival_delay_s=delay,scenario=f'arrival_{int(delay*1000)}ms',
                    monitored_event_windows=len(picked),selected_unknown_windows=int((~picked.label_known).sum()),
                    selected_known_positive_windows=len(positive_keys),positive_hours_with_attempts=len(positive_keys&attempt_keys),
                    completion_rate=len(completed)/len(taken) if len(taken) else np.nan,
                    **funding_metrics(trades))
                rows.append(result)
    return pd.DataFrame(rows)


def run(output=OUTPUT,models=MODELS):
    output,models=Path(output),Path(models)
    verify_models(models)
    prediction=pd.read_parquet(models/'predictions.parquet')
    if not (prediction.time.ge(FIRST)&prediction.time.lt(END)).all():
        raise ValueError('Predictions outside declared forward interval')
    chosen=selections(prediction)
    protocol=json.loads((PROJECT_ROOT/'results/final_protocol/protocol.json').read_text())
    original_data=PROCESSED_DIR/'opportunities'
    later_data=PROCESSED_DIR/'final_evaluation'
    for source in (original_data,later_data):
        fees=pd.read_csv(source/'fee_evidence.csv')
        if fees.empty or not fees.fees_enabled_gamma.eq(False).all():
            raise ValueError('Frozen zero-fee scenario evidence changed')
    raw=pd.concat([pd.read_csv(p/'input_inventory.csv',dtype={'token_id':str}) for p in (original_data,later_data)],ignore_index=True)
    if raw.groupby('token_id').event_slug.nunique().gt(1).any() or raw.groupby('token_id').input_sha256.nunique().gt(1).any():
        raise ValueError('Cross-event tokens or inconsistent raw history require a different ledger')
    expected=raw.drop_duplicates('token_id').set_index('token_id').input_sha256.to_dict()
    inventory=pd.read_csv(PROJECT_ROOT/'results/final_coverage/token_inventory.csv',dtype={'token_id':str}).set_index('token_id')
    source=['src/simulator/walk_forward.py','src/simulator/final_execution.py','src/simulator/opportunities.py',
        'src/simulator/sizing.py','src/simulator/scheduler.py','src/simulator/execution.py','src/simulator/profitability.py',
        'src/simulator/capacity.py','src/data/final_evaluation.py','src/data/books.py','src/data/universe.py']
    execution=asdict(setup(.1));execution['freshness_s']=None
    design=dict(version=1,start=FIRST,end_exclusive=END,context_end=CONTEXT_END,
        model_completion_sha256=sha256(models/'complete.json'),predictions_sha256=sha256(models/'predictions.parquet'),
        primary_delay_s=.1,delays_s=DELAYS,execution=execution,
        policies=sorted(prediction.model.unique()),random_count=20,random_seed=SEED,
        scenarios='All deterministic policies at 0/50/100/250/500ms, N=3/5. Random and December-Fed exclusion at 100ms; exclusion N=3.',
        accounting='Empty at first evaluated hour; persistent across all weeks and original segment boundary. Separate ledger per policy/scenario; no shared tokens across events.',
        reader='Backward as-of from complete recorded token histories, each bounded by its own recording endpoint and terminal context. No hourly/weekly resets.',
        signal_policy='Reuse original qualified signals from both segments. No missing outcomes converted to negatives. No assumed random failures or artificial size/capital caps.',
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in source},
        evidence_sha256={str(p.relative_to(PROJECT_ROOT)):sha256(p) for p in [original_data/'complete.json',later_data/'complete.json',
            original_data/'fee_evidence.csv',later_data/'fee_evidence.csv',PROJECT_ROOT/'results/final_coverage/token_inventory.csv',
            PROJECT_ROOT/'results/final_protocol/protocol.json']},
        historical_fee_scenario='Frozen zero-exchange-fee cohort; not a reconstruction of fee changes.',
        interpretation='Simulated displayed-depth per-leg FOK support, one-second partial unwind; not exchange-confirmed fills. Unknown execution PnL excluded and counted.',
        models_retrained=False,labels_rebuilt=False)
    normalized=json.loads(json.dumps(design,default=str))
    output.mkdir(parents=True,exist_ok=True)
    if (output/'design.json').exists() and json.loads((output/'design.json').read_text())!=normalized:
        raise ValueError('Replay design changed; choose a new directory')
    write_json(output/'design.json',normalized)
    chosen.to_parquet(output/'selections.parquet',index=False)
    details,audits,cache_rows=[],[],[]
    for plan in final_plans(protocol):
        slug=plan['event_slug']
        selected=chosen.loc[chosen.event_slug.eq(slug)]
        signals=load_signals([dataset/'events'/slug/'signals.parquet' for dataset in (original_data,later_data)])
        if signals.empty or selected.empty:continue
        signals=signals.loc[signals.time.isin(selected.time)]
        if signals.empty:continue
        if signals.duplicated(['pair_id','start_time','time']).any():
            raise ValueError('Signal duplicated across source segments')
        required=set(signals.pair_id)
        mapping={f'{slug}:{i}':tuple(ids) for i,(ids,_,_) in enumerate(plan['packages']) if f'{slug}:{i}' in required}
        tokens={t for members in mapping.values() for t in members}
        for token in tokens:
            if sha256(plan['paths'][token])!=expected[token]:
                raise ValueError('Raw book changed since source labels')
        folder=output/'events'/slug
        if (folder/'complete.json').exists():
            for name,digest in json.loads((folder/'complete.json').read_text())['files_sha256'].items():
                if sha256(folder/name)!=digest:raise ValueError('Saved event replay changed')
            trades=pd.read_parquet(folder/'attempts.parquet')
            audit=pd.read_parquet(folder/'audit.parquet')
            cache=pd.read_csv(folder/'cache.csv')
        else:
            print(f'Continuous weekly-policy replay: {slug}; {len(signals)} signal rows',flush=True)
            frames=event_frames(plan,tokens,inventory)
            groups={pid:FinalGroup([frames[t] for t in members],context_end=CONTEXT_END) for pid,members in mapping.items()}
            trades,audit,cache=event_replay(signals,groups,mapping,selected)
            folder.mkdir(parents=True,exist_ok=True)
            trades.to_parquet(folder/'attempts.parquet',index=False)
            audit.to_parquet(folder/'audit.parquet',index=False)
            cache.to_csv(folder/'cache.csv',index=False)
            write_json(folder/'complete.json',dict(files_sha256={p.name:sha256(p) for p in folder.iterdir() if p.is_file()},
                raw_sha256={t:expected[t] for t in sorted(tokens)}))
        if len(trades):details.append(trades)
        if len(audit):audits.append(audit)
        if len(cache):cache_rows.append(cache.assign(event_slug=slug))
    attempts=pd.concat(details,ignore_index=True) if details else pd.DataFrame(columns=ATTEMPT_COLUMNS)
    attempts.to_parquet(output/'attempts.parquet',index=False)
    (pd.concat(audits,ignore_index=True) if audits else pd.DataFrame()).to_parquet(output/'scheduler_audit.parquet',index=False)
    pd.concat(cache_rows,ignore_index=True).to_csv(output/'cache_audit.csv',index=False)
    summary=summarize(attempts,chosen)
    summary.to_csv(output/'summary.csv',index=False)
    for name,digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/name)!=digest:raise ValueError('Replay source changed during execution')
    write_json(output/'complete.json',dict(files_sha256={p.relative_to(output).as_posix():sha256(p)
        for p in output.rglob('*') if p.is_file() and p.name!='complete.json'},weekly_ledger_resets=False))
    print(summary.query("scope=='all_events' and fold=='all' and top_n==3 and arrival_delay_s==.1")
        [['model','attempts','completed','known_pnl_usd','unknown_attempts']].to_string(index=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=OUTPUT)
    parser.add_argument('--models',type=Path,default=MODELS)
    args=parser.parse_args()
    run(args.output,args.models)
