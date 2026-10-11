"""Persistent policy replay and a reranked dominant-event exclusion check."""
import json
from pathlib import Path
import numpy as np
import pandas as pd
from src.common import sha256
from src.config import ScreeningSpec
from src.data.io import load_frames, observation_groups
from src.data.profitability import profit_setup
from src.data.universe import build_plan
from src.models.evaluation import ranked
from src.models.settings import FOLDS, SEED
from src.simulator.capacity import capacity_summary
from src.simulator.scheduler import run_event

SCOPES = {'all_events':None,'exclude_december_fed':'fed-decision-in-december'}


def policy_choices(prediction,top_n=5,excluded=None):
    pool = prediction if excluded is None else prediction.loc[prediction.event_slug.ne(excluded)]
    choices={}
    for name,part in pool.groupby('model'):
        chosen=ranked(part,part.score,top_n)
        choices[name]=set(zip(chosen.event_slug,chosen.time))
    choices['monitor_all']=set(zip(pool.event_slug,pool.time))
    return choices


def economic_summary(part,model,spec):
    row=capacity_summary(part,model,spec)
    taken=part.loc[part.attempted].copy() if len(part) else part
    if len(taken):
        taken['accounting_time']=taken.arrival_time.where(taken.status.eq('complete'),taken.unwind_time)
        increments=taken.loc[taken.pnl_usd.notna()].groupby('accounting_time').pnl_usd.agg(['sum','size'])
        curve=(increments['sum']-increments['size']*spec.assumed_other_cost_usd).cumsum().to_numpy()
        with_zero=np.r_[0.,curve]
        row['max_drawdown_usd']=float(np.max(np.maximum.accumulate(with_zero)-with_zero))
        row['worst_attempt_usd']=float(taken.pnl_usd.min())
    else:
        row['max_drawdown_usd']=row['worst_attempt_usd']=0.
    return row


def replay_profitability(output,dataset_dir,models_dir,top_n=5):
    output,dataset_dir,models_dir=Path(output),Path(dataset_dir),Path(models_dir)
    design=json.loads((models_dir/'design.json').read_text())
    complete=json.loads((models_dir/'complete.json').read_text())
    if design['dataset_sha256'] != sha256(dataset_dir/'examples.parquet'):
        raise ValueError('Profitability models and labels differ')
    if complete['predictions_sha256'] != sha256(models_dir/'predictions.parquet'):
        raise ValueError('Incomplete or modified predictions')
    spec=ScreeningSpec(**design['dataset_design']['contract'])
    prediction=pd.read_parquet(models_dir/'predictions.parquet')
    if prediction.time.min() < pd.Timestamp(spec.development_start) or prediction.time.max() >= pd.Timestamp(spec.lockbox_start):
        raise ValueError('Replay is development-only')
    choices={scope:policy_choices(prediction,top_n,excluded) for scope,excluded in SCOPES.items()}
    setup=profit_setup(spec)
    output.mkdir(parents=True,exist_ok=False)
    plans,_=build_plan('all')
    details=[]
    for plan in plans:
        slug=plan['event_slug']
        source=dataset_dir/'events'/slug/'signals.parquet'
        if not source.exists():
            continue
        signals=pd.read_parquet(source)
        if signals.empty:
            continue
        eligible={time for event,time in choices['all_events']['monitor_all'] if event == slug}
        signals=signals.loc[signals.time.isin(eligible)]
        if signals.empty:
            continue
        print(f'Profitability policy replay: {slug}',flush=True)
        groups,mapping=observation_groups(plan,load_frames(plan),set(signals.pair_id))
        cache={}
        for scope,policies in choices.items():
            for model,selected in policies.items():
                times=frozenset(time for event,time in selected if event == slug)
                if times not in cache:
                    cache[times]=run_event(signals.loc[signals.time.isin(times)],groups,mapping,setup)[0]
                if len(cache[times]):
                    details.append(cache[times].assign(model=model,scope=scope))
        if details:
            pd.concat(details,ignore_index=True).to_parquet(output/'attempts.parquet',index=False)
    attempts=pd.concat(details,ignore_index=True) if details else pd.DataFrame()
    if not details:
        attempts.to_parquet(output/'attempts.parquet',index=False)
    rows=[]
    intervals=[('all',FOLDS[0][1],FOLDS[-1][2])]+FOLDS
    for scope,policies in choices.items():
        for model,selected in policies.items():
            part=attempts.loc[attempts.scope.eq(scope)&attempts.model.eq(model)] if len(attempts) else attempts
            for fold,start,end in intervals:
                lo,hi=pd.Timestamp(start,tz='UTC'),pd.Timestamp(end,tz='UTC')
                subset=part.loc[part.decision_time.ge(lo)&part.decision_time.lt(hi)] if len(part) else part
                rows.append(dict(scope=scope,fold=fold,top_n=top_n if model != 'monitor_all' else None,
                    monitored_event_windows=sum(lo <= time < hi for event,time in selected),
                    **economic_summary(subset,model,spec)))
    summary=pd.DataFrame(rows)
    summary.to_csv(output/'summary.csv',index=False)
    # Exploratory paired family resampling; reuse actual policy returns, not labels.
    population=prediction.drop_duplicates(['event_slug','time'])
    family_map=population.drop_duplicates('event_slug').set_index('event_slug').family
    comparisons=[]
    for scope,policies in choices.items():
        families=sorted(population.loc[population.event_slug.ne(SCOPES[scope]) if SCOPES[scope] else np.ones(len(population),dtype=bool),'family'].unique())
        totals={}
        unknowns={}
        for model in policies:
            part=attempts.loc[attempts.scope.eq(scope)&attempts.model.eq(model)&attempts.attempted].copy()
            part['family']=part.event_slug.map(family_map)
            part['net']=part.pnl_usd-spec.assumed_other_cost_usd
            totals[model]=part.groupby('family').net.sum().reindex(families,fill_value=0.).to_numpy()
            unknowns[model]=int(part.pnl_usd.isna().sum())
        draws=np.random.default_rng(SEED).integers(0,len(families),size=(2000,len(families)))
        for model in policies:
            diff=totals[model]-totals['quoted_gap_rule']
            values=diff[draws].sum(axis=1)
            comparisons.append(dict(scope=scope,model=model,reference='quoted_gap_rule',
                pnl_difference_usd=float(diff.sum()),lower95=float(np.quantile(values,.025)),upper95=float(np.quantile(values,.975)),
                unknown_attempts=unknowns[model],reference_unknown_attempts=unknowns['quoted_gap_rule'],families=len(families)))
    pd.DataFrame(comparisons).to_csv(output/'paired_comparisons.csv',index=False)
    (output/'run.json').write_text(json.dumps(dict(predictions_sha256=sha256(models_dir/'predictions.parquet'),
        examples_sha256=sha256(dataset_dir/'examples.parquet'),top_n=top_n,scopes=SCOPES,
        execution=design['dataset_design']['execution'],source_sha256=sha256(__file__),final_lockbox_opened=False,
        exclusion='Post-hoc concentration sensitivity: remove December Fed from the ranking pool and reallocate slots using the same fitted models. Family-holdout models separately assess transfer.',
        uncertainty='Exploratory paired family bootstrap of actual known replay P&L; not adjusted for multiple comparisons.',
        interpretation='Persistent per-event reservations across windows/folds. Unknown P&L excluded and counted. Unlimited funding, unmeasured latency and displayed-depth fills remain assumptions.'),indent=2)+'\n')
    return summary
