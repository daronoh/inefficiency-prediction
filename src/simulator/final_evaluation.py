"""Final-only persistent policy replay; frozen predictions and execution rules."""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.common import sha256
from src.config import ScreeningSpec
from src.data.final_evaluation import FinalGroup, load_final_frames, final_plans, verify as verify_data
from src.paths import PROJECT_ROOT, resolve_data_path
from src.reporting.final_protocol import verify as verify_protocol
from src.simulator.final_execution import run_event
from src.simulator.opportunities import setup, funding_metrics
from src.simulator.profitability import economic_summary


ATTEMPT_COLUMNS = ['event_slug','window','decision_time','start_time','confirmation_time','arrival_time',
    'unwind_time','status','attempted','quantity','n_members','n_filled_legs','quoted_gross_usd',
    'reserved_cash_usd','limits_json','entry_cost_usd','pnl_usd','filled_legs_json','leg_costs_json','pair_id',
    'scope','top_n','model','arrival_delay_s']


def verify_files(folder):
    folder = Path(folder)
    complete = json.loads((folder/'complete.json').read_text())
    for name,digest in complete['files_sha256'].items():
        if sha256(folder/name) != digest:
            raise ValueError(f'Final artifact changed: {name}')
    for name,digest in json.loads((folder/'design.json').read_text())['source_sha256'].items():
        if sha256(PROJECT_ROOT/name) != digest:
            raise ValueError(f'Final stage source changed: {name}')
    return complete


def scenario_delays(scope, protocol):
    return [protocol['economic']['primary_arrival_s']] if scope.startswith('leave_out:') else protocol['latency_sensitivity_s']


def summarize(attempts, choices, protocol):
    spec = ScreeningSpec(development_start=protocol['period']['start'],lockbox_start=protocol['period']['end_exclusive'])
    rows = []
    groups = attempts.groupby(['scope','top_n','model','arrival_delay_s']) if len(attempts) else None
    for (scope,n,model),selected in choices.groupby(['scope','top_n','model']):
        for delay in scenario_delays(scope,protocol):
            key = (scope,n,model,delay)
            part = groups.get_group(key) if groups is not None and key in groups.groups else attempts.iloc[:0]
            result = economic_summary(part,model,spec)
            taken = part.loc[part.attempted] if len(part) else part
            complete = taken.loc[taken.status.eq('complete')] if len(taken) else taken
            positive = selected.loc[selected.label_known & selected.target.eq(1)]
            known_keys = set(zip(positive.event_slug,positive.time))
            attempted_keys = set(zip(taken.event_slug,taken.decision_time)) if len(taken) else set()
            complete_keys = set(zip(complete.event_slug,complete.decision_time)) if len(complete) else set()
            result.update(scope=scope,top_n=n,fold='final',scenario=f'arrival_{int(delay*1000)}ms',arrival_delay_s=delay,
                monitored_event_windows=len(selected),selected_unknown_windows=int((~selected.label_known).sum()),
                known_positive_hours_selected=len(known_keys),positive_hours_with_attempts=len(known_keys&attempted_keys),
                positive_hours_with_completions=len(known_keys&complete_keys),
                opportunity_hour_to_attempt_rate=len(known_keys&attempted_keys)/len(known_keys) if known_keys else np.nan,
                opportunity_hour_to_completion_rate=len(known_keys&complete_keys)/len(known_keys) if known_keys else np.nan,
                completion_rate=len(complete)/len(taken) if len(taken) else np.nan,
                no_fill_attempts=int(taken.status.eq('no_fill').sum()) if len(taken) else 0,
                **funding_metrics(part))
            rows.append(result)
    return pd.DataFrame(rows)


def replay(output, data, models):
    output,data,models = map(Path,(output,data,models))
    data_design = verify_data(data)
    verify_files(models)
    protocol = verify_protocol(PROJECT_ROOT/'results/final_protocol',PROJECT_ROOT/'results/final_coverage')
    selected = pd.read_parquet(models/'selections.parquet')
    context_end = pd.Timestamp(data_design['context_end'])
    end = pd.Timestamp(protocol['period']['end_exclusive'])
    start = pd.Timestamp(protocol['period']['start'])
    if selected.time.lt(start).any() or selected.time.ge(end).any():
        raise ValueError('Selections outside fixed final interval')
    inventory = pd.read_csv(PROJECT_ROOT/'results/final_coverage/token_inventory.csv',dtype={'token_id':str}).set_index('token_id')
    raw = pd.read_csv(data/'input_inventory.csv',dtype={'token_id':str})
    if raw.groupby('token_id').event_slug.nunique().gt(1).any():
        raise ValueError('Shared tokens need a cross-event liquidity ledger')
    for row in raw.itertuples():
        if sha256(resolve_data_path(row.path)) != row.input_sha256:
            raise ValueError('Raw final input changed')
    output.mkdir(parents=True,exist_ok=False)
    (output/'events').mkdir()
    design = dict(version='final_replay_v1',protocol_sha256=sha256(PROJECT_ROOT/'results/final_protocol/protocol.json'),
        dataset_completion_sha256=sha256(data/'complete.json'),models_completion_sha256=sha256(models/'complete.json'),
        source_sha256={**data_design['source_sha256'],
            **{p:sha256(PROJECT_ROOT/p) for p in ['src/simulator/final_evaluation.py','src/simulator/final_execution.py']}},
        execution=protocol['execution'],models_retrained=False,
        final_outcomes_opened=True,ledger_initialisation='empty at final start, persistent across all final hours')
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    details,audits,cache_audit = [],[],[]
    for plan in final_plans(protocol):
        slug = plan['event_slug']
        path = data/'events'/slug/'signals.parquet'
        if not path.exists():
            continue
        signals = pd.read_parquet(path)
        if signals.empty:
            continue
        print(f'Final persistent replay: {slug}',flush=True)
        frames = load_final_frames(plan,protocol,inventory)
        mapping = {f'{slug}:{i}':tuple(ids) for i,(ids,_,_) in enumerate(plan['packages'])}
        groups = {pid:FinalGroup([frames[t] for t in ids],context_end=context_end) for pid,ids in mapping.items()}
        choices = selected.loc[selected.event_slug.eq(slug)]
        available = set(signals.time)
        cache = {}
        event_details,event_audits = [],[]
        for (scope,n,model),part in choices.groupby(['scope','top_n','model']):
            times = frozenset(set(part.time)&available)
            if not times:
                continue
            for delay in scenario_delays(scope,protocol):
                key = delay,times
                if key not in cache:
                    cache[key] = run_event(signals.loc[signals.time.isin(times)],groups,mapping,setup(delay),
                        entry_end=end,context_end=context_end)
                attempts,audit = cache[key]
                tags = dict(scope=scope,top_n=n,model=model,arrival_delay_s=delay)
                if len(attempts):
                    event_details.append(attempts.assign(**tags))
                if len(audit):
                    event_audits.append(audit.assign(**tags))
        cache_audit.append(dict(event_slug=slug,unique_persistent_replays=len(cache),scenario_references=len(event_details)))
        if event_details:
            trades = pd.concat(event_details,ignore_index=True)
            trades.to_parquet(output/'events'/f'{slug}.parquet',index=False)
            details.append(trades)
        if event_audits:
            audits.append(pd.concat(event_audits,ignore_index=True))
    attempts = pd.concat(details,ignore_index=True) if details else pd.DataFrame(columns=ATTEMPT_COLUMNS)
    attempts.to_parquet(output/'attempts.parquet',index=False)
    (pd.concat(audits,ignore_index=True) if audits else pd.DataFrame()).to_parquet(output/'scheduler_audit.parquet',index=False)
    pd.DataFrame(cache_audit).to_csv(output/'cache_audit.csv',index=False)
    summary = summarize(attempts,selected,protocol)
    # At N=5 the same eligible pool must yield exactly the same economics.
    checks = []
    for (scope,delay),part in summary.loc[summary.top_n.eq(5)].groupby(['scope','arrival_delay_s']):
        numeric = ['known_pnl_usd','attempts','completed','unknown_attempts','partial_pnl_usd']
        reference = part.loc[part.model.eq('monitor_all'),numeric].iloc[0].to_numpy(dtype=float)
        if not np.allclose(part[numeric].to_numpy(dtype=float),reference,equal_nan=True):
            raise ValueError('N=5 full-pool replay must be policy invariant')
        checks.append(dict(scope=scope,arrival_delay_s=delay,n5_economic_equality=True))
    summary.to_csv(output/'summary.csv',index=False)
    pd.DataFrame(checks).to_csv(output/'consistency.csv',index=False)
    files = {p.relative_to(output).as_posix():sha256(p) for p in output.rglob('*') if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(files_sha256=files,models_retrained=False,
        final_outcomes_opened=True),indent=2)+'\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data',type=Path,required=True)
    parser.add_argument('--models',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    replay(args.output,args.data,args.models)
