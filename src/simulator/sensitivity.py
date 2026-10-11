"""Execution timing scenarios on fixed out-of-sample rankings and entry signals."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import pandas as pd
from src.common import sha256
from src.config import SPEC, spec_hash
from src.data.io import load_frames, observation_groups
from src.data.universe import build_plan
from src.models.evaluation import ranked
from src.models.settings import MODEL_NAMES, method_name, ensemble_name
from src.simulator.execution import experiment
from src.simulator.scheduler import run_event

SCENARIOS = {
    'arrival_500ms': (.5, 1.),
    'arrival_100ms': (.1, 1.),
    'arrival_50ms': (.05, 1.),
    'arrival_10ms': (.01, 1.),
    'arrival_0ms': (0., 1.),
    'fast_unwind_100ms': (.5, .1),
    'instant_no_hold': (0., 0.),
}
POLICIES = [method_name(name, view) for view in ('book', 'combined') for name in MODEL_NAMES]
POLICIES += [ensemble_name(view) for view in ('book', 'combined')] + ['quoted_gap_rule', 'monitor_all']


def summarize(details, model, scenario, delay, unwind, other_cost=0.):
    taken = details.loc[details.attempted] if len(details) else details
    complete = taken.loc[taken.status.eq('complete')] if len(taken) else taken
    partial = taken.loc[taken.status.str.startswith('partial')] if len(taken) else taken
    return dict(model=model, scenario=scenario, arrival_delay_s=delay, unwind_delay_s=unwind,
        attempts=len(taken), completed=len(complete), partial_attempts=len(partial),
        unknown_attempts=int(taken.pnl_usd.isna().sum()) if len(taken) else 0,
        completed_profit_usd=float(complete.pnl_usd.sum()) if len(complete) else 0.,
        partial_pnl_usd=float(partial.pnl_usd.sum()) if len(partial) else 0.,
        known_pnl_usd=float(taken.pnl_usd.sum()) - len(taken)*other_cost if len(taken) else 0.)


def replay_timing(output, labels_dir, models_dir, spec=SPEC, top_n=5):
    """Keep predictions/labels fixed; each policy/scenario gets a fresh ledger.

    Zero-delay results assume access to the aligned displayed books at signal
    time. They are optimistic timing benchmarks within the same capped strategy,
    not a market-wide capacity bound or measured HFT performance.
    """
    output, labels_dir, models_dir = Path(output), Path(labels_dir), Path(models_dir)
    design = json.loads((models_dir/'design.json').read_text())
    if design['label_sha256'] != sha256(labels_dir/'labels.parquet') or spec_hash(spec) != spec_hash_from_contract(design['contract']):
        raise ValueError('Timing comparison requires the unchanged model/label contract')
    prediction = pd.read_parquet(models_dir/'predictions.parquet')
    if prediction.time.min() < pd.Timestamp(spec.development_start) or prediction.time.max() >= pd.Timestamp(spec.lockbox_start):
        raise ValueError('Timing comparison is development-only')
    choices = {}
    for name in POLICIES[:-1]:
        frame = prediction.loc[prediction.model.eq(name)]
        if frame.empty:
            raise ValueError(f'Missing policy: {name}')
        selected = ranked(frame, frame.score, top_n)
        choices[name] = set(zip(selected.event_slug, selected.time))
    pool = prediction.drop_duplicates(['event_slug','time'])
    choices['monitor_all'] = set(zip(pool.event_slug, pool.time))
    output.mkdir(parents=True, exist_ok=False)
    details = []
    plans, _ = build_plan('all')
    for plan in plans:
        slug = plan['event_slug']
        path = labels_dir/'events'/slug/'signals.parquet'
        if not path.exists():
            continue
        signals = pd.read_parquet(path)
        if signals.empty:
            continue
        eligible = {time for event,time in choices['monitor_all'] if event == slug}
        signals = signals.loc[signals.time.isin(eligible)]
        if signals.empty:
            continue
        print(f'Timing scenarios: {slug}', flush=True)
        groups, mapping = observation_groups(plan, load_frames(plan), set(signals.pair_id))
        for scenario, (delay, unwind) in SCENARIOS.items():
            setup = replace(experiment(spec), delay_s=delay, unwind_delay_s=unwind)
            cache = {}
            for model, selected in choices.items():
                times = frozenset(time for event,time in selected if event == slug)
                if times not in cache:
                    cache[times] = run_event(signals.loc[signals.time.isin(times)], groups, mapping, setup)[0]
                if len(cache[times]):
                    details.append(cache[times].assign(model=model, scenario=scenario))
        if details:
            pd.concat(details, ignore_index=True).to_parquet(output/'attempts.parquet', index=False)
    combined = pd.concat(details, ignore_index=True) if details else pd.DataFrame()
    rows = []
    for scenario, (delay, unwind) in SCENARIOS.items():
        for model in choices:
            part = combined.loc[combined.model.eq(model) & combined.scenario.eq(scenario)] if len(combined) else combined
            rows.append(summarize(part, model, scenario, delay, unwind, spec.assumed_other_cost_usd))
    summary = pd.DataFrame(rows)
    summary.to_csv(output/'summary.csv', index=False)
    (output/'run.json').write_text(json.dumps(dict(scenarios=SCENARIOS, policies=POLICIES,
        contract=asdict(spec), top_n=top_n, predictions_sha256=sha256(models_dir/'predictions.parquet'),
        label_sha256=sha256(labels_dir/'labels.parquet'), source_sha256=sha256(__file__),
        labels_rebuilt=False, models_retrained=False, final_lockbox_opened=False,
        interpretation='Fixed-selection execution sensitivity; zero delay assumes displayed liquidity is attainable. No measured feed/network latency. Same discovery, sizes, cash cap, entry rules, and quote reservations. Unwind delay also controls the conservative post-attempt hold and reserved quote version, even after complete fills.'), indent=2)+'\n')
    return summary


def spec_hash_from_contract(contract):
    from src.config import ScreeningSpec
    return spec_hash(ScreeningSpec(**contract))
