"""Resource caps, earlier-only policy choice and paired failures from saved data.

This exploratory extension neither refits the predictors nor reads raw books.
Its output directory is independent of the submitted primary study.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import platform
import shutil
import sys

SUBMISSION = Path(__file__).resolve().parents[2]
if __package__ in (None, ''):
    sys.path.insert(0, str(SUBMISSION))
os.environ.setdefault('FYP_ROOT', str(SUBMISSION))

import numpy as np
import pandas as pd

from FYP.experiments.robustness_extension import check_targets, digest, summarize, write_json
from FYP.models.metrics import ranked
from FYP.models.uncertainty import paired_block_intervals

FIRST = pd.Timestamp('2025-11-11', tz='UTC')
END = pd.Timestamp('2026-02-03T08:00:00Z')
EARLIER_END = pd.Timestamp('2026-01-08', tz='UTC')
HOUR = pd.Timedelta(hours=1)
WEEK = pd.Timedelta(days=7)
BEST = 'fixed_book_random_forest'
HISTORY = 'history_event_mean_value'
SELECTOR = 'earlier_only_selector'
# Declaration order resolves ties in past captured value; never use test labels.
POLICIES = [HISTORY, BEST, 'fixed_book_logistic', 'quoted_gap_rule', 'freshness_rule']
TOKEN_BUDGETS = [10, 25, 50, 100]
WARMUP_BLOCKS = 4
KEYS = ['event_slug', 'time']
TIE_USD = 1e-9


def resource_select(pool, budget):
    """Pack whole events in score order, skipping those that exceed the remainder.

    Cost is the sum of within-event active-token counts. Tokens shared across
    events are counted separately: these are event-token observations, not a
    deduplicated count of exchange subscriptions. There is no extra event cap.
    """
    if budget <= 0:
        raise ValueError('Positive token budget required')
    costs = pool.x_active_tokens.to_numpy()
    if not np.isfinite(costs).all() or (costs <= 0).any() or not (costs == np.floor(costs)).all():
        raise ValueError('Active-token costs must be positive integer counts')
    order = pool.sort_values(['time', 'score', 'event_slug'], ascending=[True, False, True], kind='stable')
    chosen, audits = [], []
    for time, part in order.groupby('time', sort=True):
        used, count = 0, 0
        for index, cost in zip(part.index, part.x_active_tokens):
            cost = int(cost)
            if used + cost <= budget:
                chosen.append(index)
                used += cost
                count += 1
        audits.append(dict(time=time, budget=budget, used_tokens=used, selected_events=count,
                           candidate_events=len(part), candidate_tokens=int(part.x_active_tokens.sum())))
    return pool.loc[chosen].copy(), pd.DataFrame(audits)


def sequential_selection(predictions, policies=POLICIES, *, first=FIRST, end=END,
                         warmup_blocks=WARMUP_BLOCKS, n=3):
    """Choose the next week's policy using only earlier completed known outcomes.

    The first four scored weeks supply warm-up history. Candidate predictors
    retain their original earlier-only weekly fitting; this chooses a policy,
    not new model parameters. All historical candidate selections include unknowns.
    """
    pools = {m: predictions.loc[predictions.model.eq(m)].copy() for m in policies}
    base = pools[policies[0]]
    selections = {m: ranked(p, p.score, n) for m, p in pools.items()}
    frames, audits, history_audits = [], [], []
    for origin in pd.date_range(first + warmup_blocks * WEEK, end, freq='7D', inclusive='left'):
        stop = min(origin + WEEK, end)
        available = base.loc[base.label_known & base.time.ge(first) & (base.time + HOUR).lt(origin)]
        total_value = math.fsum(available.value_target)
        total_positive = int(available.target.eq(1).sum())
        rows = []
        for m in policies:
            past = selections[m]
            past = past.loc[past.label_known & past.time.ge(first) & (past.time + HOUR).lt(origin)]
            amount = math.fsum(past.value_target)
            rows.append(dict(origin=origin, candidate=m, captured_history_value=amount,
                             available_history_value=total_value,
                             history_value_capture=amount / total_value if total_value else np.nan,
                             captured_history_positives=int(past.target.eq(1).sum()),
                             available_history_positives=total_positive,
                             available_history_known=len(available),
                             maximum_history_label_end=available.time.max() + HOUR))
        maximum = max(r['captured_history_value'] for r in rows)
        selected_model = next(r['candidate'] for r in rows if maximum - r['captured_history_value'] <= TIE_USD)
        for row in rows:
            row['chosen'] = row['candidate'] == selected_model
        history_audits.extend(rows)
        block = pools[selected_model].loc[pools[selected_model].time.ge(origin) & pools[selected_model].time.lt(stop)].copy()
        if block.empty:
            raise ValueError('Missing selector evaluation block')
        picked = ranked(block, block.score, n)
        frames.append(block.assign(policy_source=selected_model, model=SELECTOR))
        audits.append(dict(origin=origin, end=stop, chosen_policy=selected_model,
                           maximum_history_label_end=available.time.max() + HOUR,
                           history_known=len(available), history_positive=total_positive,
                           history_total_value=total_value, **summarize(block, picked)))
    return pd.concat(frames, ignore_index=True), pd.DataFrame(audits), pd.DataFrame(history_audits)


def choose_cases(frame):
    """Choose disjoint, outcome-defined illustrations; never call them a test set."""
    positive = frame.loc[frame.label_known & frame.target.eq(1)]
    groups = [
        ('A', 'forest_only', positive.forest_selected & ~positive.history_selected),
        ('B', 'history_only', ~positive.forest_selected & positive.history_selected),
        ('C', 'neither', ~positive.forest_selected & ~positive.history_selected),
    ]
    cases, support = [], []
    both = positive.loc[positive.forest_selected & positive.history_selected]
    support.append(dict(group='both', positive_hours=len(both), value=both.value_target.sum()))
    for name, group, mask in groups:
        part = positive.loc[mask].sort_values(['value_target', 'time', 'event_slug'], ascending=[False, True, True])
        support.append(dict(group=group, positive_hours=len(part), value=part.value_target.sum()))
        if not part.empty:
            cases.append(part.head(1).assign(case_id=name, case_group=group))
    result = pd.concat(cases, ignore_index=True) if cases else frame.iloc[:0].assign(case_id='', case_group='')
    return result, pd.DataFrame(support)


def paired_failures(predictions, examples, output):
    forest = predictions.loc[predictions.model.eq(BEST) & predictions.time.lt(EARLIER_END)].copy()
    history = predictions.loc[predictions.model.eq(HISTORY) & predictions.time.lt(EARLIER_END)].copy()
    ordered = []
    for name, part in [('forest', forest), ('history', history)]:
        part = part.sort_values(['time', 'score', 'event_slug'], ascending=[True, False, True], kind='stable')
        part[name + '_rank'] = part.groupby('time').cumcount() + 1
        part[name + '_selected'] = part[name + '_rank'].le(3)
        part = part.rename(columns={'score': name + '_score'})
        ordered.append(part)
    combined = ordered[0].merge(ordered[1][KEYS + ['history_score', 'history_rank', 'history_selected']],
                                on=KEYS, validate='one_to_one')
    features = [c for c in examples if c.startswith('x_') and c not in combined]
    combined = combined.merge(examples[KEYS + features], on=KEYS, validate='one_to_one', how='left', indicator=True)
    if not combined['_merge'].eq('both').all():
        raise ValueError('Missing earlier features for paired analysis')
    combined = combined.drop(columns='_merge')
    cases, support = choose_cases(combined)
    cases.to_csv(output / 'tables/paired_cases.csv', index=False)
    support.to_csv(output / 'tables/paired_failure_groups.csv', index=False)
    combined.to_parquet(output / 'paired_earlier_predictions.parquet', index=False)
    pools = []
    for row in cases.itertuples():
        pools.append(combined.loc[combined.time.eq(row.time)].assign(case_id=row.case_id))
    if pools:
        pd.concat(pools, ignore_index=True).to_csv(output / 'tables/case_candidate_pools.csv', index=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=SUBMISSION / 'artifacts/extensions/allocation_review_v1')
    args = parser.parse_args()
    output = args.output.resolve()
    (output / 'tables').mkdir(parents=True, exist_ok=True)
    inputs = {
        'predictions': SUBMISSION / 'artifacts/models/predictions.parquet',
        'historical_predictions': SUBMISSION / 'artifacts/extensions/robustness_review_v1/historical_predictions.parquet',
        'examples': SUBMISSION / 'artifacts/extensions/processed_review_v1/inputs/earlier_examples.parquet',
    }
    sources = [Path(__file__), SUBMISSION / 'FYP/experiments/robustness_extension.py',
               SUBMISSION / 'FYP/models/metrics.py', SUBMISSION / 'FYP/models/uncertainty.py']
    design = dict(inputs={k: dict(path=p.relative_to(SUBMISSION).as_posix(), sha256=digest(p)) for k, p in inputs.items()},
                  source_sha256={p.relative_to(SUBMISSION).as_posix(): digest(p) for p in sources},
                  policies=POLICIES, token_budgets=TOKEN_BUDGETS, primary_token_budget=50,
                  resource_selection='descending score, event identifier ties; greedily include an event if its whole token cost fits; skip otherwise; no event-count cap',
                  resource_cost='sum of within-event active-token counts; shared tokens across events counted separately; acquisition and inference cost unmeasured',
                  first=str(FIRST), end_exclusive=str(END), weekly_policy_warmup_blocks=WARMUP_BLOCKS,
                  selection_objective='cumulative captured quoted value under global N=3 on available earlier forward outcomes',
                  label_availability='known label time + one hour strictly before selection origin',
                  selection_tie_order=POLICIES, selection_tie_tolerance_usd=TIE_USD,
                  cases='earlier feature period; largest known positive value in forest-only, history-only and neither-captured groups; tie by time then event identifier',
                  case_interpretation='outcome-selected illustrations, not representative samples or causal explanations',
                  bootstrap='existing paired weekly-block procedure; conditional on fitted models and observed selector path; not selection-adjusted inference',
                  raw_orderbooks_used=False, model_refits=0, new_economic_replay=False,
                  interpretation='Designed after earlier results were inspected; exploratory, not an untouched final test.',
                  versions=dict(python=platform.python_version(), numpy=np.__version__, pandas=pd.__version__))
    if (output / 'design.json').exists() and json.loads((output / 'design.json').read_text()) != design:
        raise ValueError('Changed source, design or inputs; use a new output directory')
    write_json(output / 'design.json', design)
    if not (output / 'executed_source.py').exists():
        shutil.copyfile(__file__, output / 'executed_source.py')
    original = pd.read_parquet(inputs['predictions'])
    historical = pd.read_parquet(inputs['historical_predictions'])
    predictions = pd.concat([original.loc[original.model.isin(POLICIES)],
                             historical.loc[historical.model.eq(HISTORY)]], ignore_index=True)
    reference = predictions.loc[predictions.model.eq(BEST)]
    for model, pool in predictions.groupby('model'):
        check_targets(reference[KEYS + ['label_known', 'target', 'value_target']],
                      pool[KEYS + ['label_known', 'target', 'value_target']])
        if not pool[KEYS + ['x_active_tokens']].set_index(KEYS).sort_index().equals(
                reference[KEYS + ['x_active_tokens']].set_index(KEYS).sort_index()):
            raise ValueError('Changed resource costs: ' + model)
    resource_metrics, resource_audits, selected_frames, resource_intervals = [], [], [], []
    for budget in TOKEN_BUDGETS:
        choices = {}
        for model in POLICIES:
            pool = predictions.loc[predictions.model.eq(model)]
            selected, audit = resource_select(pool, budget)
            choices[model] = selected
            selected_frames.append(selected.assign(token_budget=budget))
            resource_audits.append(audit.assign(model=model))
            resource_metrics.append(dict(model=model, token_budget=budget, scans=len(audit),
                                         mean_tokens=audit.used_tokens.mean(), mean_events=audit.selected_events.mean(),
                                         token_share=audit.used_tokens.sum() / audit.candidate_tokens.sum(),
                                         mean_utilization=audit.used_tokens.mean() / budget,
                                         maximum_used=int(audit.used_tokens.max()), **summarize(pool, selected)))
        for comparator in [HISTORY, 'quoted_gap_rule', 'freshness_rule', 'fixed_book_logistic']:
            resource_intervals.append(dict(token_budget=budget, comparator=comparator,
                                           **paired_block_intervals(reference, choices[BEST], choices[comparator])))
        print(f'Token-budget analysis complete: {budget}.', flush=True)
    pd.DataFrame(resource_metrics).to_csv(output / 'tables/token_budget_metrics.csv', index=False)
    pd.concat(resource_audits, ignore_index=True).to_csv(output / 'tables/token_budget_audit.csv', index=False)
    pd.concat(selected_frames, ignore_index=True).to_parquet(output / 'resource_selections.parquet', index=False)
    pd.DataFrame(resource_intervals).to_csv(output / 'tables/token_budget_intervals.csv', index=False)
    selector, audit, history_audit = sequential_selection(predictions)
    selector.to_parquet(output / 'selector_predictions.parquet', index=False)
    audit.to_csv(output / 'tables/selector_weekly.csv', index=False)
    history_audit.to_csv(output / 'tables/selector_history_audit.csv', index=False)
    start = FIRST + WARMUP_BLOCKS * WEEK
    evaluation = pd.concat([predictions.loc[predictions.time.ge(start)], selector], ignore_index=True)
    rows, selections = [], {}
    for model, pool in evaluation.groupby('model'):
        picked = ranked(pool, pool.score, 3)
        selections[model] = picked
        rows.append(dict(model=model, start=start, end=END, scans=pool.time.nunique(), **summarize(pool, picked)))
    pd.DataFrame(rows).to_csv(output / 'tables/selector_metrics.csv', index=False)
    intervals = [dict(comparator=m, **paired_block_intervals(selector, selections[SELECTOR], selections[m]))
                 for m in [BEST, HISTORY]]
    pd.DataFrame(intervals).to_csv(output / 'tables/selector_intervals.csv', index=False)
    print('Earlier-only policy selection complete.', flush=True)
    paired_failures(predictions, pd.read_parquet(inputs['examples']), output)
    write_json(output / 'complete.json', dict(files_sha256={
        p.relative_to(output).as_posix(): digest(p) for p in output.rglob('*')
        if p.is_file() and p.name != 'complete.json'}))
    print('Allocation extension complete:', output, flush=True)


if __name__ == '__main__':
    main()
