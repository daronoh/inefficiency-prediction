"""Extend the report using saved predictions and processed features, never books.

Run directly with --examples pointing to the earlier opportunity-value table.
Outputs are separate from the original study and carry their own input digests.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
from time import perf_counter

SUBMISSION = Path(__file__).resolve().parents[2]
if __package__ in (None, ''):
    sys.path.insert(0, str(SUBMISSION))
os.environ.setdefault('FYP_ROOT', str(SUBMISSION))

import joblib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn
from threadpoolctl import threadpool_limits

from FYP.models.estimators import predict
from FYP.models.metrics import ranked
from FYP.models.tuning import make_model
from FYP.models.uncertainty import paired_block_intervals
from FYP.models.value import value_metrics
from FYP.reporting.artifacts import LABELS

FIRST = pd.Timestamp('2025-11-11', tz='UTC')
EARLIER_END = pd.Timestamp('2026-01-08', tz='UTC')
END = pd.Timestamp('2026-02-03T08:00:00Z')
HOUR = pd.Timedelta(hours=1)
SEEDS = (20261001, 20261002, 20261003)
BEST = 'fixed_book_random_forest'
FOCUS = [BEST, 'quoted_gap_rule', 'freshness_rule', 'depth_rule',
         'fixed_book_logistic', 'tabm_two_stage']


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8')


def metrics(pool, score, n):
    return value_metrics(pool, np.asarray(score), n)


def same_targets(a, b):
    """Cross-check rows against the original predictions before any new fit."""
    joined = a.merge(b, on=['event_slug', 'time'], suffixes=('_a', '_b'),
                     how='outer', indicator=True, validate='one_to_one')
    if not joined['_merge'].eq('both').all():
        raise ValueError('Earlier example keys differ from the original study')
    for column in ['target', 'value_target', 'label_known']:
        x, y = joined[column + '_a'], joined[column + '_b']
        if not (x.eq(y) | (x.isna() & y.isna())).all():
            raise ValueError('Earlier labels differ: ' + column)


def conditional_summary(pool, selected):
    known = pool.label_known.astype(bool)
    caught = int(selected.target.eq(1).sum())
    positive = int(pool.target.eq(1).sum())
    total = float(pool.value_target.sum())
    value = float(selected.value_target.sum())
    return dict(rows=len(pool), known=int(known.sum()), positive=positive,
                captured=caught, recall=caught / positive if positive else np.nan,
                peak_value=total, captured_value=value,
                value_capture=value / total if total else np.nan,
                selected=len(selected), unknown_selected=int((~selected.label_known).sum()))


def analyse_saved(predictions, output):
    tables = output / 'tables'
    base = predictions.loc[predictions.model.eq(BEST)].copy()
    keys = ['event_slug', 'time']
    if base.duplicated(keys).any():
        raise ValueError('Duplicate original prediction rows')
    budget_rows, type_rows, event_rows, paired = [], [], [], []
    selected_by_model = {}
    for model, pool in predictions.groupby('model', sort=True):
        pool = pool.sort_values(keys).reset_index(drop=True)
        same_targets(base[keys + ['target', 'value_target', 'label_known']],
                     pool[keys + ['target', 'value_target', 'label_known']])
        for n in list(range(1, 11)) + [19]:
            budget_rows.append(dict(model=model, budget=n, **metrics(pool, pool.score, n)))
        selected = ranked(pool, pool.score, 3)
        selected_by_model[model] = selected
        for kind, part in pool.groupby('stratum'):
            chosen = selected.loc[selected.stratum.eq(kind)]
            type_rows.append(dict(model=model, stratum=kind,
                                  **conditional_summary(part, chosen)))
        for event, part in pool.groupby('event_slug'):
            chosen = selected.loc[selected.event_slug.eq(event)]
            event_rows.append(dict(model=model, event_slug=event,
                                   **conditional_summary(part, chosen)))
    budgets = pd.DataFrame(budget_rows)
    original = pd.read_csv(SUBMISSION / 'artifacts/tables/all_prediction_metrics.csv')
    for n in (3, 5):
        reference = original.loc[original.scope.eq('all_events') & original.fold.eq('all')
                                 & original.top_n.eq(n)].set_index('model')
        actual = budgets.loc[budgets.budget.eq(n)].set_index('model').loc[reference.index]
        for column in ['recall', 'peak_value_recall', 'selected', 'selected_unknown']:
            if not np.allclose(actual[column], reference[column], equal_nan=True):
                raise ValueError('Reanalysis disagrees with original metrics: ' + column)
    for model in ['quoted_gap_rule', 'freshness_rule', 'depth_rule',
                  'fixed_book_logistic', 'tabm_two_stage']:
        interval = paired_block_intervals(base, selected_by_model[BEST],
                                         selected_by_model[model])
        paired.append(dict(comparator=model, **interval))
    budgets.to_csv(tables / 'budget_curves.csv', index=False)
    pd.DataFrame(type_rows).to_csv(tables / 'capture_by_structure.csv', index=False)
    events = pd.DataFrame(event_rows)
    events.to_csv(tables / 'capture_by_event.csv', index=False)
    pd.DataFrame(paired).to_csv(tables / 'paired_strong_controls.csv', index=False)

    # Leave one event out of ranking only; these are not event-held-out refits.
    exclusion = []
    positives = base.groupby('event_slug').target.sum().sort_values(ascending=False)
    for event in positives.loc[positives.gt(0)].index:
        for model in [BEST, 'quoted_gap_rule', 'freshness_rule']:
            pool = predictions.loc[predictions.model.eq(model)
                                   & predictions.event_slug.ne(event)]
            exclusion.append(dict(excluded_event=event, model=model,
                                  **metrics(pool, pool.score, 3)))
    pd.DataFrame(exclusion).to_csv(tables / 'leave_one_event_out_reranking.csv', index=False)

    diagnostics = []
    for model in FOCUS:
        pool = predictions.loc[predictions.model.eq(model)]
        selected = selected_by_model[model]
        for label, mask in [('visible_at_scan', pool.opportunity_at_scan),
                            ('not_visible_at_scan', ~pool.opportunity_at_scan)]:
            part = pool.loc[mask]
            chosen = selected.loc[selected.opportunity_at_scan.eq(label == 'visible_at_scan')]
            diagnostics.append(dict(model=model, group=label,
                                    **conditional_summary(part, chosen)))
    pd.DataFrame(diagnostics).to_csv(tables / 'scan_visibility_capture.csv', index=False)
    print('Full-period saved-score analysis reconciled at N=3 and N=5.', flush=True)
    return budgets, events


def run_ablations(examples, predictions, output):
    examples = examples.sort_values(['time', 'event_slug']).reset_index(drop=True)
    if examples.time.max() >= EARLIER_END:
        raise ValueError('Ablation input exceeds the declared earlier segment')
    earlier = predictions.loc[predictions.model.eq(BEST) & predictions.time.lt(EARLIER_END)]
    same_targets(examples.loc[examples.time.ge(FIRST)], earlier)
    columns = [c for c in examples if c.startswith('x_')]
    current = [c for c in columns if not c.startswith(('x_snapshot_lag1_', 'x_snapshot_change1_'))]
    views = dict(full_book=columns, current_book=current,
                 quoted_gap_only=['x_best_quoted_net_usd'],
                 without_quoted_margin=[c for c in columns if 'quoted_net' not in c],
                 without_book_age=[c for c in columns if 'book_age' not in c],
                 current_gap_and_age=['x_best_quoted_net_usd', 'x_book_age_max_s'])
    if len(columns) != 33 or len(current) != 11:
        raise ValueError('Unexpected earlier feature schema')
    write_json(output / 'feature_views.json', views)
    params = dict(n_estimators=200, max_depth=10, min_samples_leaf=20, max_features='sqrt')
    all_predictions, audits = [], []
    for week, lo in enumerate(pd.date_range(FIRST, EARLIER_END, freq='7D', inclusive='left')):
        hi = min(lo + pd.Timedelta(days=7), EARLIER_END)
        train = examples.loc[examples.label_known & (examples.time + HOUR).lt(lo)]
        test = examples.loc[examples.time.ge(lo) & examples.time.lt(hi)]
        if train.empty or test.empty:
            raise ValueError('Empty ablation fold')
        if not (train.time + HOUR).lt(test.time.min()).all():
            raise ValueError('Overlapping training labels')
        for seed in SEEDS:
            for view, features in views.items():
                checkpoint = output / 'ablation_checkpoints' / f'week_{week:02d}_{seed}_{view}'
                checkpoint.parent.mkdir(parents=True, exist_ok=True)
                scores_path = checkpoint.with_suffix('.parquet')
                audit_path = checkpoint.with_suffix('.json')
                if scores_path.exists() and audit_path.exists():
                    audit = json.loads(audit_path.read_text())
                    if digest(scores_path) != audit['predictions_sha256']:
                        raise ValueError('Changed ablation checkpoint')
                    scored = pd.read_parquet(scores_path)
                else:
                    started = perf_counter()
                    model = make_model('random_forest', 'classifier', params,
                                       train.target.astype(int), features, [])
                    model[-1].set_params(random_state=seed)
                    model.fit(train[features], train.target.astype(int))
                    score = predict(model, test[features])
                    if not np.isfinite(score).all():
                        raise ValueError('Nonfinite ablation score')
                    keep = ['event_slug', 'stratum', 'time', 'label_known', 'target',
                            'value_target', 'coverage_fraction', 'future_positive',
                            'opportunity_at_scan', 'x_active_tokens']
                    scored = test[keep].assign(score=score, view=view, seed=seed, fold=f'week_{week:02d}')
                    scored.to_parquet(scores_path, index=False)
                    audit = dict(view=view, seed=seed, fold=f'week_{week:02d}', start=lo, end=hi,
                                 train_rows=len(train), train_positive=int(train.target.sum()),
                                 train_last=str(train.time.max()),
                                 last_label_end=str(train.time.max() + HOUR), test_rows=len(test),
                                 known_test=int(test.label_known.sum()), features=features,
                                 parameters=params, seconds=perf_counter() - started,
                                 predictions_sha256=digest(scores_path))
                    write_json(audit_path, audit)
                all_predictions.append(scored)
                audits.append(audit)
            print(f'Ablations week {week:02d}, seed {seed}: six feature views complete.', flush=True)
    scored = pd.concat(all_predictions, ignore_index=True)
    scored.to_parquet(output / 'ablation_predictions.parquet', index=False)
    pd.DataFrame(audits).to_csv(output / 'tables/ablation_fit_audit.csv', index=False)
    rows = []
    for (view, seed), pool in scored.groupby(['view', 'seed']):
        for n in (3, 5):
            rows.append(dict(view=view, seed=seed, budget=n,
                             features=len(views[view]), **metrics(pool, pool.score, n)))
    for model in ['quoted_gap_rule', 'freshness_rule', 'depth_rule']:
        pool = predictions.loc[predictions.model.eq(model) & predictions.time.lt(EARLIER_END)]
        for n in (3, 5):
            rows.append(dict(view=model, seed=-1, budget=n, features=1,
                             **metrics(pool, pool.score, n)))
    results = pd.DataFrame(rows)
    results.to_csv(output / 'tables/ablation_metrics.csv', index=False)
    main = scored.loc[scored.view.eq('full_book') & scored.seed.eq(SEEDS[0])]
    match = main.merge(earlier[['event_slug', 'time', 'score']], on=['event_slug', 'time'],
                       suffixes=('_new', '_saved'), validate='one_to_one')
    write_json(output / 'ablation_reconciliation.json',
               dict(rows=len(match), maximum_score_difference=float(
                   (match.score_new - match.score_saved).abs().max()),
                    note='Scores may differ across library versions; labels and row keys must match exactly.'))
    return results


def figures(output, budgets, events, ablations):
    directory = output / 'figures'
    directory.mkdir(exist_ok=True)
    catalog = []
    def save(number, name, title, caption, source, fig):
        fig.savefig(directory / (name + '.png'), dpi=180)
        plt.close(fig)
        catalog.append(dict(number=number, title=title,
                            file=f'extensions/processed_review_v1/figures/{name}.png',
                            source_table=f'extensions/processed_review_v1/tables/{source}', caption=caption))
    fig, axes = plt.subplots(1, 2, figsize=(12, 4), layout='constrained')
    for model in FOCUS:
        rows = budgets.loc[budgets.model.eq(model) & budgets.budget.le(10)]
        axes[0].plot(rows.budget, 100 * rows.recall, marker='.', label=LABELS[model])
        axes[1].plot(rows.budget, 100 * rows.peak_value_recall, marker='.')
    for ax, label in zip(axes, ['Known positive hours captured (%)', 'Known quoted value captured (%)']):
        ax.set_xlabel('Monitoring slots per scan'); ax.set_ylabel(label); ax.set_xticks(range(1, 11))
        ax.axvline(3, color='gray', ls=':', lw=.8)
    axes[0].legend(fontsize=7)
    save(101, 'budget_curves', 'Opportunity capture across monitoring budgets',
         'Full-period saved predictions are reranked; these budgets have not been replayed economically.',
         'budget_curves.csv', fig)
    weekly = pd.read_csv(SUBMISSION / 'artifacts/tables/weekly_prediction.csv')
    weekly = weekly.loc[weekly.top_n.eq(3)]
    fig, axes = plt.subplots(2, 1, figsize=(11, 6), sharex=True, layout='constrained')
    for model in FOCUS[:4]:
        rows = weekly.loc[weekly.model.eq(model)].sort_values('fold')
        x = rows.fold.str.replace('week_', '').astype(int)
        axes[0].plot(x, 100 * rows.recall, marker='.', label=LABELS[model])
        axes[1].plot(x, 100 * rows.peak_value_recall, marker='.')
    axes[0].set_ylabel('Positive capture (%)'); axes[0].legend(fontsize=8)
    axes[1].set_ylabel('Value capture (%)'); axes[1].set_xlabel('Weekly evaluation block (0 begins 11 November 2025)')
    save(102, 'weekly_controls', 'Weekly capture against simple controls',
         'N=3; gaps indicate an undefined denominator rather than zero capture.',
         'weekly_controls.csv', fig)
    weekly.loc[weekly.model.isin(FOCUS[:4])].to_csv(output / 'tables/weekly_controls.csv', index=False)
    fig, ax = plt.subplots(figsize=(11, 5), layout='constrained')
    top = events.loc[events.model.eq(BEST) & events.positive.gt(0)].sort_values('peak_value', ascending=False)
    for model in FOCUS[:4]:
        rows = events.loc[events.model.eq(model)].set_index('event_slug').reindex(top.event_slug)
        ax.plot(range(len(rows)), 100 * rows.recall, marker='o', label=LABELS[model])
    ax.set_xticks(range(len(top)), top.event_slug.str.replace('-', ' '), rotation=65, ha='right', fontsize=7)
    ax.set_ylabel('Known positive hours captured (%)'); ax.legend(fontsize=8)
    save(103, 'event_controls', 'Capture across opportunity-bearing events',
         'N=3; events are ordered by quoted-value contribution. Each event uses its own positive-hour denominator.',
         'capture_by_event.csv', fig)
    summary = ablations.loc[ablations.budget.eq(3) & ablations.seed.ne(-1)].groupby('view').agg(
        recall=('recall', 'mean'), recall_min=('recall', 'min'), recall_max=('recall', 'max'),
        value=('peak_value_recall', 'mean'), value_min=('peak_value_recall', 'min'), value_max=('peak_value_recall', 'max')).reset_index()
    summary.to_csv(output / 'tables/ablation_seed_summary.csv', index=False)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), layout='constrained')
    for ax, column, label in zip(axes, ['recall', 'value'], ['Positive capture (%)', 'Value capture (%)']):
        x = np.arange(len(summary)); mean = summary[column].to_numpy()
        error = np.array([mean - summary[column + '_min'], summary[column + '_max'] - mean])
        ax.barh(x, 100 * mean, xerr=100 * error, capsize=3)
        ax.set_yticks(x, summary.view.str.replace('_', ' '), fontsize=8)
        ax.set_xlabel(label); ax.invert_yaxis()
    save(104, 'feature_ablations', 'Earlier-period forest feature ablations',
         '11 November 2025 to 8 January 2026, N=3. Bars show three-seed means; whiskers show seed minima and maxima, not confidence intervals.',
         'ablation_metrics.csv', fig)
    pd.DataFrame(catalog).to_csv(output / 'figure_index.csv', index=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--examples', type=Path, required=True)
    parser.add_argument('--output', type=Path,
                        default=SUBMISSION / 'artifacts/extensions/processed_review_v1')
    args = parser.parse_args()
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=True)
    (output / 'tables').mkdir(exist_ok=True)
    predictions_path = SUBMISSION / 'artifacts/models/predictions.parquet'
    paths = [Path(__file__), args.examples, predictions_path,
             SUBMISSION / 'inputs/study.json', SUBMISSION / 'FYP/models/classification.py',
             SUBMISSION / 'FYP/models/estimators.py', SUBMISSION / 'FYP/models/tuning.py']
    design = dict(input_sha256={str(p.resolve()): digest(p) for p in paths},
                  ablation_start=str(FIRST), ablation_end_exclusive=str(EARLIER_END),
                  seeds=SEEDS, family='Fixed random forest; no new hyperparameter selection',
                  original_study_preserved=True, raw_orderbooks_used=False,
                  interpretation='Exploratory analysis of inspected data; no independent final test and no new execution replay.',
                  python=platform.python_version(), numpy=np.__version__, sklearn=sklearn.__version__)
    normalized = json.loads(json.dumps(design))
    if (output / 'design.json').exists() and json.loads((output / 'design.json').read_text()) != normalized:
        raise ValueError('Extension inputs or source changed; choose a new output directory')
    write_json(output / 'design.json', design)
    predictions = pd.read_parquet(predictions_path)
    examples = pd.read_parquet(args.examples)
    budgets, events = analyse_saved(predictions, output)
    with threadpool_limits(limits=4):
        ablations = run_ablations(examples, predictions, output)
    figures(output, budgets, events, ablations)
    write_json(output / 'complete.json', dict(files_sha256={
        p.relative_to(output).as_posix(): digest(p) for p in output.rglob('*')
        if p.is_file() and p.name != 'complete.json'}))
    print('Processed-data extension complete:', output, flush=True)


if __name__ == '__main__':
    main()
