"""Exploratory history, family-transfer and balanced-metric analyses without books.

Run from the repository root with its virtual-environment Python. Original study
artifacts are inputs only; this experiment owns a separate, resumable directory.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import sys
from time import perf_counter

SUBMISSION = Path(__file__).resolve().parents[2]
if __package__ in (None, ''):
    sys.path.insert(0, str(SUBMISSION))
os.environ.setdefault('FYP_ROOT', str(SUBMISSION))

import numpy as np
import pandas as pd
import sklearn
from threadpoolctl import threadpool_limits

from FYP.models.estimators import predict
from FYP.models.features import family
from FYP.models.metrics import ranked
from FYP.models.tuning import candidates, make_model
from FYP.models.value import value_metrics

FIRST = pd.Timestamp('2025-11-11', tz='UTC')
EARLIER_END = pd.Timestamp('2026-01-08', tz='UTC')
HOUR = pd.Timedelta(hours=1)
SEEDS = (20261001, 20261002, 20261003)
BEST = 'fixed_book_random_forest'
HISTORY = ('history_event_frequency', 'history_event_mean_value',
           'history_last_known_value', 'history_family_mean_value')
KEYS = ['event_slug', 'time']
KEEP = ['event_slug', 'stratum', 'time', 'label_known', 'target', 'value_target',
        'coverage_fraction', 'future_positive', 'opportunity_at_scan', 'x_active_tokens']


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8')


def check_targets(left, right):
    joined = left.merge(right, on=KEYS, how='outer', suffixes=('_a', '_b'),
                        validate='one_to_one', indicator=True)
    if not joined['_merge'].eq('both').all():
        raise ValueError('Candidate keys differ')
    for name in ('label_known', 'target', 'value_target'):
        a, b = joined[name + '_a'], joined[name + '_b']
        if not (a.eq(b) | (a.isna() & b.isna())).all():
            raise ValueError('Changed labels: ' + name)


def historical_scores(timeline, pool, alpha=24.0):
    """Update only on known labels whose complete one-hour window ends before t.

    Historical availability here assumes immediate label processing once its
    window has ended. It is an offline benchmark, not a deployed feedback feed.
    Unknown windows contribute neither zeros nor counts. Last-known value does
    include observed zeros. Smoothing strengths are fixed before running.
    """
    history = timeline.loc[timeline.label_known].sort_values(['time', 'event_slug'])
    if not history.target.isin([0, 1]).all() or not np.isfinite(history.value_target).all():
        raise ValueError('Invalid known historical targets')
    available = history.assign(_end=history.time + HOUR).to_dict('records')
    events, families = defaultdict(lambda: [0, 0.0, 0.0]), defaultdict(lambda: [0, 0.0, 0.0])
    total = [0, 0.0, 0.0]
    last, index, maximum = {}, 0, None
    scored, audits = [], []
    for time, part in pool.sort_values(['time', 'event_slug']).groupby('time', sort=True):
        while index < len(available) and available[index]['_end'] < time:
            row = available[index]
            for counts in (total, events[row['event_slug']], families[family(row['event_slug'])]):
                counts[0] += 1
                counts[1] += row['target']
                counts[2] += row['value_target']
            last[row['event_slug']] = row['value_target']
            maximum = row['_end']
            index += 1
        gp = total[1] / total[0] if total[0] else 0.0
        gv = total[2] / total[0] if total[0] else 0.0
        rows = []
        for event in part.event_slug:
            f, e = families[family(event)], events[event]
            fp = (f[1] + alpha * gp) / (f[0] + alpha)
            fv = (f[2] + alpha * gv) / (f[0] + alpha)
            rows.append([(e[1] + alpha * fp) / (e[0] + alpha),
                         (e[2] + alpha * fv) / (e[0] + alpha), last.get(event, fv), fv,
                         e[0], f[0]])
        frame = part.copy()
        frame[list(HISTORY) + ['event_history_count', 'family_history_count']] = rows
        scored.append(frame)
        audits.append(dict(time=time, known_history_rows=total[0],
                           maximum_label_end=maximum, candidate_rows=len(part)))
    return pd.concat(scored, ignore_index=True), pd.DataFrame(audits)


def summarize(pool, selected):
    known = pool.label_known.astype(bool)
    positive = int((known & pool.target.eq(1)).sum())
    value = float(pool.loc[known, 'value_target'].sum())
    caught = int((selected.label_known & selected.target.eq(1)).sum())
    captured = float(selected.loc[selected.label_known, 'value_target'].sum())
    return dict(rows=len(pool), known=int(known.sum()), positive=positive, captured=caught,
                recall=caught / positive if positive else np.nan, value=value,
                captured_value=captured, value_capture=captured / value if value else np.nan,
                selected=len(selected), selected_unknown=int((~selected.label_known).sum()))


def balanced_analysis(predictions, output):
    """Keep global N=3 selection; only change aggregation of the same outcomes."""
    event_rows, family_rows, pooled_rows = [], [], []
    base = predictions.loc[predictions.model.eq(BEST)]
    for model, pool in predictions.groupby('model', sort=True):
        check_targets(base[KEYS + ['label_known', 'target', 'value_target']],
                      pool[KEYS + ['label_known', 'target', 'value_target']])
        chosen = ranked(pool, pool.score, 3)
        pooled_rows.append(dict(model=model, **summarize(pool, chosen)))
        for event, part in pool.groupby('event_slug'):
            event_rows.append(dict(model=model, event_slug=event, family=family(event),
                                   **summarize(part, chosen.loc[chosen.event_slug.eq(event)])))
        for name, part in pool.groupby('family'):
            family_rows.append(dict(model=model, family=name,
                                    **summarize(part, chosen.loc[chosen.family.eq(name)])))
    events, families = pd.DataFrame(event_rows), pd.DataFrame(family_rows)
    events.to_csv(output / 'tables/balanced_by_event.csv', index=False)
    families.to_csv(output / 'tables/balanced_by_family.csv', index=False)
    summaries = []
    for row in pooled_rows:
        ev, fa = events.loc[events.model.eq(row['model'])], families.loc[families.model.eq(row['model'])]
        summaries.append(dict(**row, positive_events=int(ev.positive.gt(0).sum()),
                              positive_families=int(fa.positive.gt(0).sum()),
                              macro_event_recall=ev.recall.mean(),
                              macro_event_value=ev.value_capture.mean(),
                              macro_family_recall=fa.recall.mean(),
                              macro_family_value=fa.value_capture.mean()))
    pd.DataFrame(summaries).to_csv(output / 'tables/balanced_summary.csv', index=False)
    # Paired draws preserve the same events across the forest and comparator.
    # Family resampling retains ALL positive events within each sampled family,
    # and recomputes the event mean: same estimand, coarser dependence unit.
    ev = events.loc[events.positive.gt(0)]
    names = sorted(ev.event_slug.unique())
    event_family = ev.drop_duplicates('event_slug').set_index('event_slug').loc[names, 'family']
    fam_names = sorted(event_family.unique())
    rng = np.random.default_rng(20261102)
    draws = rng.integers(len(names), size=(10000, len(names)))
    cluster_draws = rng.integers(len(fam_names), size=(10000, len(fam_names)))
    membership = np.array([event_family.eq(f).to_numpy(dtype=int) for f in fam_names])
    weights = membership[cluster_draws].sum(axis=1)
    intervals = []
    for comparator in ['quoted_gap_rule', 'freshness_rule', 'fixed_book_logistic',
                       'tabm_two_stage', *HISTORY]:
        for metric in ('recall', 'value_capture'):
            wide = ev.pivot(index='event_slug', columns='model', values=metric).loc[names]
            difference = (wide[BEST] - wide[comparator]).to_numpy()
            for unit, boot in [('event', difference[draws].mean(axis=1)),
                               ('family', (weights * difference).sum(axis=1) / weights.sum(axis=1))]:
                lo, hi = np.quantile(boot, [.025, .975])
                intervals.append(dict(comparator=comparator, metric=metric, resampling_unit=unit,
                                      difference=difference.mean(), lower95=lo, upper95=hi,
                                      events=len(names), families=len(fam_names), draws=10000))
    pd.DataFrame(intervals).to_csv(output / 'tables/balanced_intervals.csv', index=False)


def family_refits(examples, predictions, ablations, output):
    features = [c for c in examples if c.startswith('x_')]
    if len(features) != 33:
        raise ValueError('Expected the retained 33-feature table')
    examples = examples.assign(family=examples.event_slug.map(family))
    families = sorted(examples.family.unique())  # Includes zero-positive families.
    audits, frames = [], []
    for week, origin in enumerate(pd.date_range(FIRST, EARLIER_END, freq='7D', inclusive='left')):
        end = min(origin + pd.Timedelta(days=7), EARLIER_END)
        eligible = examples.loc[examples.time.ge(origin) & examples.time.lt(end)]
        past = examples.loc[examples.label_known & (examples.time + HOUR).lt(origin)]
        for withheld in families:
            if not eligible.family.eq(withheld).any():
                audits.append(dict(fold=f'week_{week:02d}', withheld_family=withheld,
                                   status='no target-family candidates in this block'))
                continue
            train = past.loc[past.family.ne(withheld)]
            if train.empty or train.family.eq(withheld).any() or not (train.time + HOUR).lt(origin).all():
                raise ValueError('Invalid family-withheld training split')
            for algorithm, seeds in [('random_forest', SEEDS), ('logistic', SEEDS[:1])]:
                for seed in seeds:
                    key = f'week_{week:02d}_{withheld}_{algorithm}_{seed}'
                    score_path = output / 'checkpoints' / (key + '.parquet')
                    audit_path = score_path.with_suffix('.json')
                    if score_path.exists() and audit_path.exists():
                        audit = json.loads(audit_path.read_text())
                        if digest(score_path) != audit['predictions_sha256']:
                            raise ValueError('Changed family-transfer checkpoint: ' + key)
                        frame = pd.read_parquet(score_path)
                    else:
                        started = perf_counter()
                        params = candidates(algorithm, 'classifier')[0]
                        model = make_model(algorithm, 'classifier', params, train.target.astype(int), features, [])
                        if 'random_state' in model[-1].get_params():
                            model[-1].set_params(random_state=seed)
                        model.fit(train[features], train.target.astype(int))
                        scores = predict(model, eligible[features])
                        if not np.isfinite(scores).all():
                            raise ValueError('Nonfinite transfer predictions')
                        frame = eligible[KEEP + ['family']].assign(
                            score=scores, model=algorithm, seed=seed, withheld_family=withheld,
                            fold=f'week_{week:02d}')
                        frame.to_parquet(score_path, index=False)
                        audit = dict(fold=f'week_{week:02d}', origin=origin, end=end,
                                     withheld_family=withheld, model=algorithm, seed=seed,
                                     train_rows=len(train), train_positive=int(train.target.sum()),
                                     excluded_family_rows=int(past.family.eq(withheld).sum()),
                                     training_families=sorted(train.family.unique()),
                                     maximum_train_label_end=train.time.max() + HOUR,
                                     target_family_training_rows=0, test_rows=len(eligible),
                                     target_family_test_rows=int(eligible.family.eq(withheld).sum()),
                                     features=features, parameters=params, status='fitted',
                                     seconds=perf_counter() - started,
                                     predictions_sha256=digest(score_path))
                        write_json(audit_path, audit)
                    audits.append(audit)
                    frames.append(frame)
        print(f'Family-withheld week {week:02d} complete.', flush=True)
    fitted = pd.concat(frames, ignore_index=True)
    fitted.to_parquet(output / 'family_withheld_predictions.parquet', index=False)
    pd.DataFrame(audits).to_csv(output / 'tables/family_fit_audit.csv', index=False)
    results = []
    controls = predictions.loc[predictions.time.lt(EARLIER_END)]
    for withheld in families:
        pools = []
        for (model, seed), pool in fitted.loc[fitted.withheld_family.eq(withheld)].groupby(['model', 'seed']):
            pools.append(('withheld_' + model, seed, pool))
        # Unrestricted controls use all evaluation blocks. Missing family blocks
        # in a withheld model have no target-family rows, hence no missing target.
        for seed in SEEDS:
            pool = ablations.loc[ablations.view.eq('full_book') & ablations.seed.eq(seed)].copy()
            pools.append(('unrestricted_random_forest', seed, pool.assign(family=pool.event_slug.map(family))))
        for model in ['fixed_book_logistic', 'quoted_gap_rule', 'freshness_rule']:
            pools.append((model, SEEDS[0], controls.loc[controls.model.eq(model)]))
        for model, seed, pool in pools:
            chosen = ranked(pool, pool.score, 3)
            target = pool.loc[pool.family.eq(withheld)]
            target_chosen = chosen.loc[chosen.family.eq(withheld)]
            results.append(dict(withheld_family=withheld, model=model, seed=seed,
                                events=target.event_slug.nunique(), **summarize(target, target_chosen)))
    pd.DataFrame(results).to_csv(output / 'tables/family_transfer.csv', index=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=SUBMISSION / 'artifacts/extensions/robustness_review_v1')
    args = parser.parse_args()
    output = args.output.resolve()
    for name in ('tables', 'checkpoints'):
        (output / name).mkdir(parents=True, exist_ok=True)
    inputs = {
        'examples': SUBMISSION / 'artifacts/extensions/processed_review_v1/inputs/earlier_examples.parquet',
        'predictions': SUBMISSION / 'artifacts/models/predictions.parquet',
        'ablations': SUBMISSION / 'artifacts/extensions/processed_review_v1/ablation_predictions.parquet',
    }
    sources = [Path(__file__), *[SUBMISSION / ('FYP/models/' + s + '.py') for s in
               ('estimators', 'features', 'metrics', 'classification', 'value', 'tuning', 'settings')]]
    design = dict(inputs={k: dict(path=p.relative_to(SUBMISSION).as_posix(), sha256=digest(p)) for k, p in inputs.items()},
                  source_sha256={p.relative_to(SUBMISSION).as_posix(): digest(p) for p in sources},
                  first=str(FIRST), fitting_end_exclusive=str(EARLIER_END), seeds=list(SEEDS),
                  history_models=list(HISTORY), history_smoothing_pseudocount=24,
                  history_availability='known label time + 1 hour strictly before scan; update each scan',
                  family_exclusion='all broad family rows removed from training; rank the entire usual candidate pool',
                  family_models='fixed forest, three seeds; fixed logistic, one deterministic fit; no tuning',
                  balanced_estimands='macro recall and value over positive events; macro families; global N=3 selection',
                  bootstrap=dict(draws=10000, seed=20261102, units=['event', 'family'], paired=True, multiplicity_adjusted=False),
                  raw_orderbooks_used=False, original_study_preserved=True,
                  interpretation='Exploratory inspected-data analysis; no independent final test or new economic replay.',
                  versions=dict(python=platform.python_version(), numpy=np.__version__, sklearn=sklearn.__version__))
    if (output / 'design.json').exists() and json.loads((output / 'design.json').read_text()) != design:
        raise ValueError('Changed design, source or inputs; choose a new output directory')
    write_json(output / 'design.json', design)
    if not (output / 'executed_source.py').exists():
        shutil.copyfile(__file__, output / 'executed_source.py')
    predictions, examples, ablations = [pd.read_parquet(inputs[k]) for k in ('predictions', 'examples', 'ablations')]
    predictions['family'] = predictions.event_slug.map(family)
    base = predictions.loc[predictions.model.eq(BEST)].copy()
    earlier = base.loc[base.time.lt(EARLIER_END)]
    check_targets(examples.loc[examples.time.ge(FIRST), KEYS + ['label_known', 'target', 'value_target']],
                  earlier[KEYS + ['label_known', 'target', 'value_target']])
    timeline = pd.concat([examples[KEEP], base.loc[base.time.ge(EARLIER_END), KEEP]], ignore_index=True)
    if timeline.duplicated(KEYS).any():
        raise ValueError('Duplicate history records')
    scored, audit = historical_scores(timeline, base)
    if not (pd.to_datetime(audit.maximum_label_end, utc=True) < audit.time).all():
        raise ValueError('Historical information crosses its scan cutoff')
    audit.to_csv(output / 'tables/history_availability_audit.csv', index=False)
    history_predictions = pd.concat([scored[KEEP + ['family', 'fold', 'event_history_count', 'family_history_count']].assign(
        model=name, score=scored[name]) for name in HISTORY], ignore_index=True)
    history_predictions.to_parquet(output / 'historical_predictions.parquet', index=False)
    metrics = []
    for name, pool in history_predictions.groupby('model'):
        for n in (3, 5):
            metrics.append(dict(model=name, budget=n, **value_metrics(pool, pool.score, n)))
    pd.DataFrame(metrics).to_csv(output / 'tables/history_metrics.csv', index=False)
    write_json(output / 'input_reconciliation.json', dict(
        earlier_rows=len(examples), earlier_evaluation_rows=len(earlier),
        full_evaluation_rows=len(base), timeline_rows=len(timeline),
        timeline_known=int(timeline.label_known.sum()), timeline_positive=int(timeline.target.eq(1).sum()),
        full_features_available=False, full_label_timeline_reconstructed=True))
    print('Historical controls and availability audit complete.', flush=True)
    balanced_analysis(pd.concat([predictions, history_predictions], ignore_index=True), output)
    print('Event- and family-balanced evaluation complete.', flush=True)
    with threadpool_limits(limits=4):
        family_refits(examples, predictions, ablations, output)
    write_json(output / 'complete.json', dict(files_sha256={
        p.relative_to(output).as_posix(): digest(p) for p in output.rglob('*')
        if p.is_file() and p.name != 'complete.json'}))
    print('Robustness extension complete:', output, flush=True)


if __name__ == '__main__':
    main()
