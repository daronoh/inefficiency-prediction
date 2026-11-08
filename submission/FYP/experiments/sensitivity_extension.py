"""Focused design sensitivities and cached-feature timing; never reads raw books.

The small grid is recorded before execution. Existing expanded-history fits and
the original study are read-only controls. Timings are descriptive local costs,
not measurements of a complete market-data or execution system.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import sys
from time import perf_counter, perf_counter_ns

SUBMISSION = Path(__file__).resolve().parents[2]
if __package__ in (None, ''):
    sys.path.insert(0, str(SUBMISSION))
os.environ.setdefault('FYP_ROOT', str(SUBMISSION))

import joblib
import numpy as np
import pandas as pd
import sklearn
from threadpoolctl import threadpool_limits

from FYP.experiments.robustness_extension import (
    BEST, EARLIER_END, FIRST, HOUR, KEEP, SEEDS, check_targets,
    historical_scores, summarize,
)
from FYP.models.estimators import predict
from FYP.models.metrics import ranked
from FYP.models.tuning import candidates, make_model

ALPHAS = (6.0, 24.0, 72.0)
WINDOW_DAYS = (14, 28)
TIMING_SCANS = 12
TIMING_WARMUPS = 5
TIMING_REPEATS = 30
KEYS = ['event_slug', 'time']


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8')


def training_rows(examples, origin, days):
    """A rolling window uses scan times [origin-days, origin), with purged labels."""
    if days <= 0:
        raise ValueError('Training window must be positive')
    return examples.loc[
        examples.label_known
        & examples.time.ge(origin - pd.Timedelta(days=days))
        & (examples.time + HOUR).lt(origin)
    ]


def metrics(pool):
    selected = ranked(pool, pool.score, 3)
    result = summarize(pool, selected)
    per_event = []
    for event, part in pool.groupby('event_slug', sort=True):
        row = summarize(part, selected.loc[selected.event_slug.eq(event)])
        if row['positive']:
            per_event.append(row)
    result['positive_events'] = len(per_event)
    result['event_recall'] = float(np.mean([r['recall'] for r in per_event]))
    result['event_value_capture'] = float(np.mean([r['value_capture'] for r in per_event]))
    return result


def history_sensitivity(examples, base, reference, output):
    timeline = pd.concat([examples[KEEP], base.loc[base.time.ge(EARLIER_END), KEEP]], ignore_index=True)
    if timeline.duplicated(KEYS).any():
        raise ValueError('Duplicate historical outcome records')
    frames, rows, audits = [], [], []
    for alpha in ALPHAS:
        scored, audit = historical_scores(timeline, base, alpha=alpha)
        if not (pd.to_datetime(audit.maximum_label_end, utc=True) < audit.time).all():
            raise ValueError('Historical availability crossed the scan time')
        frame = scored[KEEP + ['fold']].assign(
            score=scored.history_event_mean_value, smoothing=alpha,
            model='history_event_mean_value')
        if alpha == 24.0:
            saved = reference.loc[reference.model.eq('history_event_mean_value')]
            comparison = frame.merge(saved[KEYS + ['score']], on=KEYS, validate='one_to_one',
                                     suffixes=('_new', '_saved'))
            difference = float((comparison.score_new - comparison.score_saved).abs().max())
            if len(comparison) != len(base) or difference > 1e-12:
                raise ValueError('The original alpha=24 historical scores did not reproduce')
            write_json(output / 'history_reconciliation.json',
                       dict(rows=len(comparison), maximum_score_difference=difference))
        for scope, pool in [('full_period', frame), ('earlier_feature_period', frame.loc[frame.time.lt(EARLIER_END)])]:
            rows.append(dict(smoothing=alpha, scope=scope, **metrics(pool)))
        frames.append(frame)
        audits.append(audit.assign(smoothing=alpha))
        print(f'Historical smoothing {alpha:g} complete.', flush=True)
    pd.concat(frames, ignore_index=True).to_parquet(output / 'historical_predictions.parquet', index=False)
    pd.concat(audits, ignore_index=True).to_csv(output / 'tables/history_availability_audit.csv', index=False)
    pd.DataFrame(rows).to_csv(output / 'tables/history_sensitivity.csv', index=False)


def rolling_sensitivity(examples, base, ablations, output):
    features = [c for c in examples if c.startswith('x_')]
    if len(features) != 33 or examples.time.max() >= EARLIER_END:
        raise ValueError('Unexpected retained feature schema or endpoint')
    check_targets(examples.loc[examples.time.ge(FIRST), KEYS + ['label_known', 'target', 'value_target']],
                  base.loc[base.time.lt(EARLIER_END), KEYS + ['label_known', 'target', 'value_target']])
    frames, audits = [], []
    for week, origin in enumerate(pd.date_range(FIRST, EARLIER_END, freq='7D', inclusive='left')):
        stop = min(origin + pd.Timedelta(days=7), EARLIER_END)
        test = examples.loc[examples.time.ge(origin) & examples.time.lt(stop)]
        for days in WINDOW_DAYS:
            train = training_rows(examples, origin, days)
            if train.empty or test.empty:
                raise ValueError('Empty declared rolling-window fold')
            for seed in SEEDS:
                key = f'week_{week:02d}_{days}d_{seed}'
                score_path = output / 'checkpoints' / (key + '.parquet')
                audit_path = score_path.with_suffix('.json')
                if score_path.exists() and audit_path.exists():
                    audit = json.loads(audit_path.read_text())
                    if digest(score_path) != audit['predictions_sha256']:
                        raise ValueError('Changed rolling-window checkpoint')
                    frame = pd.read_parquet(score_path)
                else:
                    started = perf_counter()
                    params = candidates('random_forest', 'classifier')[0]
                    model = make_model('random_forest', 'classifier', params,
                                       train.target.astype(int), features, [])
                    if 'random_state' in model[-1].get_params():
                        model[-1].set_params(random_state=seed)
                    model.fit(train[features], train.target.astype(int))
                    scores = predict(model, test[features])
                    if not np.isfinite(scores).all():
                        raise ValueError('Nonfinite rolling-window predictions')
                    frame = test[KEEP].assign(score=scores, window=f'{days}d', seed=seed, fold=f'week_{week:02d}')
                    frame.to_parquet(score_path, index=False)
                    audit = dict(fold=f'week_{week:02d}', origin=origin, end=stop,
                                 window_days=days, seed=seed,
                                 minimum_allowed_scan=origin - pd.Timedelta(days=days),
                                 minimum_train_scan=train.time.min(),
                                 maximum_train_label_end=train.time.max() + HOUR,
                                 train_rows=len(train), train_positive=int(train.target.eq(1).sum()),
                                 test_rows=len(test), features=features, parameters=params,
                                 estimator=type(model[-1]).__name__, seconds=perf_counter() - started,
                                 predictions_sha256=digest(score_path))
                    write_json(audit_path, audit)
                frames.append(frame)
                audits.append(audit)
        print(f'Rolling-window week {week:02d} complete.', flush=True)
    # Reuse the already verified three-seed expanded-history control without refitting it.
    for seed in SEEDS:
        control = ablations.loc[ablations.view.eq('full_book') & ablations.seed.eq(seed)].copy()
        check_targets(control[KEYS + ['label_known', 'target', 'value_target']],
                      examples.loc[examples.time.ge(FIRST), KEYS + ['label_known', 'target', 'value_target']])
        frames.append(control[KEEP + ['score', 'seed', 'fold']].assign(window='expanding'))
    predictions = pd.concat(frames, ignore_index=True)
    predictions.to_parquet(output / 'rolling_predictions.parquet', index=False)
    pd.DataFrame(audits).to_csv(output / 'tables/rolling_fit_audit.csv', index=False)
    rows = [dict(window=window, seed=seed, **metrics(pool))
            for (window, seed), pool in predictions.groupby(['window', 'seed'], sort=True)]
    pd.DataFrame(rows).to_csv(output / 'tables/rolling_sensitivity.csv', index=False)


def timing_benchmark(examples, base, bundle_path, output):
    # Preserve the original timing sample if an interrupted run is resumed.
    if (output / 'timing_complete.json').exists():
        for name, expected in json.loads((output / 'timing_complete.json').read_text()).items():
            if digest(output / name) != expected:
                raise ValueError('Changed timing evidence')
        return
    bundle = json.loads(bundle_path.read_text())
    origin = pd.Timestamp(bundle['trained_before'])
    pool = examples.loc[examples.time.ge(origin) & examples.time.lt(EARLIER_END)].copy()
    times = sorted(pool.time.unique())
    indexes = np.unique(np.linspace(0, len(times) - 1, TIMING_SCANS, dtype=int))
    selected_times = [times[i] for i in indexes]
    policies = []
    for name in [BEST, 'fixed_book_logistic']:
        info = bundle['policies'][name]
        model = joblib.load(bundle_path.parent / info['artifact'])
        features = info['features']
        if len(features) != 33:
            raise ValueError('Unexpected timing-model feature list')
        modes = [('saved_4_jobs', 4), ('single_job', 1)] if name == BEST else [('saved', None)]
        for mode, jobs in modes:
            policies.append((name, mode, model, features, jobs))
    policies.append(('quoted_gap_rule', 'saved', None, ['x_best_quoted_net_usd'], None))
    records, reconciliations = [], []
    with threadpool_limits(limits=4):
        for time in selected_times:
            scan = pool.loc[pool.time.eq(time)].sort_values('event_slug').copy()
            for name, mode, model, features, jobs in policies:
                if jobs is not None:
                    model[-1].set_params(n_jobs=jobs)
                for repeat in range(-TIMING_WARMUPS, TIMING_REPEATS):
                    started = perf_counter_ns()
                    scores = (scan[features[0]].to_numpy() if model is None
                              else predict(model, scan[features]))
                    scored = perf_counter_ns()
                    chosen = ranked(scan, scores, 3)
                    finished = perf_counter_ns()
                    if repeat >= 0:
                        records.append(dict(time=time, model=name, mode=mode, repeat=repeat,
                                            candidates=len(scan), all_tokens=float(scan.x_active_tokens.sum()),
                                            selected_tokens=float(chosen.x_active_tokens.sum()),
                                            score_us=(scored-started)/1000,
                                            selection_us=(finished-scored)/1000,
                                            total_us=(finished-started)/1000))
                reference = base.loc[base.time.eq(time)].set_index('event_slug')
                if name == BEST:
                    delta = float(np.max(np.abs(scores - reference.loc[scan.event_slug, 'score'].to_numpy())))
                    if delta > 1e-12:
                        raise ValueError('Timing forest scores differ from the saved weekly policy')
                    reconciliations.append(dict(time=time, mode=mode, maximum_score_difference=delta))
    timings = pd.DataFrame(records)
    timings.to_csv(output / 'tables/timing_samples.csv', index=False)
    summary = []
    for (name, mode), part in timings.groupby(['model', 'mode'], sort=True):
        saving = part.all_tokens - part.selected_tokens
        summary.append(dict(model=name, mode=mode, measured_calls=len(part), scans=part.time.nunique(),
                            min_candidates=int(part.candidates.min()), max_candidates=int(part.candidates.max()),
                            median_score_us=float(part.score_us.median()),
                            median_selection_us=float(part.selection_us.median()),
                            median_total_us=float(part.total_us.median()),
                            p95_total_us=float(part.total_us.quantile(.95)),
                            mean_total_us=float(part.total_us.mean()),
                            mean_avoided_token_observations=float(saving.mean()),
                            overhead_us_per_avoided_token=float(part.total_us.sum()/saving.sum()) if saving.sum() else None))
    pd.DataFrame(summary).to_csv(output / 'tables/timing_summary.csv', index=False)
    pd.DataFrame(reconciliations).to_csv(output / 'tables/timing_reconciliation.csv', index=False)
    write_json(output / 'timing_environment.json', dict(
        platform=platform.platform(), processor=platform.processor(), logical_cpus=os.cpu_count(),
        python=platform.python_version(), numpy=np.__version__, pandas=pd.__version__, sklearn=sklearn.__version__,
        timer='perf_counter_ns', timer_unit='microseconds in saved samples',
        bundle_origin=origin, sample_scans=[str(t) for t in selected_times],
        sampling='12 equally spaced scan indices in the final retained weekly block',
        warmups_per_policy_per_scan=TIMING_WARMUPS, measured_repeats=TIMING_REPEATS,
        order='scan ascending; forest 4 jobs, forest 1 job, logistic, quoted gap; fixed descriptive order',
        included='cached DataFrame feature selection, fitted preprocessing, scoring and N=3 allocation',
        excluded='raw-book reads, feature construction, network, historical-label feedback, fitting, model loading and execution',
        interpretation='Warm local timings, not production tail latency or an end-to-end savings estimate.'))
    names = ['tables/timing_samples.csv', 'tables/timing_summary.csv',
             'tables/timing_reconciliation.csv', 'timing_environment.json']
    write_json(output / 'timing_complete.json', {name:digest(output/name) for name in names})
    print('Cached-feature timing complete.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=SUBMISSION / 'artifacts/extensions/sensitivity_review_v1')
    args = parser.parse_args()
    output = args.output.resolve()
    for name in ('tables', 'checkpoints'):
        (output / name).mkdir(parents=True, exist_ok=True)
    inputs = dict(
        examples=SUBMISSION/'artifacts/extensions/processed_review_v1/inputs/earlier_examples.parquet',
        predictions=SUBMISSION/'artifacts/models/predictions.parquet',
        ablations=SUBMISSION/'artifacts/extensions/processed_review_v1/ablation_predictions.parquet',
        historical=SUBMISSION/'artifacts/extensions/robustness_review_v1/historical_predictions.parquet',
        timing_bundle=SUBMISSION/'artifacts/models/folds/week_08/bundle.json')
    bundle = json.loads(inputs['timing_bundle'].read_text())
    for name in [BEST, 'fixed_book_logistic']:
        inputs['timing_'+name] = inputs['timing_bundle'].parent/bundle['policies'][name]['artifact']
    sources = [Path(__file__), SUBMISSION/'FYP/experiments/robustness_extension.py']
    sources += [SUBMISSION/('FYP/models/'+name+'.py') for name in
                ('estimators','features','metrics','classification','value','tuning','settings')]
    design = dict(
        inputs={k:dict(path=p.relative_to(SUBMISSION).as_posix(), sha256=digest(p)) for k,p in inputs.items()},
        source_sha256={p.relative_to(SUBMISSION).as_posix():digest(p) for p in sources},
        historical_smoothing=list(ALPHAS), historical_scope='full period and matched earlier subset',
        rolling_days=list(WINDOW_DAYS), seeds=list(SEEDS), new_fits=54,
        training_window='scan time >= origin-days; known label end strictly < weekly origin',
        evaluation_start=str(FIRST), fitting_end_exclusive=str(EARLIER_END),
        control='reuse existing full-book expanded-history three-seed scores',
        metrics='N=3 pooled recall/value and equal-positive-event recall/value; rank before known-label filtering',
        timing=dict(scans=TIMING_SCANS, warmups=TIMING_WARMUPS, repeats=TIMING_REPEATS,
                    block='week_08 retained scans', forest_jobs=[4,1],
                    target='warm cached-feature scoring and selection only'),
        tuning=False, raw_orderbooks_used=False, new_labels=False, new_execution_replay=False,
        interpretation='Exploratory sensitivities fixed after prior results were inspected; all settings reported, no new winning-policy selection.',
        versions=dict(python=platform.python_version(),numpy=np.__version__,sklearn=sklearn.__version__))
    if (output/'design.json').exists() and json.loads((output/'design.json').read_text()) != design:
        raise ValueError('Design, inputs or executed source changed; choose a new output directory')
    write_json(output/'design.json', design)
    if not (output/'executed_source.py').exists():
        shutil.copyfile(__file__, output/'executed_source.py')
    predictions, examples, ablations, historical = [pd.read_parquet(inputs[k]) for k in
                                                    ('predictions','examples','ablations','historical')]
    base = predictions.loc[predictions.model.eq(BEST)].copy()
    history_sensitivity(examples, base, historical, output)
    with threadpool_limits(limits=4):
        rolling_sensitivity(examples, base, ablations, output)
    timing_benchmark(examples, base, inputs['timing_bundle'], output)
    write_json(output/'complete.json',dict(files_sha256={
        p.relative_to(output).as_posix():digest(p) for p in output.rglob('*')
        if p.is_file() and p.name!='complete.json'}))
    print('Focused sensitivity study complete:',output,flush=True)


if __name__ == '__main__':
    main()
