from __future__ import annotations
import json
from pathlib import Path
from time import perf_counter
import joblib
import numpy as np
import pandas as pd
from FYP.common import sha256
from FYP.data.pipeline import ROOT as DATA, START, FIRST, END, HORIZON, load, write_json
from FYP.models.estimators import predict
from FYP.models.metrics import ranked
from FYP.models.features import family
from FYP.models.classification import fit_classifier, split_masks, validation_windows
from FYP.models.value import fit_value, value_metrics
from FYP.models.settings import SEED
from FYP.models.tabm import TabMEstimator, candidates as tabm_candidates
from FYP.models.tuning import candidates
from FYP.paths import PROJECT_ROOT
OUTPUT = PROJECT_ROOT / 'artifacts/models'
METHODS = ('logistic', 'random_forest', 'xgboost', 'tabm_two_stage')
SEEDS = (SEED, SEED + 1, SEED + 2)
CORE = ['event_slug', 'stratum', 'time', 'family', 'label_known', 'target', 'value_target', 'coverage_fraction', 'future_positive', 'opportunity_at_scan', 'x_active_tokens']

def folds(first=FIRST, end=END):
    return [(f'week_{i:02d}', t, min(t + pd.Timedelta(days=7), end)) for i, t in enumerate(pd.date_range(first, end, freq='7D', inclusive='left'))]

def train_before(frame, cutoff):
    return frame.loc[frame.label_known & (frame.time + HORIZON).lt(cutoff)]

def choose(rows):
    """Same primary N=3 value objective for every tuned family."""
    table = pd.DataFrame(rows).groupby('candidate', sort=True).agg(total=('total_peak_value', 'sum'), captured=('captured_peak_value', 'sum'), positives=('positive_windows', 'sum'), caught=('captured_positive_windows', 'sum'), ap=('average_precision', 'mean'))
    if table.positives.nunique() != 1 or not np.allclose(table.total, table.total.iloc[0]):
        raise ValueError('Tuning candidates do not share evaluation populations')
    return int(table.fillna(-np.inf).sort_values(['captured', 'caught', 'ap'], ascending=False, kind='stable').index[0])

def fit_network(train, valid, task, params, columns, device, seed=SEED, epochs=None):
    if not train.label_known.all():
        raise ValueError('Unknown training labels')
    if task == 'amount':
        train = train.loc[train.value_target.gt(0)]
        valid = valid.loc[valid.label_known & valid.value_target.gt(0)] if valid is not None else None
    elif valid is not None:
        valid = valid.loc[valid.label_known]
    if train.empty:
        raise ValueError('No stage training examples')
    model = TabMEstimator('classifier' if task == 'classifier' else 'regression', params, seed=seed, device=device, batch_size=1024, max_epochs=60, patience=10)
    target = 'target' if task == 'classifier' else 'value_target'
    if valid is not None and valid.empty:
        return model.fit(train[columns], train[target], epochs=16)
    return model.fit(train[columns], train[target], validation=(valid[columns], valid[target]) if valid is not None else None, epochs=epochs)

def tune(frame, cutoff, folder, design):
    folder.mkdir(parents=True, exist_ok=True)
    rows, chosen = ([], {})
    columns = design['feature_views']['combined']
    for method in METHODS:
        local = []
        for index, params in enumerate(design['candidates'][method]):
            for inner, (lo, hi) in enumerate(validation_windows(frame, cutoff, HORIZON)):
                name = f'{method}_c{index}_i{inner}'
                cached = folder / f'{name}.json'
                scores_path = folder / f'{name}.parquet'
                if cached.exists():
                    row = json.loads(cached.read_text())
                    if row['predictions_sha256'] != sha256(scores_path):
                        raise ValueError('Changed cached tuning predictions')
                else:
                    a, b = split_masks(frame, lo, hi, HORIZON)
                    fitting, valid = (frame.loc[a], frame.loc[b])
                    started = perf_counter()
                    extra = {}
                    print(f'  tune {cutoff.date()} {method} candidate {index} inner {inner}', flush=True)
                    if method == 'tabm_two_stage':
                        gate = fit_network(fitting, valid, 'classifier', params, columns, design['device'])
                        amount = fit_network(fitting, valid, 'amount', params, columns, design['device'])
                        score = gate.predict(valid[columns]) * amount.predict(valid[columns])
                        extra = dict(classifier_epochs=max(1, gate.best_epoch_), amount_epochs=max(1, amount.best_epoch_))
                    else:
                        model = fit_classifier(fitting, method, params, columns)
                        score = predict(model, valid[columns])
                    valid[CORE].assign(score=score).to_parquet(scores_path, index=False)
                    metric = value_metrics(valid, score, 3)
                    row = dict(method=method, candidate=index, inner=inner, params=params, training_rows=len(fitting), training_positive=int(fitting.target.sum()), train_last=str(fitting.time.max()), validation_start=str(lo), validation_end=str(hi), fitting_cutoff=str(cutoff), seconds=perf_counter() - started, **{k: metric[k] for k in ['total_peak_value', 'captured_peak_value', 'positive_windows', 'captured_positive_windows', 'average_precision', 'selected_unknown']}, **extra, predictions_sha256=sha256(scores_path))
                    write_json(cached, row)
                local.append(row)
        best = choose(local)
        selection = dict(candidate=best, params=design['candidates'][method][best], tuned_before=str(cutoff))
        if method == 'tabm_two_stage':
            selected = [r for r in local if r['candidate'] == best]
            selection['epochs'] = {task: max(1, int(np.ceil(np.median([r[f'{task}_epochs'] for r in selected])))) for task in ('classifier', 'amount')}
        chosen[method] = selection
        rows.extend(local)
    pd.DataFrame(rows).to_csv(folder / 'search.csv', index=False)
    write_json(folder / 'selected.json', chosen)
    return chosen

def add_ensembles(test, scores):
    groups = {'classifier_rank_ensemble': [f'tuned_combined_{n}' for n in METHODS[:3]], 'rank_ensemble': ['tuned_combined_logistic', 'tuned_combined_random_forest', 'tabm_two_stage'], 'value_rank_ensemble': ['forest_two_stage', 'tabm_two_stage']}
    for name, members in groups.items():
        scores[name] = np.mean([test[['time']].assign(score=scores[m]).groupby('time').score.rank(pct=True).to_numpy() for m in members], axis=0)
    return scores

def score_saved(folder, bundle, features):
    scores = {'quoted_gap_rule': features.x_best_quoted_net_usd.fillna(-1000000000000.0).to_numpy(), 'depth_rule': features.x_ask_depth_sum.fillna(-1000000000000.0).to_numpy(), 'freshness_rule': -features.x_book_age_max_s.fillna(1000000000000.0).to_numpy()}
    for name, entry in bundle['policies'].items():
        x = features[entry['features']]
        if entry['kind'] == 'classifier':
            scores[name] = predict(joblib.load(folder / entry['artifact']), x)
        elif entry['kind'] == 'forest_hurdle':
            scores[name] = joblib.load(folder / entry['artifact']).predict(x)
        else:
            scores[name] = np.mean([joblib.load(folder / p['gate']).predict(x) * joblib.load(folder / p['amount']).predict(x) for p in entry['members']], axis=0)
    return add_ensembles(features, scores)

def fit_week(train, test, selected, folder, design, origin, start, end):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / 'models').mkdir(exist_ok=True)
    bundle = dict(origin=origin, trained_before=str(start), end_exclusive=str(end), selected=selected, policies={})
    audits = []
    book, combined = (design['feature_views']['book'], design['feature_views']['combined'])
    cache = {}
    for method in METHODS[:3]:
        for prefix, columns, params in [('fixed_book', book, design['candidates'][method][0]), ('fixed_combined', combined, design['candidates'][method][0]), ('tuned_combined', combined, selected[method]['params'])]:
            name = f'{prefix}_{method}'
            key = (method, tuple(columns), json.dumps(params, sort_keys=True))
            if key not in cache:
                started = perf_counter()
                model = fit_classifier(train, method, params, columns)
                artifact = f'models/{name}.joblib'
                joblib.dump(model, folder / artifact, compress=3)
                cache[key] = artifact
                audits.append(dict(model=name, task='classifier', seed=SEED, artifact=artifact, training_rows=len(train), training_positive=int(train.target.sum()), train_last=str(train.time.max()), label_end_last=str(train.time.max() + HORIZON), features=json.dumps(columns), seconds=perf_counter() - started))
            bundle['policies'][name] = dict(kind='classifier', features=columns, artifact=cache[key])
    model = fit_value(train, 'random_forest', 'two_stage', design['forest_value_params'], combined)
    artifact = 'models/forest_two_stage.joblib'
    joblib.dump(model, folder / artifact, compress=3)
    bundle['policies']['forest_two_stage'] = dict(kind='forest_hurdle', features=combined, artifact=artifact)
    audits.append(dict(model='forest_two_stage', task='hurdle', seed=SEED, artifact=artifact, training_rows=len(train), training_positive=int(train.target.sum()), train_last=str(train.time.max()), label_end_last=str(train.time.max() + HORIZON), features=json.dumps(combined)))
    members = []
    for seed in SEEDS:
        member = {'seed': seed}
        for task in ('classifier', 'amount'):
            epochs = selected['tabm_two_stage']['epochs'][task]
            print(f'  {origin} TabM {task}, seed {seed}, epochs {epochs}', flush=True)
            model = fit_network(train, None, task, selected['tabm_two_stage']['params'], combined, design['device'], seed, epochs)
            artifact = f'models/tabm_{task}_{seed}.joblib'
            joblib.dump(model, folder / artifact, compress=3)
            member['gate' if task == 'classifier' else 'amount'] = artifact
            stage = train if task == 'classifier' else train.loc[train.value_target.gt(0)]
            audits.append(dict(model='tabm_two_stage', task=task, seed=seed, artifact=artifact, training_rows=len(stage), training_positive=int(stage.target.sum()), train_last=str(stage.time.max()), label_end_last=str(stage.time.max() + HORIZON), features=json.dumps(combined), epochs=epochs))
        members.append(member)
    bundle['policies']['tabm_two_stage'] = dict(kind='hurdle_ensemble', features=combined, members=members)
    write_json(folder / 'bundle.json', bundle)
    scores = score_saved(folder, bundle, test)
    predictions = []
    for name, score in scores.items():
        if not np.isfinite(score).all():
            raise ValueError('Nonfinite forward scores')
        predictions.append(test[CORE].assign(model=name, score=score, fold=origin))
    prediction = pd.concat(predictions, ignore_index=True)
    prediction.to_parquet(folder / 'predictions.parquet', index=False)
    pd.DataFrame(audits).to_csv(folder / 'fit_audit.csv', index=False)
    population = dict(origin=origin, start=start, end=end, training_rows=len(train), training_positive=int(train.target.sum()), training_events=train.event_slug.nunique(), training_last=train.time.max(), evaluation_rows=len(test), evaluation_known=int(test.label_known.sum()), evaluation_positive=int(test.target.eq(1).sum()), evaluation_events=test.event_slug.nunique(), scheduled_hours=int((end - start) / HORIZON))
    write_json(folder / 'population.json', population)
    write_json(folder / 'complete.json', dict(files_sha256={p.relative_to(folder).as_posix(): sha256(p) for p in folder.rglob('*') if p.is_file()}))
    return (prediction, population)

def evaluate(prediction, output):
    metrics, selections = ([], [])
    for scope, excluded in [('all_events', None), ('exclude_december_fed', 'fed-decision-in-december')]:
        pool = prediction if excluded is None else prediction.loc[prediction.event_slug.ne(excluded)]
        for name, part in pool.groupby('model'):
            subsets = [('all', part)] + list(part.groupby('fold'))
            for fold, subset in subsets:
                for n in (3, 5):
                    metrics.append(dict(scope=scope, model=name, fold=fold, **value_metrics(subset, subset.score, n)))
            for n in (3, 5):
                selections.append(ranked(part, part.score, n).drop(columns='_score').assign(scope=scope, top_n=n))
    pd.DataFrame(metrics).to_csv(output / 'metrics.csv', index=False)
    pd.concat(selections, ignore_index=True).to_parquet(output / 'selections.parquet', index=False)

def run(output=OUTPUT, data=DATA, device='cuda'):
    output, data = (Path(output), Path(data))
    frame = load(data).sort_values(['time', 'event_slug']).reset_index(drop=True)
    frame['family'] = frame.event_slug.map(family)
    prior = json.loads((PROJECT_ROOT / 'inputs/study.json').read_text())
    options = {m: candidates(m, 'classifier') for m in METHODS[:3]}
    options['tabm_two_stage'] = [tabm_candidates()[i] for i in (0, 1, 2, 4)]
    sources = [p.relative_to(PROJECT_ROOT).as_posix() for p in (PROJECT_ROOT / 'FYP/models').glob('*.py')] + ['FYP/experiments/study.py','FYP/data/pipeline.py','FYP/config.py','FYP/common.py','FYP/paths.py']
    design = dict(version=1, start=START, first_origin=FIRST, end_exclusive=END, folds=folds(), horizon_minutes=60, primary_budget=3, secondary_budget=5, feature_views=prior['feature_views'], candidates=options, tabm_candidate_indices=[0, 1, 2, 4], forest_value_params=candidates('random_forest', 'regression')[0], seeds=SEEDS, device=device, tabm=dict(k=32, max_epochs=60, patience=10, batch_size=1024), tuning='Every fourth weekly origin when at least seven evaluation days remain; two purged chronological inner blocks. Refit weekly. Carry settings/epochs between tuning dates.', objective='Pooled captured peak quoted value at N=3, ties captured positive hours, mean inner AP, candidate order.', features='Fixed book and combined controls; tuned models and both hurdles use combined features. No outcomes or segment markers are predictors.', label_policy='Known training windows end strictly before origin. Unknown evaluation rows consume monitoring slots.', ensembles='Equal within-scan percentile ranks: three tuned classifiers; tuned logistic+forest+TabM; forest hurdle+TabM. No learned outer weights.', interpretation='Retrospective chronological comparison of weekly refitted screening policies.', economic_selection='Prediction scores only; PnL never enters fitting, tuning or ensemble weights.', source_sha256={p: sha256(PROJECT_ROOT / p) for p in sources}, dataset_completion_sha256=sha256(data / 'complete.json'))
    normalized = json.loads(json.dumps(design, default=str))
    output.mkdir(parents=True, exist_ok=True)
    if (output / 'design.json').exists() and json.loads((output / 'design.json').read_text()) != normalized:
        raise ValueError('Existing study has different code/data/settings; use a new output directory')
    write_json(output / 'design.json', normalized)
    predictions, populations = ([], [])
    selected = None
    for i, (origin, start, end) in enumerate(folds()):
        folder = output / 'folds' / origin
        if (folder / 'complete.json').exists():
            for name, digest in json.loads((folder / 'complete.json').read_text())['files_sha256'].items():
                if sha256(folder / name) != digest:
                    raise ValueError('Saved weekly artifact changed')
            selected = json.loads((folder / 'bundle.json').read_text())['selected']
            predictions.append(pd.read_parquet(folder / 'predictions.parquet'))
            populations.append(json.loads((folder / 'population.json').read_text()))
            continue
        train = train_before(frame, start)
        test = frame.loc[frame.time.ge(start) & frame.time.lt(end)]
        print(f'{origin}: {start} to {end}; {len(train)} training rows / {int(train.target.sum())} positives', flush=True)
        if i % 4 == 0 and end - start == pd.Timedelta(days=7):
            selected = tune(frame, start, output / 'tuning' / origin, normalized)
        if selected is None:
            raise ValueError('Initial tuning is required')
        prediction, population = fit_week(train, test, selected, folder, normalized, origin, start, end)
        predictions.append(prediction)
        populations.append(population)
    prediction = pd.concat(predictions, ignore_index=True)
    prediction.to_parquet(output / 'predictions.parquet', index=False)
    pd.DataFrame(populations).to_csv(output / 'populations.csv', index=False)
    evaluate(prediction, output)
    for name, digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT / name) != digest:
            raise ValueError('Source changed during model study')
    write_json(output / 'complete.json', dict(files_sha256={p.relative_to(output).as_posix(): sha256(p) for p in output.rglob('*') if p.is_file() and p.name != 'complete.json'}, weekly_folds=len(folds()), execution_outcomes_used=False))
    print(pd.read_csv(output / 'metrics.csv').query("scope=='all_events' and fold=='all' and top_n==3")[['model', 'positive_windows', 'recall', 'peak_value_recall']].to_string(index=False))
