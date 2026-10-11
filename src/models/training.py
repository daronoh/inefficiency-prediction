"""Compare models, rules, ensembles and feature ablations on forward folds."""
from pathlib import Path
from dataclasses import asdict
import json
import platform
import numpy as np
import pandas as pd
import sklearn
import joblib
from sklearn.metrics import roc_auc_score, brier_score_loss, accuracy_score, balanced_accuracy_score
from src.common import sha256
from src.config import SPEC
from src.data.dataset import load_examples
from src.data.metadata import add_metadata, METADATA_COLUMNS, METADATA_NUMERIC, METADATA_CATEGORICAL
from src.paths import MARKETS_PARQUET, EVENT_UNIVERSE
from src.models.settings import (SEED, FOLDS, MODEL_NAMES, BUDGETS, FEATURE_SETS,
                                 CLASSIFIER_METHODS, method_name, ensemble_name)
from src.models.estimators import estimator, predict
from src.models.evaluation import masks, ranking_metrics, safe_ap
from src.models.features import family, feature_groups

def fit_comparison(output, labels_dir, features_path, spec=SPEC):
    output, labels_dir = Path(output), Path(labels_dir)
    output.mkdir(parents=True, exist_ok=False)
    design = json.loads((labels_dir / 'design.json').read_text())
    frame = add_metadata(load_examples(features_path, labels_dir, spec))
    frame['family'] = frame.event_slug.map(family)
    columns = [c for c in frame if c.startswith('x_')]
    groups = feature_groups(columns)
    feature_sets = {'book': columns, 'metadata': METADATA_COLUMNS, 'combined': columns + METADATA_COLUMNS}

    def make_model(name, target, view):
        if view == 'book':
            return estimator(name, target)
        numeric = METADATA_NUMERIC if view == 'metadata' else columns + METADATA_NUMERIC
        return estimator(name, target, numeric=numeric, categorical=METADATA_CATEGORICAL)

    run_design = dict(contract=asdict(spec), seed=SEED, folds=FOLDS, budgets=BUDGETS, primary_budget=5,
        models=MODEL_NAMES, ensemble='Equal mean of within-window percentile ranks of the three models',
        hyperparameters='Fixed before comparison; no search; see source hash',
        target='At least one positive displayed-depth attempt; carry-forward conditional on complete change capture',
        scoring_mask='Symmetric full-window coverage for positives and negatives; partial positives retained only in separate execution evidence',
        label_sha256=sha256(labels_dir / 'labels.parquet'), feature_sha256=sha256(features_path),
        label_design_sha256=sha256(labels_dir / 'design.json'),
        quote_policies=design['quote_policies'],
        source_sha256=sha256(__file__), sklearn=sklearn.__version__, python=platform.python_version(),
        final_lockbox_opened=False, family_mapping=dict(zip(frame.event_slug, frame.family)),
        features=columns, feature_groups=groups, feature_sets=feature_sets, random_repeats=20,
        metadata=dict(fields=METADATA_COLUMNS, topic_encoding='One-hot fitted on training rows; unseen categories ignored',
            age='Days since earliest frozen member created_at; stable creation-time assumption; future or absent dates missing',
            deadlines='Excluded: frozen end_date and closed_time do not establish the deadline known at the scan',
            exclusions=['event identifiers', 'text embeddings', 'volume', 'outcomes', 'status', 'closing dates'],
            markets_sha256=sha256(MARKETS_PARQUET), universe_sha256=sha256(EVENT_UNIVERSE)),
        embargo_seconds=spec.horizon.total_seconds() + spec.arrival_delay_s + spec.unwind_delay_s)
    (output / 'design.json').write_text(json.dumps(run_design, indent=2) + '\n')
    predictions, importance, fit_records = [], [], []
    (output / 'diagnostics').mkdir()
    (output / 'fitted_models').mkdir()
    for fold, start, end in FOLDS:
        train_mask, test_mask = masks(frame, start, end, spec)
        train, test = frame.loc[train_mask].copy(), frame.loc[test_mask].copy()
        if train.empty or test.empty:
            raise ValueError(f'Empty fold {fold}')
        print(f'{fold}: train={len(train)} positives={train.target.sum():.0f}; evaluate={len(test)}', flush=True)
        ytrain = train.target.astype(int)
        scores, fitted = {}, {}
        for view, selected_columns in feature_sets.items():
            for name in MODEL_NAMES:
                key = method_name(name, view)
                model = make_model(name, ytrain, view).fit(train[selected_columns], ytrain)
                fitted[key] = model
                joblib.dump(model, output / 'fitted_models' / f'{fold}_{key}.joblib', compress=3)
                scores[key] = predict(model, test[selected_columns])
                fit_records.append(dict(fold=fold, model=key, training_rows=len(train), training_positives=int(ytrain.sum()),
                    train_last_time=str(train.time.max()), test_first_time=str(test.time.min()),
                    single_class_fallback=ytrain.nunique() < 2))
            percentile = [pd.Series(scores[method_name(name, view)], index=test.index).groupby(test.time).rank(pct=True)
                          for name in MODEL_NAMES]
            scores[ensemble_name(view)] = np.mean(percentile, axis=0)
        scores['quoted_gap_rule'] = test.x_best_quoted_net_usd.fillna(-1e12).to_numpy()
        scores['depth_rule'] = test.x_ask_depth_sum.fillna(-1e12).to_numpy()
        scores['freshness_rule'] = -test.x_book_age_max_s.fillna(1e12).to_numpy()
        for seed in range(20):
            scores[f'random_{seed:02d}'] = np.random.default_rng(SEED + seed + FOLDS.index((fold, start, end)) * 100).random(len(test))
        # Feature ablations refit; permutation importance alone cannot test usefulness.
        for group in ['history', 'quoted_gap', 'freshness_quality']:
            kept = [c for c in columns if c not in groups[group]]
            model = estimator('boosted_trees', ytrain).fit(train[kept], ytrain)
            scores[f'boosted_without_{group}'] = predict(model, test[kept])
        # Every evaluated topic is predicted by a model trained without that topic.
        family_scores = {method_name(name, view): np.full(len(test), np.nan)
                         for view in FEATURE_SETS for name in MODEL_NAMES}
        for topic in sorted(test.family.unique()):
            topic_train = train.loc[train.family.ne(topic)]
            where = test.family.eq(topic).to_numpy()
            if topic_train.empty:
                continue
            for view, selected_columns in feature_sets.items():
                for name in MODEL_NAMES:
                    target = topic_train.target.astype(int)
                    model = make_model(name, target, view).fit(topic_train[selected_columns], target)
                    family_scores[method_name(name, view)][where] = predict(model, test.loc[where, selected_columns])
        for name, values in family_scores.items():
            scores[f'family_holdout_{name}'] = values
        for view in FEATURE_SETS:
            scores[ensemble_name(view, heldout=True)] = np.mean([
                pd.Series(family_scores[method_name(name, view)], index=test.index).groupby(test.time).rank(pct=True)
                for name in MODEL_NAMES], axis=0)
        # Held-out permutation is within each decision window, preserving date/regime.
        # Report the transparent logistic model and the nonlinear boosted model.
        importance_jobs = [(name, 'book') for name in ['logistic', 'boosted_trees']]
        importance_jobs += [(method_name(name, view), view) for view in ['metadata', 'combined']
                            for name in ['logistic', 'boosted_trees']]
        for model_name, view in importance_jobs:
            selected_columns = feature_sets[view]
            base_score = scores[model_name]
            known = test.label_known
            base_ap = safe_ap(test.loc[known, 'target'], base_score[known])
            base_recall = ranking_metrics(test, base_score, 5)['recall']
            if view == 'book':
                units = {f'feature:{c}': [c] for c in columns}
                units.update({f'group:{g}': cols for g, cols in groups.items()})
            else:
                units = {f'feature:{c}': [c] for c in METADATA_COLUMNS}
                units['group:metadata'] = METADATA_COLUMNS
                if view == 'combined':
                    units.update({f'group:{g}': cols for g, cols in groups.items()})
                    units['group:book'] = columns
            positions = list(test.groupby('time', sort=False).indices.values())
            for unit, cols in units.items():
                for repeat in range(3):
                    rng = np.random.default_rng(SEED + repeat)
                    permuted = test[selected_columns].copy()
                    order = np.arange(len(test))
                    for pos in positions:
                        order[pos] = rng.permutation(pos)
                    permuted.loc[:, cols] = test[cols].iloc[order].to_numpy()
                    pscore = predict(fitted[model_name], permuted)
                    importance.append(dict(fold=fold, model=model_name, unit=unit, repeat=repeat,
                        ap_drop=base_ap - safe_ap(test.loc[known, 'target'], pscore[known]),
                        recall5_drop=base_recall - ranking_metrics(test, pscore, 5)['recall']))
        for name, score in scores.items():
            prediction = test[['event_slug', 'family', 'stratum', 'time', 'label_known', 'target',
                'future_positive', 'coverage_fraction', 'x_active_tokens', 'known_pnl_usd']].copy()
            prediction['model'], prediction['fold'], prediction['score'] = name, fold, score
            predictions.append(prediction)
        # Persist completed folds so a long run is inspectable and recoverable.
        pd.concat(predictions, ignore_index=True).to_parquet(output / 'predictions.parquet', index=False)
        pd.DataFrame(importance).to_csv(output / 'diagnostics' / 'permutation_importance.csv', index=False)
        pd.DataFrame(fit_records).to_csv(output / 'diagnostics' / 'fit_audit.csv', index=False)
    combined = pd.concat(predictions, ignore_index=True)
    metrics, classification = [], []
    for name, part in combined.groupby('model'):
        for n in BUDGETS:
            metrics.append(dict(model=name, **ranking_metrics(part, part.score, n)))
        known = part.loc[part.label_known]
        y = known.target
        classification.append(dict(model=name, rows=len(known), prevalence=y.mean(),
            average_precision=safe_ap(y, known.score),
            roc_auc=float(roc_auc_score(y, known.score)) if y.nunique() == 2 else np.nan,
            brier=float(brier_score_loss(y, known.score)) if name in CLASSIFIER_METHODS else np.nan,
            accuracy_at_05=float(accuracy_score(y, known.score.ge(.5))) if name in CLASSIFIER_METHODS else np.nan,
            balanced_accuracy_at_05=float(balanced_accuracy_score(y, known.score.ge(.5))) if name in CLASSIFIER_METHODS else np.nan))
    pd.DataFrame(metrics).to_csv(output / 'ranking_metrics.csv', index=False)
    pd.DataFrame(classification).to_csv(output / 'classification_metrics.csv', index=False)
    return pd.DataFrame(metrics)
