"""Exploratory fixed-origin Dome validation with shorter training histories.

This does not reopen or replace the already evaluated final protocol. All new
fits and evaluation use development-only, execution-independent labels.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter

import joblib
import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.data.metadata import add_metadata
from src.data.opportunities import load_dataset
from src.models.estimators import predict
from src.models.evaluation import ranked
from src.models.features import family
from src.models.opportunities import fit_classifier
from src.models.opportunity_value import fit_value, value_metrics
from src.models.settings import SEED
from src.models.tabm import candidates as tabm_candidates
from src.models.tabm_study import fit as fit_tabm, SEEDS
from src.models.tuning import candidates
from src.paths import PROJECT_ROOT, PROCESSED_DIR, MARKETS_PARQUET, EVENT_UNIVERSE

ORIGINS = {'train_14d': '2025-10-28T00:00:00Z', 'train_28d': '2025-11-11T00:00:00Z'}
END = pd.Timestamp('2026-01-08T00:00:00Z')
COMMON_START = pd.Timestamp(ORIGINS['train_28d'])
HORIZON = pd.Timedelta(hours=1)
OUTPUT = PROJECT_ROOT/'results/validation_horizon'


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str)+'\n', encoding='utf-8')


def training_rows(frame, cutoff):
    return frame.loc[frame.label_known & (frame.time+HORIZON).lt(cutoff)].copy()


def forward_rows(frame, cutoff):
    if not (frame.time+HORIZON).le(END).all():
        raise ValueError('No original final-period outcomes allowed')
    return frame.loc[frame.time.ge(cutoff) & frame.time.lt(END)].copy()


def paired_block_intervals(pool, selected, baseline, *, repetitions=10000):
    """Resample whole scheduled seven-day blocks jointly for both policies.

    This conditions on fitted models and this observed cohort. It does not
    estimate uncertainty from refitting, missing outcomes, or market selection.
    """
    origin = pool.time.min().floor('D')
    count = int((pool.time.max()-origin)//pd.Timedelta(days=7))+1
    def blocks(part):
        return ((part.time-origin)//pd.Timedelta(days=7)).astype(int)
    denominator = pool.assign(block=blocks(pool)).groupby('block').agg(
        positives=('target', lambda x: x.eq(1).sum()), value=('value_target', 'sum')).reindex(range(count), fill_value=0)
    def totals(part):
        return part.assign(block=blocks(part)).groupby('block').agg(
            positives=('target', lambda x: x.eq(1).sum()), value=('value_target', 'sum')).reindex(range(count), fill_value=0)
    difference = totals(selected)-totals(baseline)
    draws = np.random.default_rng(20261030).integers(count, size=(repetitions, count))
    result = {'seven_day_blocks': count}
    for column, name in [('positives', 'recall'), ('value', 'value_capture')]:
        num = difference[column].to_numpy()[draws].sum(axis=1)
        den = denominator[column].to_numpy()[draws].sum(axis=1)
        valid = den > 0
        low, high = np.quantile(num[valid]/den[valid], [.025, .975]) if valid.any() else (np.nan, np.nan)
        total = denominator[column].sum()
        result.update({f'{name}_difference': float(difference[column].sum()/total) if total else np.nan,
            f'{name}_lower95': low, f'{name}_upper95': high, f'{name}_defined_draws': int(valid.sum())})
    return result


def evaluate_predictions(predictions, output):
    metrics, blocks, paired, selections = [], [], [], []
    for origin, origin_pool in predictions.groupby('origin'):
        for scope, pool in [('full_forward', origin_pool), ('common_forward', origin_pool.loc[origin_pool.time.ge(COMMON_START)])]:
            for name, part in pool.groupby('model'):
                for n in (3, 5):
                    metric = value_metrics(part, part.score, n)
                    metrics.append(dict(origin=origin, scope=scope, model=name, **metric))
                    chosen = ranked(part, part.score, n).drop(columns='_score')
                    selections.append(chosen.assign(scope=scope, top_n=n))
                    # Calendar blocks expose concentration rather than just pooled totals.
                    for month, subset in part.groupby(part.time.dt.strftime('%Y-%m')):
                        blocks.append(dict(origin=origin, scope=scope, month=month, model=name, **value_metrics(subset, subset.score, n)))
                    if scope == 'common_forward':
                        reference = pool.loc[pool.model.eq('quoted_gap_rule')]
                        baseline = ranked(reference, reference.score, n)
                        paired.append(dict(origin=origin, scope=scope, model=name, top_n=n,
                            **paired_block_intervals(part, chosen, baseline)))
    pd.DataFrame(metrics).to_csv(output/'metrics.csv', index=False)
    pd.DataFrame(blocks).to_csv(output/'monthly_metrics.csv', index=False)
    pd.DataFrame(paired).to_csv(output/'paired_intervals.csv', index=False)
    pd.concat(selections, ignore_index=True).to_parquet(output/'selections.parquet', index=False)
    # Exact random expectations use the entire eligible pool, including unknowns.
    references = pd.DataFrame(metrics)
    references = references.loc[references.model.eq('quoted_gap_rule')].copy()
    references['expected_random_recall'] = references.expected_random_captured/references.positive_windows.replace(0, np.nan)
    references['expected_random_value_capture'] = references.expected_random_peak_value/references.total_peak_value.replace(0, np.nan)
    references.to_csv(output/'random_reference.csv', index=False)


def run(output=OUTPUT, device='cpu'):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output/'fitted_models').mkdir()
    data = PROCESSED_DIR/'opportunity_value'
    original = json.loads((PROJECT_ROOT/'results/final_preparation/design.json').read_text())
    sources = ['src/models/validation_horizon.py','src/models/opportunities.py','src/models/opportunity_value.py',
        'src/models/tuning.py','src/models/estimators.py','src/models/tabm.py','src/models/tabm_study.py',
        'src/models/features.py','src/models/evaluation.py','src/data/opportunities.py','src/data/metadata.py']
    design = dict(version=1, origins=ORIGINS, common_start=COMMON_START, end_exclusive=END,
        development_start='2025-10-14T00:00:00Z', primary_budget=3, secondary_budget=5,
        purpose='Earlier fixed-origin fits with 14/28 calendar days of training; longer subsequent development evaluation. Compare origins only on common_forward.',
        split_status='Exploratory after repeatedly inspected development and opened original final test; no new untouched test claim.',
        periods_chosen_by='Fixed 14-day/28-day calendar histories, not a search over model performance.',
        fitting='Known labels only, full one-hour label horizon ends strictly before origin. Each model stays frozen throughout its forward interval.',
        feature_views=original['feature_views'],
        parameters={n:candidates(n,'classifier')[0] for n in ('logistic','random_forest','xgboost')},
        forest_value_params=candidates('random_forest','regression')[0],
        tabm_params=tabm_candidates()[0], seeds=SEEDS, tabm_members=32,
        tabm_epochs='Use last calendar quarter of pre-origin history for epoch selection only; purge one-hour windows at inner and outer boundaries. Refit whole earlier history at selected epochs for three seeds.',
        tuning='Original first-declared fixed configurations; no new outer-score hyperparameter, seed or ensemble weight selection.',
        ensemble='Equal within-scan percentile ranks of fixed book logistic, fixed book forest, combined TabM hurdle.',
        targets='Occurrence and peak one-basket quoted value, independent of execution. Unknowns rank but do not train or become zero.',
        economic_assumptions='No new economic replay. Retain separate primary 100ms, uncapped-capital/share, depth-limited scenario.',
        uncertainty='Paired seven-day block bootstrap, 10000 draws, seed 20261030. Conditional on fixed models/cohort; not refitting or missing-label uncertainty.',
        source_sha256={p:sha256(PROJECT_ROOT/p) for p in sources},
        dataset_sha256=sha256(data/'examples.parquet'), dataset_completion_sha256=sha256(data/'complete.json'),
        metadata_sha256={str(p.relative_to(PROJECT_ROOT)):sha256(p) for p in (MARKETS_PARQUET, EVENT_UNIVERSE)},
        original_final_protocol_sha256=sha256(PROJECT_ROOT/'results/final_protocol/protocol.json'),
        original_final_models_completion_sha256=sha256(PROJECT_ROOT/'results/final_preparation/complete.json'),
        original_final_outcomes_opened=True, original_final_outcomes_used=False, execution_targets_used=False)
    write_json(output/'design.json', design)
    frame = add_metadata(load_dataset(data)).sort_values(['time','event_slug']).reset_index(drop=True)
    frame['family'] = frame.event_slug.map(family)
    if frame.duplicated(['event_slug','time']).any() or any(c in frame for c in ('pnl_target','known_pnl_usd','activity_target')):
        raise ValueError('Invalid opportunity-only population')
    book, combined = design['feature_views']['book'], design['feature_views']['combined']
    pieces, audit, populations, epochs = [], [], [], []
    for origin, boundary in ORIGINS.items():
        cutoff = pd.Timestamp(boundary)
        train, test = training_rows(frame, cutoff), forward_rows(frame, cutoff)
        if train.empty or test.empty or train.target.nunique() != 2:
            raise ValueError('Insufficient binary training history')
        for population, part in [('training', train), ('full_forward', test), ('common_forward', test.loc[test.time.ge(COMMON_START)])]:
            count = part.groupby('time').size()
            populations.append(dict(origin=origin, population=population, rows=len(part), events=part.event_slug.nunique(),
                first=part.time.min(), last=part.time.max(), scheduled_hours=int((END-cutoff)/HORIZON) if population=='full_forward' else None,
                known=int(part.label_known.sum()), positive=int(part.target.eq(1).sum()), unknown=int((~part.label_known).sum()),
                positive_events=part.loc[part.target.eq(1),'event_slug'].nunique(),
                maximum_candidates=int(count.max()), median_candidates=float(count.median()), competitive_hours_n3=int(count.gt(3).sum())))
        print(f'{origin}: {len(train)} training rows, {int(train.target.sum())} positives; {len(test)} forward rows', flush=True)
        scores = {'quoted_gap_rule': test.x_best_quoted_net_usd.fillna(-1e12).to_numpy(),
            'depth_rule': test.x_ask_depth_sum.fillna(-1e12).to_numpy(),
            'freshness_rule': -test.x_book_age_max_s.fillna(1e12).to_numpy()}
        for name in ('logistic','random_forest','xgboost'):
            started = perf_counter()
            model = fit_classifier(train, name, design['parameters'][name], book)
            key = f'fixed_book_{name}'
            joblib.dump(model, output/'fitted_models'/f'{origin}_{key}.joblib', compress=3)
            scores[key] = predict(model, test[book])
            audit.append(dict(origin=origin, model=key, seed=SEED, task='classifier', training_rows=len(train),
                positive=int(train.target.sum()), train_last=train.time.max(), label_end_last=train.time.max()+HORIZON,
                cutoff=cutoff, features=json.dumps(book), seconds=perf_counter()-started))
        forest = fit_value(train,'random_forest','two_stage',design['forest_value_params'],combined)
        joblib.dump(forest, output/'fitted_models'/f'{origin}_forest_two_stage.joblib', compress=3)
        scores['forest_two_stage'] = forest.predict(test[combined])
        audit.append(dict(origin=origin, model='forest_two_stage', seed=SEED, task='hurdle', training_rows=len(train),
            positive=int(train.target.sum()), train_last=train.time.max(), label_end_last=train.time.max()+HORIZON,
            cutoff=cutoff, features=json.dumps(combined)))
        inner_start = pd.Timestamp(design['development_start'])+(cutoff-pd.Timestamp(design['development_start']))*.75
        inner_start = inner_start.floor('h')
        inner_train = training_rows(frame, inner_start)
        valid = frame.loc[frame.time.ge(inner_start) & (frame.time+HORIZON).lt(cutoff)]
        chosen_epochs = {}
        for task in ('classifier', 'amount'):
            print(f'{origin}: TabM {task} epoch selection', flush=True)
            model = fit_tabm(inner_train, valid, task, design['tabm_params'], combined, device)
            chosen_epochs[task] = model.best_epoch_
            epochs.append(dict(origin=origin, task=task, epochs=model.best_epoch_, inner_train_rows=len(inner_train),
                inner_train_last=inner_train.time.max(), validation_start=inner_start, validation_last=valid.time.max(), cutoff=cutoff))
        members = []
        for seed in SEEDS:
            print(f'{origin}: TabM refit seed {seed}, epochs {chosen_epochs}', flush=True)
            models = {}
            for task in ('classifier','amount'):
                model = fit_tabm(train,None,task,design['tabm_params'],combined,device,seed,chosen_epochs[task])
                joblib.dump(model, output/'fitted_models'/f'{origin}_tabm_{task}_{seed}.joblib', compress=3)
                models[task] = model
                subset = train.loc[train.target.eq(1)] if task == 'amount' else train
                audit.append(dict(origin=origin, model='tabm_two_stage', seed=seed, task=task, training_rows=len(subset),
                    positive=int(subset.target.sum()), train_last=subset.time.max(), label_end_last=subset.time.max()+HORIZON,
                    cutoff=cutoff, features=json.dumps(combined), epochs=chosen_epochs[task]))
            members.append(models['classifier'].predict(test[combined])*models['amount'].predict(test[combined]))
        scores['tabm_two_stage'] = np.mean(members, axis=0)
        ranks = [test[['time']].assign(score=scores[name]).groupby('time').score.rank(pct=True).to_numpy()
            for name in ('fixed_book_logistic','fixed_book_random_forest','tabm_two_stage')]
        scores['rank_ensemble'] = np.mean(ranks, axis=0)
        for name, score in scores.items():
            if not np.isfinite(score).all():
                raise ValueError('Nonfinite scores')
            pieces.append(test.assign(origin=origin, model=name, score=score))
        pd.DataFrame(audit).to_csv(output/'fit_audit.csv', index=False)
        pd.DataFrame(epochs).to_csv(output/'epoch_selection.csv', index=False)
    prediction = pd.concat(pieces, ignore_index=True)
    prediction.to_parquet(output/'predictions.parquet', index=False)
    pd.DataFrame(populations).to_csv(output/'populations.csv', index=False)
    evaluate_predictions(prediction, output)
    for p, digest in design['source_sha256'].items():
        if sha256(PROJECT_ROOT/p) != digest:
            raise ValueError('Code changed during analysis')
    write_json(output/'complete.json', dict(files_sha256={p.relative_to(output).as_posix():sha256(p)
        for p in output.rglob('*') if p.is_file()}, original_final_outcomes_used=False, original_final_protocol_replaced=False))
    print(pd.read_csv(output/'metrics.csv').query("scope == 'common_forward' and top_n == 3")
        [['origin','model','positive_windows','captured_positive_windows','recall','peak_value_recall']].to_string(index=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    parser.add_argument('--device', choices=['cpu','cuda'], default='cpu')
    args = parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        run(args.output, args.device)
