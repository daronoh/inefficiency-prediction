"""Validation and uncertainty summaries for a completed development comparison."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
from src.common import sha256
from src.config import SPEC
from src.models.evaluation import ranked
from src.models.settings import SEED, MODEL_NAMES, method_name, ensemble_name
from src.paths import PROJECT_ROOT, DATA_DIR


def audit_models(root, labels_dir):
    root, labels_dir = Path(root), Path(labels_dir)
    predictions = pd.read_parquet(root / 'predictions.parquet')
    design = json.loads((root / 'design.json').read_text())
    assert design['label_sha256'] == sha256(labels_dir / 'labels.parquet')
    assert not predictions.duplicated(['model', 'event_slug', 'time']).any()
    assert predictions.time.max() < pd.Timestamp(SPEC.lockbox_start)
    assert np.isfinite(predictions.score).all()
    assert predictions.groupby('model').size().nunique() == 1
    for _, frame in predictions.groupby('model'):
        assert ranked(frame, frame.score, 5).groupby('time').size().le(5).all()
        assert frame.loc[~frame.label_known, 'target'].isna().all()
        assert frame.loc[frame.label_known, 'coverage_fraction'].ge(1 - 1e-9).all()
    fits = pd.read_csv(root / 'diagnostics' / 'fit_audit.csv')
    gap = pd.to_datetime(fits.test_first_time, utc=True) - pd.to_datetime(fits.train_last_time, utc=True)
    assert gap.dt.total_seconds().gt(design['embargo_seconds']).all()
    methods = ['logistic', 'random_forest', 'boosted_trees', 'rank_ensemble',
        'boosted_without_history', 'boosted_without_quoted_gap', 'boosted_without_freshness_quality']
    pairs = [(method, 'boosted_trees' if method.startswith('boosted_without_') else 'quoted_gap_rule')
             for method in methods]
    for heldout in (False, True):
        for view in ('metadata', 'combined'):
            pairs.extend((method_name(name, view, heldout), method_name(name, 'book', heldout)) for name in MODEL_NAMES)
            pairs.append((ensemble_name(view, heldout), ensemble_name('book', heldout)))
    comparisons = []
    for method, reference in pairs:
        if method not in set(predictions.model):
            continue
        scores = []
        for name in [method, reference]:
            frame = predictions.loc[predictions.model.eq(name)]
            selected = ranked(frame, frame.score, 5)
            scores.append(frame.assign(hit=frame.index.isin(selected.index) & frame.target.eq(1),
                positive=frame.target.eq(1)).groupby('family')[['hit', 'positive']].sum())
        a, b = scores
        assert a.positive.equals(b.positive)
        totals = pd.DataFrame({'difference': a.hit - b.hit, 'positive': a.positive}).to_numpy()
        rng = np.random.default_rng(SEED)
        draws = totals[rng.integers(0, len(totals), size=(2000, len(totals)))].sum(axis=1)
        valid = draws[:, 1] > 0
        difference = draws[valid, 0] / draws[valid, 1]
        comparisons.append(dict(model=method, reference=reference,
            captured_difference=int(totals[:, 0].sum()),
            recall_difference=float(totals[:, 0].sum() / totals[:, 1].sum()),
            lower95=float(np.quantile(difference, .025)), upper95=float(np.quantile(difference, .975))))
    pd.DataFrame(comparisons).to_csv(root / 'diagnostics' / 'paired_comparisons.csv', index=False)
    population = predictions.drop_duplicates(['event_slug', 'time'])
    population.assign(known_positive=population.target.eq(1)).groupby('family').agg(
        events=('event_slug', 'nunique'), candidate_windows=('time', 'size'),
        known_windows=('label_known', 'sum'), positive_windows=('known_positive', 'sum'),
        new_gap_windows=('future_positive', 'sum')).to_csv(root / 'diagnostics' / 'evaluation_family_counts.csv')
    result = dict(final_lockbox_opened=False, prediction_rows=len(predictions),
        methods=predictions.model.nunique(), positive_windows=int(population.target.eq(1).sum()),
        predictions_sha256=sha256(root / 'predictions.parquet'),
        checks=['unique event/window/model', 'identical candidate populations', 'finite scores',
            'top-five budget', 'unknown targets missing', 'symmetric coverage', 'purged chronological splits'])
    (root / 'audit.json').write_text(json.dumps(result, indent=2) + '\n')
    return pd.DataFrame(comparisons)


def write_evidence(results_dir, features_path, labels_dir):
    """Fingerprint the current submission code and immutable statistical outputs."""
    results_dir, labels_dir = Path(results_dir), Path(labels_dir)
    paths = list((PROJECT_ROOT / 'src').rglob('*.py'))
    paths += list((PROJECT_ROOT / 'notebooks').glob('*.ipynb'))
    paths += [PROJECT_ROOT / 'README.md', PROJECT_ROOT / 'docs/dataset.md', PROJECT_ROOT / 'docs/simulator.md',
              PROJECT_ROOT / 'docs/reports/current_results.md', PROJECT_ROOT / 'requirements.txt']
    paths += [p for p in results_dir.rglob('*') if p.is_file() and p.name != 'evidence.json']
    paths += [Path(features_path), labels_dir / 'labels.parquet', labels_dir / 'design.json', labels_dir / 'complete.json']
    def key(path):
        path = path.resolve()
        if path.is_relative_to(PROJECT_ROOT):
            return path.relative_to(PROJECT_ROOT).as_posix()
        return 'DATA_ROOT/' + path.relative_to(DATA_DIR).as_posix()

    files = {key(p): sha256(p) for p in paths}
    result = dict(final_lockbox_opened=False, files=files)
    (results_dir / 'evidence.json').write_text(json.dumps(result, indent=2) + '\n')
    return result
