"""Compare fixed predictions under different persistent replay budgets."""
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.config import require_split_access
from src.reporting.workflow import execution_path


def budget_comparison(run_dir, dataset_dir, budgets=(3, 5)):
    """Keep the original primary replay; smaller budgets are separate sensitivities."""
    require_split_access('development')
    root, data = Path(run_dir), Path(dataset_dir)
    output = root / 'budget_sensitivity'
    design = json.loads((root / 'design.json').read_text())
    original = json.loads((execution_path(root) / 'run.json').read_text())
    prediction_hash = sha256(root / 'predictions.parquet')
    example_hash = sha256(data / 'examples.parquet')
    if example_hash != design['dataset_sha256']:
        raise ValueError('Budget comparison dataset differs from the fitted models')
    prediction = pd.read_parquet(root / 'predictions.parquet')
    metrics = pd.read_csv(root / 'metrics.csv')
    pool = prediction.loc[prediction.model.eq('quoted_gap_rule')]
    features = pd.read_parquet(data / 'examples.parquet',
                               columns=['event_slug', 'time', 'x_best_quoted_net_usd'])
    pool = pool.merge(features, on=['event_slug', 'time'], validate='one_to_one')
    cutoff = pd.Timestamp(design['dataset_design']['contract']['lockbox_start'])
    if not pool.time.lt(cutoff).all():
        raise ValueError('Budget comparison must remain in development')
    pool['quoted_available'] = pool.x_best_quoted_net_usd.notna()
    counts = pool.groupby('time').agg(eligible_events=('event_slug', 'size'),
                                     events_with_quoted_score=('quoted_available', 'sum'))
    comparisons, competition, inputs = [], [], {}
    for budget in budgets:
        folder = execution_path(root) if budget == original['top_n'] else output / f'top{budget}'
        manifest = json.loads((folder / 'run.json').read_text())
        if manifest['top_n'] != budget or manifest.get('final_lockbox_opened'):
            raise ValueError('Wrong budget or final-test access in replay manifest')
        if manifest['predictions_sha256'] != prediction_hash or manifest['examples_sha256'] != example_hash:
            raise ValueError('Budget replays must share predictions and targets')
        if manifest['execution'] != original['execution']:
            raise ValueError('Budget replays must share execution assumptions')
        table = pd.read_csv(folder / 'summary.csv')
        table = table.loc[table.fold.eq('all')].copy()
        table['budget'] = budget
        keep = metrics.loc[metrics.top_n.eq(budget), ['model', 'recall', 'future_recall', 'selected_unknown']]
        table = table.merge(keep, on='model', how='left', validate='many_to_one')
        # These ranking metrics describe the full pool, not the event-exclusion pool.
        table.loc[table.scope.ne('all_events'), ['recall', 'future_recall', 'selected_unknown']] = float('nan')
        comparisons.append(table)
        competition.append(dict(budget=budget, hours=len(counts),
            all_events_fit_hours=int(counts.eligible_events.le(budget).sum()),
            all_quoted_events_fit_hours=int(counts.events_with_quoted_score.le(budget).sum()),
            competitive_hours=int(counts.eligible_events.gt(budget).sum()),
            quoted_competitive_hours=int(counts.events_with_quoted_score.gt(budget).sum()),
            selected_event_windows=int(counts.eligible_events.clip(upper=budget).sum()),
            available_event_windows=int(counts.eligible_events.sum())))
        inputs[str(budget)] = dict(manifest_sha256=sha256(folder / 'run.json'),
                                  summary_sha256=sha256(folder / 'summary.csv'))
    result = pd.concat(comparisons, ignore_index=True)
    output.mkdir(parents=True, exist_ok=True)
    result.to_csv(output / 'comparison.csv', index=False)
    counts.to_csv(output / 'candidate_counts.csv')
    pd.DataFrame(competition).to_csv(output / 'competition.csv', index=False)
    report = dict(predictions_sha256=prediction_hash, examples_sha256=example_hash,
        source_sha256=sha256(__file__), budgets=list(budgets), primary_budget=original['top_n'],
        inputs=inputs, final_lockbox_opened=False, models_retrained=False,
        interpretation='Budget sensitivity using identical predictions and execution assumptions. '
        'Known persistent replay P&L, not summed standalone labels. Quoted-score availability '
        'is a screening diagnostic, not a requirement for event eligibility or proof of future coverage. '
        'Slot fractions are not measured compute savings; update processing and screening overhead differ.')
    (output / 'comparison_manifest.json').write_text(json.dumps(report, indent=2)+'\n')
    return result, pd.DataFrame(competition)
