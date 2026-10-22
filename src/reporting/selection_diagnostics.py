"""Explain fixed development rankings using their actual persistent replay.

This is post-hoc accounting, not retraining, a new strategy, or causal evidence
that a feature prevented a loss. Standalone labels are used only for diagnostics.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common import sha256
from src.config import require_split_access
from src.data.profitability import load_profitability
from src.models.evaluation import ranked
from src.models.settings import method_name
from src.reporting.profitability import PRIMARY
from src.reporting.workflow import execution_path, profitability_dataset, profitability_run

REFERENCE = 'quoted_gap_rule'
FOCUS = [REFERENCE, 'combined_random_forest', 'combined_reg_ensemble',
         'combined_two_stage_random_forest', 'monitor_all']
LABELS = dict(zip(FOCUS, ['Quoted gap', 'Random forest + metadata',
                        'Regression ensemble + metadata', 'Two-stage forest + metadata', 'Monitor all']))
WINDOW = ['event_slug', 'time']
TRADE = ['event_slug', 'decision_time', 'pair_id', 'start_time']


def onset_categories(frame):
    """Separate true observed onsets from left-censored first observations."""
    if frame[['original_spell_start', 'decision_time', 'start_observed']].isna().any().any():
        raise ValueError('Onset evidence must be present for every attempt')
    visible = frame.original_spell_start.le(frame.decision_time)
    return pd.Series(np.where(visible, 'visible_at_scan',
                     np.where(frame.start_observed, 'new_observed_after_scan',
                              'first_seen_after_scan_onset_unknown')), index=frame.index)


def mark_scan_state(frame):
    value = frame.x_best_quoted_net_usd
    return np.where(value.isna(), 'quote_unavailable',
                    np.where(value.gt(0), 'positive_quote', 'nonpositive_quote'))


def selection_attribution(selected, reference, attempts, model):
    """Exact event-window accounting; common selections can have different histories."""
    keys = selected[WINDOW].merge(reference[WINDOW], on=WINDOW, how='outer',
                                  indicator=True, validate='one_to_one')
    keys['selection_group'] = keys['_merge'].map({'left_only': 'model_only',
        'right_only': 'rule_only', 'both': 'both'}).astype(str)
    for policy, prefix in [(model, 'model'), (REFERENCE, 'rule')]:
        part = attempts.loc[attempts.model.eq(policy)].drop(columns='time', errors='ignore').rename(columns={'decision_time': 'time'})
        sums = part.groupby(WINDOW).agg(net_usd=('net_usd', 'sum'),
                attempts=('net_usd', 'size'), unknown=('unknown_pnl', 'sum'))
        keys = keys.merge(sums.add_prefix(prefix + '_'), on=WINDOW, how='left', validate='one_to_one')
        for name in ('net_usd', 'attempts', 'unknown'):
            keys[prefix + '_' + name] = keys[prefix + '_' + name].fillna(0)
    keys['difference_usd'] = keys.model_net_usd - keys.rule_net_usd
    return keys.drop(columns='_merge')


def compare_attempts(attempts, model):
    """Outer join actual attempts. Absence is zero; unknown execution stays unknown."""
    columns = TRADE + ['net_usd', 'status', 'quantity', 'onset', 'scan_state',
                       'quoted_gross_usd', 'n_filled_legs', 'n_members', 'episode_id']
    left = attempts.loc[attempts.model.eq(model), columns]
    right = attempts.loc[attempts.model.eq(REFERENCE), columns]
    result = left.merge(right, on=TRADE, how='outer', suffixes=('_model', '_rule'),
                        indicator=True, validate='one_to_one')
    result['presence'] = result['_merge'].map({'left_only': 'model_only',
        'right_only': 'rule_only', 'both': 'both'}).astype(str)
    result.loc[result.presence.eq('rule_only'), 'net_usd_model'] = 0.
    result.loc[result.presence.eq('model_only'), 'net_usd_rule'] = 0.
    result['difference_usd'] = result.net_usd_model - result.net_usd_rule
    result['unknown_comparison'] = result.net_usd_model.isna() | result.net_usd_rule.isna()
    result['known_subtotal_difference_usd'] = result.net_usd_model.fillna(0) - result.net_usd_rule.fillna(0)
    return result.drop(columns='_merge').assign(model=model)


def _summarise_attempts(frame, groups):
    return frame.groupby(groups, dropna=False).agg(attempts=('net_usd', 'size'),
        episodes=('episode_id', 'nunique'), known_pnl_usd=('net_usd', 'sum'),
        completed_pnl_usd=('completed_pnl_usd', 'sum'), partial_pnl_usd=('partial_pnl_usd', 'sum'),
        losing_attempts=('loss', 'sum'), unknown_attempts=('unknown_pnl', 'sum')).reset_index()


def build_diagnostics(run_dir, dataset_dir, output):
    """Create a new immutable diagnostics folder from matching development artifacts."""
    require_split_access('development')
    root, data, output = Path(run_dir), Path(dataset_dir), Path(output)
    if output.exists():
        raise FileExistsError('Choose a new diagnostics folder; completed evidence is retained')
    execution = execution_path(root)
    design = json.loads((root / 'design.json').read_text())
    completion = json.loads((root / 'complete.json').read_text())
    replay = json.loads((execution / 'run.json').read_text())
    prediction_hash, example_hash = sha256(root / 'predictions.parquet'), sha256(data / 'examples.parquet')
    if prediction_hash != completion['predictions_sha256'] or prediction_hash != replay['predictions_sha256']:
        raise ValueError('Prediction manifests do not match')
    if example_hash != design['dataset_sha256'] or example_hash != replay['examples_sha256']:
        raise ValueError('Dataset and replay manifests do not match')
    if any(x.get('final_lockbox_opened') for x in (design, completion, replay)):
        raise ValueError('This analysis accepts unopened-test development runs only')
    spec = design['dataset_design']['contract']
    cutoff = pd.Timestamp(spec['lockbox_start'])
    first = pd.Timestamp(spec['development_start'])
    examples = load_profitability(data)
    prediction = pd.read_parquet(root / 'predictions.parquet')
    methods = [method_name(name, view) for view in ('book', 'combined') for name in PRIMARY]
    methods += [REFERENCE, 'depth_rule', 'freshness_rule']
    prediction = prediction.loc[prediction.model.isin(methods)].copy()
    if prediction.duplicated(['model'] + WINDOW).any():
        raise ValueError('Duplicate predictions')
    if not (prediction.time.ge(first) & (prediction.time + pd.Timedelta(minutes=spec['horizon_minutes'])).le(cutoff)).all():
        raise ValueError('Prediction window crosses development boundary')
    pool = prediction.loc[prediction.model.eq(REFERENCE)].copy()
    if pool.empty:
        raise ValueError('Quoted-gap reference is missing')
    candidates = set(map(tuple, pool[WINDOW].to_numpy()))
    for name, part in prediction.groupby('model'):
        if set(map(tuple, part[WINDOW].to_numpy())) != candidates:
            raise ValueError(f'Candidate population differs for {name}')
    features = examples[WINDOW + ['x_best_quoted_net_usd', 'x_book_age_max_s',
        'x_spread_mean', 'x_spread_max', 'x_ask_depth_sum']]
    pool = pool.merge(features, on=WINDOW, validate='one_to_one')
    pool['scan_state'] = mark_scan_state(pool)
    counts = pool.groupby('time').size()
    pool['candidate_count'] = pool.time.map(counts)
    top_n = int(replay['top_n'])
    pool['competitive'] = pool.candidate_count.gt(top_n)
    signal_paths = sorted((data / 'events').glob('*/signals.parquet'))
    signals = pd.concat([pd.read_parquet(p) for p in signal_paths], ignore_index=True)
    if not signals.time.ge(first).all() or not signals.monitoring_end_time.le(cutoff).all():
        raise ValueError('Signals cross development boundary')
    visible = signals.loc[signals.start_time.eq(signals.time), WINDOW].drop_duplicates()
    visible['qualified_entry_at_scan'] = True
    pool = pool.merge(visible, on=WINDOW, how='left', validate='one_to_one')
    pool['qualified_entry_at_scan'] = pool.qualified_entry_at_scan.eq(True)
    selected_parts = []
    for name, part in prediction.groupby('model'):
        chosen = ranked(part, part.score, top_n)
        selected_parts.append(chosen[['model', 'score'] + WINDOW])
    selected_parts.append(pool[WINDOW].assign(model='monitor_all', score=np.nan))
    selected = pd.concat(selected_parts, ignore_index=True).merge(
        pool.drop(columns=['model', 'score', 'kind']), on=WINDOW, validate='many_to_one')
    attempts = pd.read_parquet(execution / 'attempts.parquet')
    attempts = attempts.loc[attempts.scope.eq('all_events') & attempts.attempted &
                            attempts.model.isin(methods + ['monitor_all'])].copy()
    if not (attempts.start_time.ge(first) & attempts.unwind_time.lt(cutoff)).all():
        raise ValueError('Attempt inspection would cross development boundary')
    onset = signals[['pair_id', 'start_time', 'time', 'original_spell_start',
                     'start_observed', 'episode_id', 'boundary_continuation']]
    attempts = attempts.merge(onset, left_on=['pair_id', 'start_time', 'decision_time'],
        right_on=['pair_id', 'start_time', 'time'], how='left', validate='many_to_one')
    attempts['onset'] = onset_categories(attempts)
    attempts = attempts.merge(pool[WINDOW + ['fold', 'stratum', 'scan_state',
        'qualified_entry_at_scan', 'label_known', 'x_best_quoted_net_usd']], on=WINDOW,
        how='left', validate='many_to_one')
    if attempts.fold.isna().any():
        raise ValueError('Attempt is outside the evaluation candidate pool')
    selected_keys = set(map(tuple, selected[['model'] + WINDOW].to_numpy()))
    if any((r.model, r.event_slug, r.time) not in selected_keys for r in attempts.itertuples()):
        raise ValueError('Replay contains an unselected event-window')
    cost = spec['assumed_other_cost_usd']
    attempts['net_usd'] = attempts.pnl_usd - cost
    attempts['unknown_pnl'] = attempts.net_usd.isna()
    attempts['loss'] = attempts.net_usd.lt(0)
    attempts['completed_pnl_usd'] = attempts.net_usd.where(attempts.status.eq('complete'), 0.)
    attempts['partial_pnl_usd'] = attempts.net_usd.where(attempts.status.str.startswith('partial_'), 0.)
    totals = _summarise_attempts(attempts, ['model'])
    original = pd.read_csv(execution / 'summary.csv')
    original = original.loc[original.scope.eq('all_events') & original.fold.eq('all')].set_index('model')
    for row in totals.itertuples():
        if not np.isclose(row.known_pnl_usd, original.loc[row.model, 'known_pnl_usd'], atol=1e-7):
            raise ValueError('Attempt totals do not reconcile to saved replay')
    ref = selected.loc[selected.model.eq(REFERENCE)]
    attribution, comparisons, policy_rows = [], [], []
    ref_keys = set(map(tuple, ref[WINDOW].to_numpy()))
    ref_net = float(totals.set_index('model').loc[REFERENCE, 'known_pnl_usd'])
    for name, part in selected.groupby('model'):
        keys = set(map(tuple, part[WINDOW].to_numpy()))
        own = attempts.loc[attempts.model.eq(name)]
        common = keys & ref_keys
        competitive = part.loc[part.competitive]
        comp_keys = set(map(tuple, competitive[WINDOW].to_numpy()))
        row = dict(model=name, selected_windows=len(part), common_rule_windows=len(common),
            common_fraction=len(common) / len(ref_keys),
            competitive_common_fraction=len(comp_keys & ref_keys) / len(ref.loc[ref.competitive]),
            selected_unknown=int((~part.label_known).sum()),
            selected_positive_quote=int(part.scan_state.eq('positive_quote').sum()),
            selected_nonpositive_quote=int(part.scan_state.eq('nonpositive_quote').sum()),
            selected_unavailable_quote=int(part.scan_state.eq('quote_unavailable').sum()),
            positive_hour_recall=float(part.target.eq(1).sum() / pool.target.eq(1).sum()),
            future_positive_hour_recall=float(part.future_positive.sum() / pool.future_positive.sum()),
            net_difference_usd=float(own.net_usd.sum() - ref_net))
        if name != REFERENCE:
            detail = selection_attribution(part, ref, attempts, name)
            detail = detail.merge(pool[WINDOW + ['fold']], on=WINDOW, validate='one_to_one')
            for fold in ['all'] + sorted(detail.fold.unique().tolist()):
                subset = detail if fold == 'all' else detail.loc[detail.fold.eq(fold)]
                table = subset.groupby('selection_group').agg(windows=('time', 'size'),
                    model_net_usd=('model_net_usd', 'sum'), rule_net_usd=('rule_net_usd', 'sum'),
                    difference_usd=('difference_usd', 'sum'), model_attempts=('model_attempts', 'sum'),
                    rule_attempts=('rule_attempts', 'sum'), model_unknown=('model_unknown', 'sum'),
                    rule_unknown=('rule_unknown', 'sum')).reset_index().assign(model=name, fold=fold)
                attribution.append(table)
            if not np.isclose(detail.difference_usd.sum(), row['net_difference_usd'], atol=1e-7):
                raise ValueError('Selection attribution does not reconcile')
            paired = compare_attempts(attempts, name)
            comparisons.append(paired)
            if not np.isclose(paired.known_subtotal_difference_usd.sum(), row['net_difference_usd'], atol=1e-7):
                raise ValueError('Attempt attribution does not reconcile')
            largest = paired.loc[paired.difference_usd.abs().idxmax()]
            row.update(largest_trade_difference_usd=float(largest.difference_usd),
                largest_trade_event=largest.event_slug, largest_trade_time=str(largest.start_time),
                difference_excluding_largest_trade_usd=float(row['net_difference_usd'] - largest.difference_usd))
        policy_rows.append(row)
    summary = pd.DataFrame(policy_rows).merge(totals, on='model', validate='one_to_one')
    onset_summary = _summarise_attempts(attempts, ['model', 'onset', 'scan_state', 'stratum'])
    events = _summarise_attempts(attempts, ['model', 'event_slug', 'stratum'])
    fold_summary = _summarise_attempts(attempts, ['model', 'fold'])
    pools = [pool.assign(population='all_candidates')]
    pools += [g.assign(population=name) for name, g in selected.groupby('model') if name in FOCUS]
    states = pd.concat(pools).groupby(['population', 'scan_state']).agg(
        windows=('time', 'size'), known_windows=('label_known', 'sum'),
        positive_hours=('target', lambda x: x.eq(1).sum()),
        observed_future_positive_hours=('future_positive', 'sum'),
        qualified_entry_windows=('qualified_entry_at_scan', 'sum')).reset_index()
    output.mkdir(parents=True)
    for name, table in dict(policy_summary=summary, selection_attribution=pd.concat(attribution),
        onset_breakdown=onset_summary, event_contributions=events, fold_summary=fold_summary,
        scan_states=states).items():
        table.to_csv(output / (name + '.csv'), index=False)
    selected.to_parquet(output / 'selected_windows.parquet', index=False)
    attempts.to_parquet(output / 'annotated_attempts.parquet', index=False)
    pd.concat(comparisons, ignore_index=True).to_parquet(output / 'attempt_comparisons.parquet', index=False)
    manifest = dict(version=1, top_n=top_n, execution=replay['execution'],
        evaluation_start=str(pool.time.min()), evaluation_last_scan=str(pool.time.max()),
        hours=pool.time.nunique(), competitive_hours=int(counts.gt(top_n).sum()),
        eligible_event_windows=len(pool), eligible_events=pool.event_slug.nunique(),
        mean_candidates=float(counts.mean()), min_candidates=int(counts.min()), max_candidates=int(counts.max()),
        label_known=int(pool.label_known.sum()), final_lockbox_opened=False,
        models_retrained=False, execution_changed=False,
        prediction_sha256=prediction_hash, examples_sha256=example_hash,
        attempts_sha256=sha256(execution / 'attempts.parquet'), source_sha256=sha256(__file__),
        signals_sha256={p.parent.name: sha256(p) for p in signal_paths},
        interpretation='Post-hoc development accounting. Same selected hour may have different persistent reservation history. '
        'Unknown targets compete in rankings. P&L is the known subtotal with unknown attempts counted. '
        'Removing one trade from accounting is not a rerun or a deployable policy. '
        'Observed onsets rely on recorded books and the existing carry-forward capture assumption.')
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return summary


def plot_diagnostics(folder):
    """Separate panels and dollar attribution avoid overlapping P&L curves."""
    folder = Path(folder)
    summary = pd.read_csv(folder / 'policy_summary.csv').set_index('model')
    names = [m for m in FOCUS if m in summary.index]
    figures = {}
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    y = np.arange(len(names))
    for ax, column, title in [(axes[0], 'completed_pnl_usd', 'Completed-basket P&L'),
                              (axes[1], 'partial_pnl_usd', 'Partial-basket P&L')]:
        values = summary.loc[names, column]
        ax.barh(y, values, color=['#315c8c' if m == REFERENCE else '#87aaba' for m in names])
        ax.set_yticks(y, [LABELS[m] for m in names] if ax == axes[0] else [])
        ax.invert_yaxis(); ax.set_title(title); ax.set_xlabel('Simulated dollars')
        ax.bar_label(ax.containers[0], fmt='$%.2f', padding=3, fontsize=8)
        ax.margins(x=.23)
    fig.tight_layout(); figures['profit_and_losses'] = fig
    onset = pd.read_csv(folder / 'onset_breakdown.csv')
    table = onset.groupby(['model', 'onset']).known_pnl_usd.sum().unstack(fill_value=0).reindex(names)
    fig, ax = plt.subplots(figsize=(11, 4.5))
    labels = {'visible_at_scan': 'Gap already open at scan',
        'new_observed_after_scan': 'Observed new gap after scan',
        'first_seen_after_scan_onset_unknown': 'Later first observation; onset unknown'}
    left = np.zeros(len(names))
    for key, color in [('visible_at_scan', '#5b8db8'), ('new_observed_after_scan', '#5da282'),
                        ('first_seen_after_scan_onset_unknown', '#999999')]:
        values = table.get(key, pd.Series(0., index=names)).to_numpy()
        ax.barh(y, values, left=left, label=labels[key], color=color)
        left += values
    ax.set_yticks(y, [LABELS[m] for m in names]); ax.invert_yaxis()
    ax.set_xlabel('Known net simulated P&L ($)'); ax.legend(loc='upper center', bbox_to_anchor=(.5, -.15), fontsize=8)
    ax.set_title('When did the traded gap first appear?'); fig.tight_layout()
    figures['opportunity_onsets'] = fig
    attribution = pd.read_csv(folder / 'selection_attribution.csv')
    names = [m for m in FOCUS if m not in (REFERENCE, 'monitor_all')]
    fig, axes = plt.subplots(1, len(names), figsize=(13, 4.5), sharey=True)
    for ax, name in zip(axes, names):
        table = attribution.loc[attribution.model.eq(name) & attribution.fold.eq('all')].set_index('selection_group')
        values = table.reindex(['model_only', 'rule_only', 'both']).difference_usd.fillna(0)
        ax.bar(range(3), values, color=['#b96158' if v < 0 else '#5da282' for v in values])
        ax.axhline(0, color='#555555', lw=.8)
        ax.set_xticks(range(3), ['Model-only\nhours', 'Rule-only\nhours', 'Shared\nhours'])
        ax.bar_label(ax.containers[0], fmt='$%.2f', padding=3, fontsize=8)
        ax.set_title(LABELS[name], fontsize=10); ax.margins(y=.25)
    axes[0].set_ylabel('Model P&L minus quoted-gap P&L ($)')
    fig.tight_layout(); figures['selection_attribution'] = fig
    for name, fig in figures.items():
        fig.savefig(folder / (name + '.png'), dpi=160, bbox_inches='tight')
    return figures


def trace_loss_case(folder, run_dir, model='combined_random_forest'):
    """Inspect the largest model-only loss using development-only raw books.

    Selection of this case is explicitly retrospective. It does not change
    any trade or establish a rule that would have avoided it prospectively.
    """
    from src.data.io import load_frames, observation_groups
    from src.data.universe import build_plan
    from src.paths import EVENT_UNIVERSE, TOKENS_PARQUET
    require_split_access('development')
    folder, root = Path(folder), Path(run_dir)
    pairs = pd.read_parquet(folder / 'attempt_comparisons.parquet')
    candidates = pairs.loc[pairs.model.eq(model) & pairs.presence.eq('model_only') &
                            pairs.net_usd_model.lt(0)]
    if candidates.empty:
        raise ValueError('No model-only losing attempt for case study')
    case = candidates.loc[candidates.net_usd_model.idxmin()]
    attempts = pd.read_parquet(folder / 'annotated_attempts.parquet')
    mask = attempts.model.eq(model)
    for key in TRADE:
        mask &= attempts[key].eq(case[key])
    row = attempts.loc[mask].iloc[0]
    plan = next(p for p in build_plan('all')[0] if p['event_slug'] == row.event_slug)
    groups, mapping = observation_groups(plan, load_frames(plan), {row.pair_id})
    group, tokens = groups[row.pair_id], mapping[row.pair_id]
    metadata = pd.read_parquet(EVENT_UNIVERSE)
    registry = pd.read_parquet(TOKENS_PARQUET)
    registry['token_id'] = registry.token_id.astype(str)
    registry['market_id'] = registry.market_id.astype(str)
    metadata['market_id'] = metadata.market_id.astype(str)
    names = registry.merge(metadata.drop_duplicates('market_id'), on='market_id', how='left', suffixes=('', '_event'))
    names = names.drop_duplicates('token_id').set_index('token_id')
    limits, fills = json.loads(row.limits_json), set(json.loads(row.filled_legs_json))
    records = []
    for stage, when in [('scan', row.decision_time), ('submit', row.confirmation_time),
                         ('arrival', row.arrival_time), ('unwind', row.unwind_time)]:
        books = group.at(when)
        if books is None:
            raise ValueError('Case-study book is unavailable')
        for leg, (token, book, limit) in enumerate(zip(tokens, books, limits, strict=True)):
            buy = book.asks.quote(row.quantity, limit=limit)
            sell = book.bids.quote(row.quantity)
            records.append(dict(stage=stage, observation_time=when, leg=leg,
                token_id=token, question=names.loc[token].get('question', token),
                snapshot_time=book.stamp, age_seconds=(when-book.stamp).total_seconds(),
                quantity=int(row.quantity), fixed_buy_limit=limit,
                ask_depth_within_limit=float(book.asks.sizes[book.asks.prices <= limit].sum()),
                buy_cost_within_limit_usd=buy[0] if buy else np.nan,
                best_ask=float(book.asks.prices[0]), best_bid=float(book.bids.prices[0]),
                unwind_bid_proceeds_usd=sell[0] if sell else np.nan, filled=leg in fills))
    trace = pd.DataFrame(records)
    arrival = trace.loc[trace.stage.eq('arrival')].set_index('leg')
    filled = set(arrival.loc[arrival.buy_cost_within_limit_usd.notna()].index)
    if filled != fills:
        raise ValueError('Raw case-study fills differ from saved replay')
    unwind = trace.loc[trace.stage.eq('unwind')].set_index('leg')
    replayed = float(unwind.loc[sorted(fills), 'unwind_bid_proceeds_usd'].sum() -
                     arrival.loc[sorted(fills), 'buy_cost_within_limit_usd'].sum())
    if not np.isclose(replayed, row.pnl_usd, atol=1e-7):
        raise ValueError('Raw case-study loss does not reconcile')
    predictions = pd.read_parquet(root / 'predictions.parquet')
    ranking = []
    for name in [REFERENCE, model, 'combined_reg_ensemble', 'combined_two_stage_random_forest']:
        part = predictions.loc[predictions.model.eq(name) & predictions.time.eq(row.decision_time)].copy()
        part = part.sort_values(['score', 'event_slug'], ascending=[False, True])
        part['rank'] = np.arange(1, len(part)+1)
        part['selected'] = part['rank'].le(json.loads((folder/'manifest.json').read_text())['top_n'])
        ranking.append(part[['model', 'event_slug', 'time', 'score', 'rank', 'selected']])
    trace.to_csv(folder / 'loss_case_books.csv', index=False)
    pd.concat(ranking).to_csv(folder / 'loss_case_rankings.csv', index=False)
    record = {key: row[key] for key in ['event_slug', 'decision_time', 'start_time', 'arrival_time',
        'unwind_time', 'quantity', 'n_members', 'n_filled_legs', 'quoted_gross_usd', 'entry_cost_usd', 'pnl_usd']}
    record.update(model=model, reconstructed_pnl_usd=replayed, final_lockbox_opened=False,
                  selection='Largest model-only losing attempt, chosen after examining outcomes')
    (folder/'loss_case.json').write_text(json.dumps(record, indent=2, default=str)+'\n')
    return trace


def seal_diagnostics(folder):
    folder = Path(folder)
    completion = folder / 'complete.json'
    if completion.exists():
        raise FileExistsError('Completed diagnostics cannot be resealed')
    files = {p.name: sha256(p) for p in sorted(folder.iterdir()) if p.is_file()}
    completion.write_text(json.dumps(dict(files_sha256=files, final_lockbox_opened=False), indent=2)+'\n')


def verify_diagnostics(folder, run_dir, dataset_dir):
    folder, root, data = Path(folder), Path(run_dir), Path(dataset_dir)
    completion = json.loads((folder/'complete.json').read_text())
    for name, expected in completion['files_sha256'].items():
        if sha256(folder/name) != expected:
            raise ValueError(f'Diagnostic output changed: {name}')
    manifest = json.loads((folder/'manifest.json').read_text())
    for path, key in [(root/'predictions.parquet', 'prediction_sha256'),
                      (data/'examples.parquet', 'examples_sha256'),
                      (execution_path(root)/'attempts.parquet', 'attempts_sha256')]:
        if sha256(path) != manifest[key]:
            raise ValueError('Diagnostics do not match the active research run')
    return manifest


if __name__ == '__main__':
    plt.switch_backend('Agg')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    summary = build_diagnostics(profitability_run(), profitability_dataset(), args.output)
    plot_diagnostics(args.output)
    trace_loss_case(args.output, profitability_run())
    seal_diagnostics(args.output)
    print(summary.loc[summary.model.isin(FOCUS), ['model', 'known_pnl_usd', 'net_difference_usd',
        'common_fraction', 'future_positive_hour_recall']].to_string(index=False))
