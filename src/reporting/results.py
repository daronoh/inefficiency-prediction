"""Small tables, figures and a generated findings report for completed runs."""
from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt
from src.data.dataset import load_examples
from src.reporting.workflow import execution_path
from src.paths import REPORTS_DIR
from src.models.settings import FEATURE_SETS, MODEL_NAMES, method_name, ensemble_name
from src.reporting.economics import plot_pnl, trade_economics, timing_table, capacity_comparison, capacity_events
from src.reporting.workflow import timing_path, capacity_path

METHODS = ['logistic', 'random_forest', 'boosted_trees', 'rank_ensemble', 'quoted_gap_rule', 'depth_rule', 'freshness_rule']
NAMES = dict(zip(METHODS, ['Logistic regression', 'Random forest', 'Boosted trees', 'Rank ensemble', 'Quoted-gap rule', 'Depth rule', 'Quote-age rule']))


def ranking_table(run_dir, top_n=5):
    frame = pd.read_csv(Path(run_dir) / 'ranking_metrics.csv')
    return frame.loc[frame.top_n.eq(top_n) & frame.model.isin(METHODS)].set_index('model').reindex(METHODS)[
        ['captured_positive_windows', 'positive_windows', 'recall', 'precision_known', 'future_recall', 'selected_unknown']]


def economic_table(run_dir):
    frame = pd.read_csv(execution_path(run_dir) / 'summary.csv').set_index('model')
    return frame.reindex(METHODS + ['monitor_all'])[
        ['attempts', 'positive_episodes', 'known_pnl_usd', 'partial_attempts', 'unknown_attempts']]


def metadata_comparison(run_dir, heldout=False, include_pnl=False):
    """Matched model/feature-set comparison from the standard result files."""
    run_dir = Path(run_dir)
    ranks = pd.read_csv(run_dir / 'ranking_metrics.csv').query('top_n == 5').set_index('model')
    classification = pd.read_csv(run_dir / 'classification_metrics.csv').set_index('model')
    pnl = pd.read_csv(execution_path(run_dir) / 'summary.csv').set_index('model') if include_pnl else None
    rows = []
    for view in FEATURE_SETS:
        for model in MODEL_NAMES + ['rank_ensemble']:
            key = ensemble_name(view, heldout) if model == 'rank_ensemble' else method_name(model, view, heldout)
            if key not in ranks.index:
                continue
            row = dict(feature_set=view, model=model,
                captured=int(ranks.loc[key, 'captured_positive_windows']), capture=ranks.loc[key, 'recall'],
                precision=ranks.loc[key, 'precision_known'], average_precision=classification.loc[key, 'average_precision'],
                selected_unknown=int(ranks.loc[key, 'selected_unknown']))
            if include_pnl:
                row.update(known_pnl_usd=pnl.loc[key, 'known_pnl_usd'], unknown_attempts=int(pnl.loc[key, 'unknown_attempts']))
            rows.append(row)
    return pd.DataFrame(rows).set_index(['feature_set', 'model'])


def plot_metadata(run_dir):
    table = metadata_comparison(run_dir, include_pnl=True).reset_index()
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ax, metric, label in zip(axes, ['capture', 'known_pnl_usd'], ['Positive-window capture', 'Known simulated P&L ($)']):
        table.pivot(index='model', columns='feature_set', values=metric).reindex(
            MODEL_NAMES + ['rank_ensemble'])[list(FEATURE_SETS)].plot.bar(ax=ax, rot=20)
        ax.set(xlabel='', ylabel=label)
        ax.legend(title='Features', fontsize=8)
    fig.tight_layout()
    return fig


def plot_capture(run_dir):
    frame = pd.read_csv(Path(run_dir) / 'ranking_metrics.csv')
    fig, ax = plt.subplots(figsize=(8, 4))
    for name in METHODS[:5]:
        rows = frame.loc[frame.model.eq(name)].sort_values('top_n')
        ax.plot(rows.top_n, rows.recall, marker='o', label=NAMES[name])
    random = frame.loc[frame.model.str.match(r'random_\d+$')].groupby('top_n').recall.mean()
    ax.plot(random.index, random, '--', color='grey', label='Random mean')
    ax.set(xlabel='Events monitored per window (N)', ylabel='Positive-window capture', ylim=(0, 1.03))
    ax.legend(fontsize=8)
    fig.tight_layout()
    return fig


def markdown_table(frame):
    shown = frame.reset_index()
    rows = ['| ' + ' | '.join(map(str, shown.columns)) + ' |', '| ' + ' | '.join(['---']*len(shown.columns)) + ' |']
    for row in shown.itertuples(index=False, name=None):
        rows.append('| ' + ' | '.join(f'{v:.4f}' if isinstance(v, float) else str(v) for v in row) + ' |')
    return '\n'.join(rows)


def build_report(run_dir, features, labels_dir, spec):
    run_dir = Path(run_dir)
    frame = load_examples(features, labels_dir, spec)
    rank = ranking_table(run_dir)
    pnl = economic_table(run_dir)
    paired = pd.read_csv(run_dir / 'diagnostics' / 'paired_comparisons.csv').set_index('model')
    fits = pd.read_csv(run_dir / 'diagnostics' / 'fit_audit.csv').query("model == 'logistic'").set_index('fold')
    metrics = pd.read_csv(run_dir / 'ranking_metrics.csv')
    ablations = metrics.loc[metrics.top_n.eq(5) & metrics.model.str.startswith('boosted_')].set_index('model')
    families = pd.read_csv(run_dir / 'diagnostics' / 'evaluation_family_counts.csv').set_index('family')
    classification = pd.read_csv(run_dir / 'classification_metrics.csv').set_index('model')
    importance = pd.read_csv(run_dir / 'diagnostics' / 'permutation_importance.csv')
    metadata_importance = importance.loc[importance.model.str.startswith('combined_') &
        (importance.unit.str.startswith('feature:m_') | importance.unit.eq('group:metadata'))].groupby(
            ['model', 'unit'])[['ap_drop', 'recall5_drop']].mean()
    text = [
        '# Development results', '',
        'Generated from the completed notebook workflow. The final test beginning 8 January 2026 remains unopened.', '',
        f'The dataset contains {frame.event_slug.nunique()} ranking-eligible events and {len(frame):,} event-windows. '
        f'{int(frame.label_known.sum()):,} have usable labels, including {int(frame.target.eq(1).sum()):,} positives; '
        f'{int((~frame.label_known).sum()):,} remain unknown. The monitoring interval is {spec.horizon_minutes} minutes.', '',
        '## Model comparison', '',
        'Primary budget: five events. Capture measures how many observed positive hours are selected; it is not classification accuracy. '
        'Unknown outcomes remain in the ranking pool and consume slots.', '',
        markdown_table(rank), '',
        'Classification diagnostics use known targets only. Class-balanced scores are not calibrated probabilities; '
        'the 0.5-threshold diagnostics do not choose the shortlist.', '',
        markdown_table(classification.loc[METHODS[:3]]), '',
        '## Does event metadata help?', '',
        'The same three classifiers and rank ensemble use book features only, metadata only, or both. '
        'Metadata contains event type, broad topic, and days since the earliest frozen constituent market was created. '
        'Creation time is treated as stable; it is not a verified listing date. Generic deadlines are excluded because '
        'the frozen end dates do not establish the schedule known at prediction time. '
        'Categorical encoding and numeric preprocessing are fitted on earlier training rows only.', '',
        'P&L in the metadata comparisons uses the original capped 500 ms execution contract. '
        'The uncapped 100 ms results are reported separately below.', '',
        markdown_table(metadata_comparison(run_dir, include_pnl=True)), '',
        'Each variant uses identical eligible events, labels, folds and monitoring budgets. '
        'Unknown executions are shown alongside known P&L; zero unknown attempts does not establish real-world fills.', '',
        'The following transfer diagnostic removes the evaluated topic family from training as well as respecting chronology. '
        'Unseen categories receive an all-zero one-hot representation; the model cannot learn a coefficient for the omitted topic.', '',
        markdown_table(metadata_comparison(run_dir, heldout=True, include_pnl=True)), '',
        'Within-scan permutation diagnostics for the combined models are averaged equally across folds and repeats below. '
        'Positive drops mean performance worsened when the field was shuffled; negative values mean it improved. '
        'Reliance on a field does not establish that adding it improves a refitted model, or that its effect is causal.', '',
        markdown_table(metadata_importance), '',
        '## Chronological folds', '', markdown_table(fits[['training_rows', 'training_positives', 'train_last_time', 'test_first_time']]), '',
        '## Uncertainty and feature findings', '',
        'Paired 95% intervals resample topic families. An interval crossing zero does not establish an improvement over its comparator. '
        'Few positive families and repeated observations limit inference. '
        'Metadata and combined methods are compared against the same book-only classifier or ensemble; '
        'held-out variants use the corresponding held-out comparator. These intervals are exploratory and not corrected for multiple comparisons.', '',
        markdown_table(paired[['reference', 'recall_difference', 'lower95', 'upper95']]), '',
        'Feature-removal comparisons refit the same boosted classifier. These are development diagnostics, not final-test model selection.', '',
        markdown_table(ablations[['recall', 'precision_known', 'future_recall']]), '',
        'Individual and grouped permutation importance is saved in diagnostics/ and displayed in the model notebook.', '',
        markdown_table(families[['known_windows', 'positive_windows', 'new_gap_windows']]), '',
        '## Original capped trading simulation', '', markdown_table(pnl), '',
        'The next table separates successful-basket profit from actual partial-basket unwind losses. '
        'Amounts are per attempted basket, which can contain many shares; a per-share margin is not a per-trade profit.', '',
        markdown_table(trade_economics(run_dir)), '',
        'These are conditional displayed-depth results using immediate submission, 0.5-second arrival, one-second partial-basket unwind, '
        'zero exchange trading fees for the historical sample and a $1,000 limit per attempt. '
        'Fee verification combines frozen market flags with the [dated exchange rollout](https://docs.polymarket.com/changelog/predictions); '
        'the January 2026 introduction covered 15-minute crypto markets, excluded here. There is no quote-age cutoff. '
        'There is no additional imposed failure probability. Actual partial-basket losses are included in known P&L. '
        'Funding, settlement cash release, a common portfolio budget, competition and measured information-delivery latency are not modelled. '
        'The cumulative P&L curve is not a cash balance.', '',
        'The target rewards finding at least one positive attempt. Ranking more positive hours need not maximise dollar returns. '
        'The simulator also retains losing and unknown attempts, including witnessed trades from partly observed windows.', '',
    ]
    if (capacity_path(run_dir)/'summary.csv').exists():
        capacity = pd.read_csv(capacity_path(run_dir)/'summary.csv').set_index('model')
        capacity_manifest = json.loads((capacity_path(run_dir)/'run.json').read_text())
        discovery = pd.DataFrame(capacity_manifest['discovery_audit']).drop(columns='event_slug').sum()
        events = capacity_events(run_dir)
        text.extend(['## Uncapped depth at 100 ms', '',
            'Orders arrive after an assumed 100 ms. There is no artificial cash cap or share limit: '
            'the decision-time optimizer selects the most profitable permitted integer quantity from displayed depth. '
            'Minimum sizes, ticks, positive worst-limit margin, frozen order limits, shared-quote reservations '
            'and the one-second partial-basket unwind/hold remain in force. No extra failure probability is imposed.', '',
            'The first two columns isolate the earlier latency change; the uncapped column additionally changes sizing '
            'and rebuilds discovery without the old quantity grid or cash cap. Models, labels, folds and hourly selections '
            'remain fixed. These dollar results therefore evaluate existing models under a new execution scenario; '
            'the classification metrics above still describe the original capped 500 ms target. '
            'The eligible ranking population is unchanged, so this is not a census of all original-universe opportunities.', '',
            markdown_table(capacity_comparison(run_dir)), '',
            f"Discovery audit: {int(discovery.original_signals):,} original and {int(discovery.uncapped_signals):,} uncapped "
            f"entry signals in the eligible evaluation windows; {int(discovery.shared_starts):,} starts are shared, "
            f"{int(discovery.added_starts):,} added and {int(discovery.removed_starts):,} removed. "
            'Removing the artificial limits need not create new gaps; it can increase the quantity traded in existing gaps.', '',
            'Each policy has its own persistent replay ledger. Largest reserved cash is a per-attempt requirement, '
            'not the portfolio funding requirement. Unlimited funding and attainable displayed depth are assumptions; '
            '100 ms is not a measured historical venue latency. Unknown attempted outcomes are excluded from known P&L, '
            'rather than valued at zero.', '',
            markdown_table(capacity.loc[['logistic','quoted_gap_rule','monitor_all'],
                ['attempts','completed','partial_attempts','unknown_attempts','median_quantity',
                 'median_completed_profit_usd','cumulative_purchase_cash_usd','unknown_entry_cash_usd']]), '',
            markdown_table(trade_economics(run_dir, capacity_path(run_dir))), '',
            f"The quoted-gap rule earns ${capacity.loc['quoted_gap_rule','known_pnl_usd']:,.2f}, compared with "
            f"${capacity.loc['logistic','known_pnl_usd']:,.2f} for book-only logistic regression. "
            'Higher profits from larger positions alone do not establish an advantage from learned rankings. '
            'Monitor-all is a reference policy, not a profit upper bound: selecting more events can add losing attempts '
            'and changes the reservation ledger.', '',
            'Event-level concentration for the book-only logistic policy (largest known P&L first):', '',
            markdown_table(events), ''])
    if (timing_path(run_dir)/'summary.csv').exists():
        timings = timing_table(run_dir).reset_index()
        shown = timings.loc[timings.model.isin(['logistic','quoted_gap_rule','monitor_all'])].set_index(['scenario','model'])
        text.extend(['## Execution timing sensitivity', '',
            'Predictions, labels, entry signals and monitoring choices are fixed. These are execution-only comparisons, '
            'not models retrained for a new latency target. All scenarios retain depth, minimum size, ticks, the $1,000 '
            'cash cap, the 1,000-share limit and shared-quote reservations.', '',
            markdown_table(shown[['arrival_delay_s','unwind_delay_s','attempts','partial_attempts','unknown_attempts','known_pnl_usd']]), '',
            'The arrival scenarios keep a one-second unwind/hold. `instant_no_hold` removes arrival delay and that hold: '
            'it assumes simultaneous access to displayed liquidity, rather than demonstrating real fills or a market-wide '
            'profit upper bound. `fast_unwind_100ms` changes both unwind timing and the conservative scheduler hold/reservation '
            'time, including after completed baskets; its difference cannot be attributed solely to faster failed-leg exits.', '',
            'Faster execution is plausible but must be measured for the relevant venue, market and period. '
            'Source-time snapshots do not measure feed delivery, acknowledgements or competition. '
            'A batch is not guaranteed to succeed as one basket: the [official order documentation](https://docs.polymarket.com/trading/place-orders) '
            'describes independent order results. Retaining a failed-leg policy is necessary for nonzero-delay execution scenarios.', ''])
    text.extend(['See [the dataset guide](../dataset.md) and [the simulator guide](../simulator.md) for definitions and limitations.', ''])
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    target = REPORTS_DIR / 'current_results.md'
    target.write_text('\n'.join(text), encoding='utf-8')
    return target
