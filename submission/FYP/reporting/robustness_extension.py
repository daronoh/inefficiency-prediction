"""Render saved robustness results, without refitting or access to raw books."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from FYP.reporting.artifacts import LABELS

NAMES = dict(LABELS, history_event_frequency='Past event frequency',
             history_event_mean_value='Past event mean value',
             history_last_known_value='Last known event value',
             history_family_mean_value='Past family mean value')
FAMILIES = dict(central_bank='Central bank', crypto='Crypto', culture='Culture',
                elections='Elections', geopolitics='Geopolitics', shutdown='Shutdown',
                sports='Sports', technology_business='Technology / business', tweet_counts='Tweet counts')


def pct(value):
    return f'{100 * value:.2f}' if np.isfinite(value) else 'n/a'


def prepare(root):
    root = Path(root)
    extension = root / 'artifacts/extensions/robustness_review_v1'
    if not (extension / 'complete.json').exists():
        raise ValueError('Run the robustness experiment before preparing its report')
    tables, entries, widths = extension / 'tables', {}, {}

    def add(key, caption, frame, fractions):
        path = tables / ('report_' + key + '.csv')
        frame.to_csv(path, index=False)
        entries[key] = dict(caption=caption, file=path.relative_to(root / 'artifacts').as_posix())
        widths[key] = fractions

    summary = pd.read_csv(tables / 'balanced_summary.csv').set_index('model')
    order = ['fixed_book_random_forest', 'quoted_gap_rule', 'freshness_rule',
             'history_event_frequency', 'history_event_mean_value',
             'history_last_known_value', 'history_family_mean_value']
    rows = []
    for model in order:
        r = summary.loc[model]
        rows.append({'Policy': NAMES[model], 'Captured / 359': int(r.captured),
                     'Recall (%)': pct(r.recall), 'Value (%)': pct(r.value_capture),
                     'Unknown slots (%)': pct(r.selected_unknown / r.selected)})
    add('history_controls', 'Historical-label controls on the full evaluation period, N=3',
        pd.DataFrame(rows), [.34, .16, .15, .15, .20])

    transfer = pd.read_csv(tables / 'family_transfer.csv')
    grouped = transfer.groupby(['withheld_family', 'model']).agg(
        positive=('positive', 'first'), events=('events', 'first'),
        recall=('recall', 'mean'), value_capture=('value_capture', 'mean'),
        recall_min=('recall', 'min'), recall_max=('recall', 'max'),
        value_min=('value_capture', 'min'), value_max=('value_capture', 'max')).reset_index()
    grouped.to_csv(tables / 'report_family_seed_summary.csv', index=False)
    rows = []
    for name in sorted(transfer.withheld_family.unique()):
        part = grouped.loc[grouped.withheld_family.eq(name)].set_index('model')
        full, held, logistic = [part.loc[m] for m in ('unrestricted_random_forest', 'withheld_random_forest', 'withheld_logistic')]
        rows.append({'Target family': FAMILIES[name], 'Positive hours': int(held.positive),
                     'Full RF recall (%)': pct(full.recall), 'Held RF recall (%)': pct(held.recall),
                     'Held RF value (%)': pct(held.value_capture), 'Held LR recall (%)': pct(logistic.recall)})
    add('family_transfer', 'Earlier-period family transfer at N=3; forest columns are three-seed means',
        pd.DataFrame(rows), [.25, .13, .155, .155, .155, .155])

    balanced_order = order[:3] + ['fixed_book_logistic', 'tabm_two_stage'] + order[3:]
    rows = []
    for model in balanced_order:
        r = summary.loc[model]
        rows.append({'Policy': NAMES[model], 'Event recall (%)': pct(r.macro_event_recall),
                     'Event value (%)': pct(r.macro_event_value), 'Family recall (%)': pct(r.macro_family_recall),
                     'Family value (%)': pct(r.macro_family_value)})
    add('balanced_results', 'Equal-weight capture over 12 positive events and eight positive families, N=3',
        pd.DataFrame(rows), [.34, .165, .165, .165, .165])

    intervals = pd.read_csv(tables / 'balanced_intervals.csv')
    rows = []
    for model in ['quoted_gap_rule', 'freshness_rule', 'fixed_book_logistic', 'tabm_two_stage', 'history_event_mean_value']:
        part = intervals.loc[intervals.comparator.eq(model) & intervals.metric.eq('value_capture')].set_index('resampling_unit')
        event, fam = part.loc['event'], part.loc['family']
        rows.append({'Comparator': NAMES[model], 'Difference (pp)': pct(event.difference),
                     'Event 95% interval (pp)': f'[{pct(event.lower95)}, {pct(event.upper95)}]',
                     'Family 95% interval (pp)': f'[{pct(fam.lower95)}, {pct(fam.upper95)}]'})
    add('balanced_intervals', 'Forest-minus-control differences in mean event value capture; conditional paired intervals',
        pd.DataFrame(rows), [.32, .16, .26, .26])
    (extension / 'report_tables.json').write_text(json.dumps(entries, indent=2) + '\n', encoding='utf-8')
    (extension / 'table_widths.json').write_text(json.dumps(widths, indent=2) + '\n', encoding='utf-8')

    directory = extension / 'figures'
    directory.mkdir(exist_ok=True)
    prefix = extension.relative_to(root / 'artifacts').as_posix()
    catalog = []

    def save(number, name, title, caption, source, fig):
        fig.savefig(directory / (name + '.png'), dpi=180)
        plt.close(fig)
        catalog.append(dict(number=number, title=title, file=f'{prefix}/figures/{name}.png',
                            source_table=f'{prefix}/tables/{source}', caption=caption))

    active = grouped.loc[grouped.positive.gt(0)]
    names = sorted(active.withheld_family.unique())
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.7), sharey=True, layout='constrained')
    for offset, (model, label, marker) in zip([-.20, 0, .20], [('unrestricted_random_forest', 'Unrestricted forest', 'o'),
                                 ('withheld_random_forest', 'Family-withheld forest', 's'),
                                 ('withheld_logistic', 'Family-withheld logistic', '^')]):
        part = active.loc[active.model.eq(model)].set_index('withheld_family').loc[names]
        for ax, metric in zip(axes, ['recall', 'value_capture']):
            range_prefix = 'recall' if metric == 'recall' else 'value'
            error = 100 * np.array([part[metric] - part[range_prefix + '_min'],
                                    part[range_prefix + '_max'] - part[metric]])
            if error.min() < -1e-9:
                raise ValueError('Seed mean lies outside its range')
            error = np.maximum(error, 0)  # Decimal CSV round trips can give tiny negatives.
            ax.errorbar(100 * part[metric], np.arange(len(names)) + offset, xerr=error,
                        fmt=marker, capsize=2, label=label, markersize=5)
    for ax, label in zip(axes, ['Positive-hour capture (%)', 'Quoted-value capture (%)']):
        ax.set_xlim(-3, 103); ax.set_xlabel(label, fontsize=12)
        ax.tick_params(labelsize=10)
        ax.grid(axis='x', alpha=.2)
    axes[0].set_yticks(range(len(names)), [FAMILIES[f] for f in names], fontsize=11)
    axes[0].invert_yaxis()
    axes[0].legend(fontsize=9, loc='lower left', bbox_to_anchor=(0, 1.01))
    save(201, 'family_transfer', 'Transfer when a whole family is excluded from fitting',
         'Earlier feature subset, N=3. Each family has its own fitted scenario; selection ranks the whole candidate pool. Forest points show three-seed means and whiskers show seed ranges, not confidence intervals. Zero-positive families remain in the table.',
         'family_transfer.csv', fig)

    shown = ['fixed_book_random_forest', 'quoted_gap_rule', 'freshness_rule', 'history_event_mean_value']
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.3), layout='constrained')
    for ax, metrics, label in [(axes[0], ['recall', 'macro_event_recall'], 'Positive-hour capture (%)'),
                                (axes[1], ['value_capture', 'macro_event_value'], 'Quoted-value capture (%)')]:
        x = np.arange(len(shown))
        ax.bar(x - .18, 100 * summary.loc[shown, metrics[0]], width=.36, label='Pooled')
        ax.bar(x + .18, 100 * summary.loc[shown, metrics[1]], width=.36, label='Equal event weight')
        ax.set_xticks(x, ['Forest', 'Quoted gap', 'Freshness', 'Past event mean'], rotation=25, ha='right', fontsize=11)
        ax.set_ylim(0, 105); ax.set_ylabel(label, fontsize=12)
        ax.tick_params(axis='y', labelsize=10)
    axes[0].legend(fontsize=10)
    save(202, 'balanced_capture', 'Pooled and event-balanced capture measure different objectives',
         'Full period, unchanged global N=3 selections. Equal-event means use the 12 events with known positive evaluation hours; zero-positive events still compete for slots.',
         'balanced_summary.csv', fig)
    pd.DataFrame(catalog).to_csv(extension / 'figure_index.csv', index=False)
