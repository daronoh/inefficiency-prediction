"""Prepare presentation tables and a resource-cap plot from saved allocations."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

from FYP.reporting.robustness_extension import NAMES

NAMES = dict(NAMES, earlier_only_selector='Earlier-only chooser')
BEST = 'fixed_book_random_forest'
HISTORY = 'history_event_mean_value'


def prepare(root):
    root = Path(root)
    extension = root / 'artifacts/extensions/allocation_review_v1'
    if not (extension / 'complete.json').exists():
        raise ValueError('Run the allocation experiment before preparing its report')
    tables, entries, widths = extension / 'tables', {}, {}

    def add(key, caption, frame, fractions):
        path = tables / ('report_' + key + '.csv')
        frame.to_csv(path, index=False)
        entries[key] = dict(caption=caption, file=path.relative_to(root / 'artifacts').as_posix())
        widths[key] = fractions

    resource = pd.read_csv(tables / 'token_budget_metrics.csv')
    order = [BEST, 'fixed_book_logistic', HISTORY, 'quoted_gap_rule', 'freshness_rule']
    primary = resource.loc[resource.token_budget.eq(50)].set_index('model')
    rows = []
    for model in order:
        r = primary.loc[model]
        rows.append({'Policy': NAMES[model], 'Tokens / scan': f'{r.mean_tokens:.2f}',
                     'Captured / 359': int(r.captured), 'Recall (%)': f'{100*r.recall:.2f}',
                     'Value (%)': f'{100*r.value_capture:.4f}'})
    add('token_cap', 'Full-period capture with a cap of 50 event-token observations per scan',
        pd.DataFrame(rows), [.32, .17, .17, .17, .17])

    selector = pd.read_csv(tables / 'selector_metrics.csv').set_index('model')
    rows = []
    for model in ['earlier_only_selector', *order]:
        r = selector.loc[model]
        rows.append({'Policy': NAMES[model], 'Captured / 77': int(r.captured),
                     'Recall (%)': f'{100*r.recall:.2f}', 'Value (%)': f'{100*r.value_capture:.2f}'})
    add('policy_choice', 'Common post-warm-up evaluation at N=3: 9 December 2025 to 3 February 2026, 08:00 UTC',
        pd.DataFrame(rows), [.40, .20, .20, .20])

    cases = pd.read_csv(tables / 'paired_cases.csv')
    event_names = {'honduras-presidential-election': 'Honduras election',
                   'avatar-fire-and-ash-opening-weekend-box-office': 'Avatar box office'}
    rows = []
    for r in cases.itertuples():
        rows.append({'Case / event': r.case_id + ': ' + event_names.get(r.event_slug, r.event_slug.replace('-', ' ')),
                     'Scan (UTC)': pd.Timestamp(r.time).strftime('%d %b %Y %H:%M'),
                     'Peak value ($)': f'{r.value_target:.2f}', 'Forest rank': r.forest_rank,
                     'History rank': r.history_rank})
    add('paired_cases', 'Largest known-value examples of selection disagreements and joint misses, earlier subset, N=3',
        pd.DataFrame(rows), [.28, .27, .15, .15, .15])

    assumptions = [
        ('Earlier-only inputs and outcomes', 'Backward as-of tests, strict fitting cutoffs and per-scan/per-origin availability audits.',
         'Implemented safeguards; source-to-receiver latency is unmeasured.'),
        ('Monitoring budget represents work', 'Common token-observation caps supplement equal event counts.',
         'Proxy only; event overlap, update rates and total computing cost are not measured.'),
        ('Full historical feedback is available', 'Historical controls and the chooser admit known labels only after their windows end.',
         'Unselected events also need labels; this acquisition requirement is not validated.'),
        ('Relationships transfer across events', 'Whole-family exclusions, event-balanced summaries and dominant-event removal.',
         'Mixed transfer; no independent future cohort.'),
        ('Execution approximates obtainable fills', 'Persistent replay, partial-basket accounting and 0 to 500 ms arrival scenarios.',
         'Simulated fills; queue effects, bounded funding and live execution remain untested.'),
    ]
    add('assumptions_checks', 'Principal assumptions, their checks and the remaining limits',
        pd.DataFrame(assumptions, columns=['Assumption', 'Check', 'Remaining limit']), [.25, .38, .37])

    f, h = primary.loc[BEST], primary.loc[HISTORY]
    s = selector.loc['earlier_only_selector']
    evidence = [
        ('Does learned screening improve capture?', 'N=3 pooled and event-balanced comparisons, historical controls and paired intervals.',
         'Strong cohort-specific capture; advantage depends on the comparator and weighting.'),
        ('Which information supports the result?', 'Controlled feature ablations, family exclusions and paired decision cases.',
         'Predictive dependence within the recorded cohort; no causal attribution.'),
        ('Does capture survive a common resource cap?',
         f'At 50 tokens, forest recall {100*f.recall:.2f}% and historical-mean recall {100*h.recall:.2f}%; logistic leads this cap.',
         'Common cap on a proxy; no measured end-to-end efficiency gain.'),
        ('Can the policy be chosen before its evaluation week?',
         f'The earlier-only chooser selects the forest at all nine origins and captures {int(s.captured)}/{int(s.positive)} positives.',
         'Post-warm-up inspected-data test with full feedback; no untouched final test.'),
        ('Does opportunity capture establish economic advantage?',
         'Separate simulated execution, latency sensitivity and December Fed exclusion.',
         'Economic advantage over quoted gap disappears on exclusion; no historical-control replay.'),
    ]
    add('evidence_scope', 'Research questions, supporting evidence and the scope of the conclusion',
        pd.DataFrame(evidence, columns=['Question', 'Evidence', 'Supported scope']), [.25, .39, .36])
    (extension / 'report_tables.json').write_text(json.dumps(entries, indent=2) + '\n', encoding='utf-8')
    (extension / 'table_widths.json').write_text(json.dumps(widths, indent=2) + '\n', encoding='utf-8')

    directory = extension / 'figures'
    directory.mkdir(exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.1), layout='constrained')
    for model in order:
        part = resource.loc[resource.model.eq(model)].sort_values('token_budget')
        axes[0].plot(part.token_budget, 100 * part.recall, marker='o', label=NAMES[model])
        axes[1].plot(part.token_budget, 100 * part.value_capture, marker='o')
    for ax, label in zip(axes, ['Positive-hour capture (%)', 'Quoted-value capture (%)']):
        ax.set_xticks([10, 25, 50, 100]); ax.set_xlabel('Token-observation cap per scan', fontsize=11)
        ax.set_ylabel(label, fontsize=11); ax.grid(alpha=.2)
    axes[0].set_ylim(45, 102); axes[1].set_ylim(90, 100.5)
    axes[0].legend(fontsize=9)
    name = 'token_budget_capture.png'
    fig.savefig(directory / name, dpi=180)
    plt.close(fig)
    prefix = extension.relative_to(root / 'artifacts').as_posix()
    pd.DataFrame([dict(number=301, title='Capture under common monitoring-resource caps',
        file=f'{prefix}/figures/{name}', source_table=f'{prefix}/tables/token_budget_metrics.csv',
        caption='Full period; no three-event cap. Whole events are packed in score order. Shared tokens across events are counted separately. Greedy allocations need not be nested as the cap rises; scanning and inference costs are excluded.')]).to_csv(extension / 'figure_index.csv', index=False)
