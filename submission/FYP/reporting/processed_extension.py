"""Prepare self-contained dissertation tables from the processed-data extension."""
from __future__ import annotations

import json
from pathlib import Path
import numpy as np
import pandas as pd

from FYP.reporting.artifacts import LABELS


def prepare(root):
    root = Path(root)
    extension = root / 'artifacts/extensions/processed_review_v1'
    tables = extension / 'tables'
    if not (extension / 'complete.json').exists():
        raise ValueError('Run the processed-data experiment before preparing its report tables')
    entries, widths = {}, {}

    def add(key, caption, frame, fractions=None):
        path = tables / ('report_' + key + '.csv')
        frame.to_csv(path, index=False)
        entries[key] = dict(caption=caption, file=path.relative_to(root / 'artifacts').as_posix())
        if fractions:
            widths[key] = fractions

    cohort = json.loads((root / 'inputs/cohort.json').read_text())
    add('cohort_structure', 'Frozen basket membership by structural type', pd.DataFrame([
        {'Structure': kind, 'Events': sum(p['stratum'] == code for p in cohort),
         'Baskets': sum(len(p['packages']) for p in cohort if p['stratum'] == code),
         'Token books': len({t for p in cohort if p['stratum'] == code for t in p['paths']})}
        for kind, code in [('negRisk', 'negrisk'), ('Ladder', 'ladder')]
    ]), [.31, .16, .20, .33])

    feature_rows = [
        ('Active baskets', 'Number of audited baskets whose validity intervals include the scan.', 'Count'),
        ('Active tokens', 'Number of distinct token books needed by those active baskets; shared legs are counted once.', 'Count'),
        ('Seen-member fraction', 'Previously observed active tokens divided by all active tokens; always one among eligible scans.', '0 to 1'),
        ('Clean-member fraction', 'Active tokens with nonempty two-sided, non-crossed books divided by all active tokens.', '0 to 1'),
        ('Mean spread', 'Mean best ask minus best bid across clean active member books; missing if none are clean.', 'Dollars/share'),
        ('Maximum spread', 'Largest best-ask-minus-best-bid difference across clean active members.', 'Dollars/share'),
        ('Total ask depth', 'Sum of displayed ask quantities at all recorded levels across distinct active tokens.', 'Shares'),
        ('Maximum book age', 'Largest elapsed source-time age among the latest observed active member snapshots.', 'Seconds'),
        ('Best quoted margin', 'Maximum guaranteed payoff minus depth-integrated ask cost across supported basket quantities; negative values are retained.', 'Dollars'),
        ('Size-feasible fraction', 'Active baskets with a finite, size- and tick-compatible quoted margin divided by all active baskets.', '0 to 1'),
        ('Positive-margin fraction', 'Active baskets whose best quoted margin exceeds zero divided by all active baskets; missing margins do not count as positive.', '0 to 1')]
    add('features', 'Current book features before adding hourly lags and changes',
        pd.DataFrame(feature_rows, columns=['Feature', 'Definition at scan time', 'Unit']), [.25, .59, .16])

    fixed = [
        ('Logistic classifier', 'L2 penalty; C=1; balanced class weights; lbfgs solver; maximum 2,000 iterations.'),
        ('Forest classifier', '200 trees; maximum depth 10; minimum leaf size 20; square-root feature subsampling; bootstrap sampling; balanced_subsample class weights.'),
        ('XGBoost classifier', '200 trees; depth 3; learning rate 0.05; minimum child weight 10; row and column subsampling 1; L2 penalty 10; L1 penalty 0; histogram tree construction; negative/positive training-count class-weight ratio.'),
        ('Two-stage forest', '200 trees per stage; depth 8; minimum leaf size 20; all features considered at each split. The probability stage has no class weighting; the amount stage uses squared-error regression on positive values.')]
    add('fixed_settings', 'Fixed classical model settings',
        pd.DataFrame(fixed, columns=['Model', 'Settings']), [.25, .75])

    search = pd.read_csv(root / 'artifacts/tables/tuning_search.csv')
    windows = search.groupby(['fitting_cutoff', 'inner'], sort=True).first().reset_index()
    validation = []
    for r in windows.itertuples():
        validation.append({'Tuning date': pd.Timestamp(r.fitting_cutoff).strftime('%d %b %Y'),
                           'Block': r.inner + 1,
                           'Validation interval (UTC)': pd.Timestamp(r.validation_start).strftime('%d %b %Y %H:%M') + ' to ' + pd.Timestamp(r.validation_end).strftime('%d %b %Y %H:%M'),
                           'Train rows': f'{r.training_rows:,}'})
    add('validation_windows', 'Exact chronological validation blocks; endpoints are exclusive',
        pd.DataFrame(validation), [.22, .10, .49, .19])

    operations = [
        ('1', 'At each scan, identify valid active baskets and require all active members to have been seen. Keep candidates regardless of later label availability.'),
        ('2', 'Read each token at its latest source timestamp at or before the scan; construct current features, consecutive-hour lags and changes.'),
        ('3', 'Offline, label the following hour only when every active interval is decidable. Record occurrence and the maximum qualifying one-basket margin; preserve unknown targets.'),
        ('4', 'At a weekly origin, fit preprocessing and models on known examples with label-end time strictly before the origin. Retune only at declared origins using earlier inner blocks.'),
        ('5', 'Score every eligible event in the evaluation block at its hourly scan. Select up to N by descending score, resolving ties by event identifier.'),
        ('6', 'Evaluate selected rows against their known labels. Separately replay selected monitoring windows under fixed arrival and unwind assumptions.')]
    add('workflow', 'Chronological feature, label, fitting and selection procedure',
        pd.DataFrame(operations, columns=['Step', 'Operation']), [.10, .90])

    p = pd.read_csv(extension / 'tables/budget_curves.csv')
    main = p.loc[p.budget.eq(3)].set_index('model')
    resources = []
    for model in ['fixed_book_random_forest', 'quoted_gap_rule', 'freshness_rule', 'depth_rule']:
        r = main.loc[model]
        resources.append({'Policy': LABELS[model], 'Event share (%)': f'{100*r.selected/r.candidates:.2f}',
                          'Token share (%)': f'{100*r.selected_token_snapshots/r.all_token_snapshots:.2f}',
                          'Unknown slots (%)': f'{100*r.selected_unknown_fraction:.2f}'})
    add('monitoring_burden', 'Monitoring burden at N=3; shares of all eligible event-hours and token counts',
        pd.DataFrame(resources), [.34, .22, .22, .22])

    structure = pd.read_csv(extension / 'tables/capture_by_structure.csv')
    rows = []
    for kind in ['negrisk', 'ladder']:
        subset = structure.loc[structure.stratum.eq(kind)]
        base = subset.loc[subset.model.eq('fixed_book_random_forest')].iloc[0]
        for model in ['fixed_book_random_forest', 'quoted_gap_rule', 'freshness_rule']:
            r = subset.loc[subset.model.eq(model)].iloc[0]
            rows.append({'Structure / policy': ('negRisk' if kind == 'negrisk' else 'Ladder') + ': ' + LABELS[model],
                         'Known positives': int(base.positive), 'Captured': int(r.captured),
                         'Recall (%)': f'{100*r.recall:.2f}', 'Value (%)': f'{100*r.value_capture:.2f}'})
    add('structure_results', 'Capture by basket type within the shared N=3 allocation',
        pd.DataFrame(rows), [.40, .16, .14, .15, .15])

    exclusion = pd.read_csv(extension / 'tables/leave_one_event_out_reranking.csv')
    exclusion = exclusion.loc[exclusion.excluded_event.eq('fed-decision-in-december')]
    add('exclusion_capture', 'Opportunity capture after removing December Fed and reranking at N=3',
        pd.DataFrame([{'Policy': LABELS[r.model], 'Positive hours captured': int(r.captured_positive_windows),
                       'Recall (%)': f'{100*r.recall:.2f}', 'Value capture (%)': f'{100*r.peak_value_recall:.2f}'}
                      for r in exclusion.itertuples()]), [.40, .22, .18, .20])

    intervals = pd.read_csv(extension / 'tables/paired_strong_controls.csv')
    add('strong_controls', 'Forest-minus-control capture differences and conditional 95% block intervals',
        pd.DataFrame([{'Comparator': LABELS[r.comparator],
                       'Recall difference (pp)': f'{100*r.recall_difference:.2f}',
                       '95% interval (pp)': f'[{100*r.recall_lower95:.2f}, {100*r.recall_upper95:.2f}]',
                       'Value difference (pp)': f'{100*r.value_capture_difference:.2f}',
                       'Value 95% interval (pp)': f'[{100*r.value_capture_lower95:.2f}, {100*r.value_capture_upper95:.2f}]'}
                      for r in intervals.itertuples()]), [.26, .17, .20, .17, .20])

    ab = pd.read_csv(extension / 'tables/ablation_metrics.csv')
    ab = ab.loc[ab.budget.eq(3)]
    names = dict(full_book='All 33 book features', current_book='Current features only',
                 quoted_gap_only='Quoted margin only', without_quoted_margin='Without quoted margin',
                 without_book_age='Without book age', current_gap_and_age='Current margin and age',
                 quoted_gap_rule='Quoted-gap rule', freshness_rule='Freshness rule', depth_rule='Depth rule')
    order = list(names)
    rows = []
    for view in order:
        part = ab.loc[ab.view.eq(view)]
        if part.empty:
            continue
        def interval(column):
            return f'{100*part[column].mean():.2f}' if len(part) == 1 else f'{100*part[column].mean():.2f} [{100*part[column].min():.2f}, {100*part[column].max():.2f}]'
        rows.append({'Feature view / rule': names[view], 'Features': int(part.features.iloc[0]),
                     'Recall: mean [range] (%)': interval('recall'),
                     'Value: mean [range] (%)': interval('peak_value_recall')})
    add('ablations', 'Earlier-period ablations at N=3; three-seed means and observed seed ranges',
        pd.DataFrame(rows), [.37, .12, .255, .255])

    study = json.loads((root / 'inputs/study.json').read_text())
    for family, title in [('logistic', 'Logistic regression'), ('random_forest', 'Random forest'),
                          ('xgboost', 'XGBoost'), ('tabm_two_stage', 'Two-stage TabM')]:
        rows = []
        for index, settings in enumerate(study['candidates'][family]):
            text = '; '.join(f'{key}={value}' for key, value in settings.items())
            rows.append({'ID': index, 'Configuration': text})
        add('grid_' + family, title + ' candidate configurations; zero-based IDs',
            pd.DataFrame(rows), [.08, .92])

    selected = pd.read_csv(root / 'artifacts/tables/selected_hyperparameters.csv')
    rows = []
    for r in selected.itertuples():
        epochs = '' if pd.isna(r.epochs) else str(r.epochs)
        rows.append({'Origin': r.origin, 'Family': r.method.replace('_', ' '),
                     'ID': r.candidate, 'Selected refit epochs': epochs or 'Not applicable'})
    add('selected_settings', 'Selected configurations at the three tuning origins',
        pd.DataFrame(rows), [.19, .28, .08, .45])
    (extension / 'report_tables.json').write_text(json.dumps(entries, indent=2) + '\n', encoding='utf-8')
    (extension / 'table_widths.json').write_text(json.dumps(widths, indent=2) + '\n', encoding='utf-8')
    return entries
