"""Presentation tables for the focused sensitivities and local timing study."""
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd


def prepare(root):
    root = Path(root)
    extension = root/'artifacts/extensions/sensitivity_review_v1'
    if not (extension/'complete.json').exists():
        raise ValueError('Run the focused sensitivity experiment first')
    tables, entries, widths = extension/'tables', {}, {}

    def add(key, caption, frame, fractions):
        path = tables/('report_'+key+'.csv')
        frame.to_csv(path,index=False)
        entries[key] = dict(caption=caption,file=path.relative_to(root/'artifacts').as_posix())
        widths[key] = fractions

    literature = [
        ('Saguillo et al. (2025)', 'Identify structural mispricing and reduce relationship-search work.',
         'Future monitoring allocation, historical depth coverage and executable fills require separate evaluation.'),
        ('Trapeznikov and Saligrama (2013)', 'Choose additional measurements under acquisition costs.',
         'Here the choice is which events to monitor next; event and token counts only approximate acquisition cost.'),
        ('Chen, Wang and Yuan (2013)', 'Learn subset choices from the outcomes of played arms.',
         'The historical controls here assume full past feedback; selective-feedback learning and regret are not evaluated.'),
        ('Grinsztajn et al. (2022); Gorishniy et al. (2025)', 'Compare tree and neural inductive biases on tabular tasks.',
         'Temporal availability, rare baskets and budgeted capture must be tested in the present domain.'),
        ('Cawley and Talbot (2010); Bailey et al. (2017)', 'Explain selection bias when choosing models or strategies from finite historical evidence.',
         'Earlier-only fitting protects each prediction; the final policy recommendation still needs independent evaluation.'),
    ]
    add('literature_synthesis','Related research and the methodological requirements for this study',
        pd.DataFrame(literature,columns=['Research','Question addressed','Implication for this study']),[.27,.32,.41])

    history = pd.read_csv(tables/'history_sensitivity.csv')
    rolling = pd.read_csv(tables/'rolling_sensitivity.csv')
    rows = []
    for r in history.loc[history.scope.eq('full_period')].sort_values('smoothing').itertuples():
        rows.append({'Setting / population':f'Historical mean, prior {r.smoothing:g} / 359 positives',
                     'Recall (%)':f'{100*r.recall:.2f}',
                     'Value (%)':f'{100*r.value_capture:.2f}',
                     'Event value (%)':f'{100*r.event_value_capture:.2f}'})
    for setting, label in [('expanding','Expanding'),('28d','28-day'),('14d','14-day')]:
        part = rolling.loc[rolling.window.eq(setting)]
        def fmt(column):
            values = 100*part[column]
            return f'{values.mean():.2f} [{values.min():.2f}, {values.max():.2f}]'
        rows.append({'Setting / population':f'{label} forest / 343 positives',
                     'Recall (%)':fmt('recall'),'Value (%)':fmt('value_capture'),
                     'Event value (%)':fmt('event_value_capture')})
    add('design_sensitivity','Declared design sensitivities at N=3: historical controls use the full period; forest fits use the earlier feature period. Forest entries are three-seed means [minimum, maximum]; ranges are not confidence intervals.',
        pd.DataFrame(rows),[.34,.22,.22,.22])

    timing = pd.read_csv(tables/'timing_summary.csv').set_index(['model','mode'])
    labels = [('fixed_book_random_forest','saved_4_jobs','Forest, 4 jobs'),
              ('fixed_book_random_forest','single_job','Forest, 1 job'),
              ('fixed_book_logistic','saved','Logistic'),('quoted_gap_rule','saved','Quoted gap')]
    rows = []
    for model,mode,label in labels:
        r = timing.loc[(model,mode)]
        rows.append({'Policy':label,'Median (ms)':f'{r.median_total_us/1000:.3f}',
                     '95th percentile (ms)':f'{r.p95_total_us/1000:.3f}',
                     'Avoided tokens / scan':f'{r.mean_avoided_token_observations:.2f}',
                     'Overhead / token (µs)':f'{r.overhead_us_per_avoided_token:.2f}'})
    add('scoring_overhead','Warm cached-feature scoring and N=3 selection: 12 scans, 30 measured calls per scan and policy. The final column divides total measured overhead by avoided event-token observations; scan and feature-construction costs are excluded.',
        pd.DataFrame(rows),[.28,.16,.20,.18,.18])
    (extension/'report_tables.json').write_text(json.dumps(entries,indent=2)+'\n',encoding='utf-8')
    (extension/'table_widths.json').write_text(json.dumps(widths,indent=2)+'\n',encoding='utf-8')
