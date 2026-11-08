"""Reader aids: one fictional example and a map of saved evaluation populations.

These tables explain the existing study. They do not create predictions, labels
or empirical comparisons. The worked example is explicitly illustrative.
"""
from __future__ import annotations

import pandas as pd

TABLE_WIDTHS = {
    'reading_example': [.20, .18, .36, .26],
    'reading_populations': [.25, .33, .24, .18],
}


def tables(artifacts):
    example = pd.DataFrame([
        ['A (1st)', 'Yes', 'Known positive', '10.00'],
        ['B (2nd)', 'Yes', 'Known negative', '0.00'],
        ['C (3rd)', 'Yes', 'Unknown', 'Unknown'],
        ['D (4th)', 'No', 'Known positive', '2.00'],
        ['E (5th)', 'No', 'Known negative', '0.00'],
    ], columns=['Event (rank)', 'Monitored?', 'Following-hour label', 'Peak margin ($)'])
    main = pd.read_csv(artifacts/'tables/population.csv').iloc[0]
    ablations = pd.read_csv(artifacts/'extensions/processed_review_v1/tables/ablation_metrics.csv')
    earlier = ablations.loc[ablations.view.eq('full_book') & ablations.seed.eq(20261001)
                            & ablations.budget.eq(3)].iloc[0]
    choices = pd.read_csv(artifacts/'extensions/allocation_review_v1/tables/selector_metrics.csv')
    later = choices.loc[choices.model.eq('earlier_only_selector')].iloc[0]
    populations = pd.DataFrame([
        ['Main evaluation', '11 Nov 2025 to 3 Feb 2026, 08:00',
         f'{int(main.known):,} / {int(main.event_hours):,}', f'{int(main.positive):,}'],
        ['Earlier evaluation', '11 Nov 2025 to 8 Jan 2026, 00:00',
         f'{int(earlier.known_candidates):,} / {int(earlier.candidates):,}', f'{int(earlier.positive_windows):,}'],
        ['After warm-up', '9 Dec 2025 to 3 Feb 2026, 08:00',
         f'{int(later.known):,} / {int(later.rows):,}', f'{int(later.positive):,}'],
    ], columns=['Analysis period', 'Dates (UTC; end excluded)', 'Known / eligible event-hours', 'Positive hours'])
    return {
        'reading_example': ('One fictional hourly allocation at N=3; all numbers illustrate the definitions and are not study results', example),
        'reading_populations': ('The three evaluation periods used in the report; the periods overlap and their row counts must not be added', populations),
    }
