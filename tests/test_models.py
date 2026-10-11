import unittest
import numpy as np
import pandas as pd
from src.models.evaluation import masks, ranked, ranking_metrics
from src.models.features import family
from src.models.estimators import estimator, predict
from src.data.labels import summarize_window
from src.data.books import RawGroup
from src.simulator.scheduler import run_event
from src.simulator.execution import experiment
from src.config import for_window
from fixtures import BASE, _frame


class ResearchTests(unittest.TestCase):
    def test_unknown_high_score_still_consumes_monitoring_slot(self):
        frame = pd.DataFrame({'time': [BASE] * 2, 'event_slug': ['unknown', 'positive'],
            'label_known': [False, True], 'target': [np.nan, 1.], 'future_positive': [False, True],
            'x_active_tokens': [3, 2]})
        selected = ranked(frame, [.9, .1], 1)
        self.assertEqual(selected.event_slug.tolist(), ['unknown'])
        metric = ranking_metrics(frame, [.9, .1], 1)
        self.assertEqual(metric['recall'], 0.)
        self.assertEqual(metric['selected_unknown'], 1)


    def test_purge_includes_horizon_and_unwind_and_excludes_unknown_training(self):
        start = pd.Timestamp('2025-11-01', tz='UTC')
        frame = pd.DataFrame({'time': [start - pd.Timedelta(hours=2), start - pd.Timedelta(hours=1), start],
                              'label_known': [True, True, True]})
        train, test = masks(frame, '2025-11-01', '2025-11-02', for_window(60))
        self.assertEqual(train.tolist(), [True, False, False])
        self.assertEqual(test.tolist(), [False, False, True])


    def test_family_mapping_groups_repeated_events(self):
        self.assertEqual(family('fed-decision-in-december'), family('fed-decision-in-october'))
        self.assertEqual(family('when-will-the-government-shutdown-end-545'), family('will-the-government-shutdown-end-by-763'))


    def test_single_class_training_has_honest_constant_fallback(self):
        x = pd.DataFrame({'x_a': [1., np.nan, 3.], 'x_empty': [np.nan] * 3})
        model = estimator('logistic', [0, 0, 0]).fit(x, [0, 0, 0])
        np.testing.assert_equal(predict(model, x), [0., 0., 0.])


    def test_shortlist_must_be_replayed_not_filtered_from_monitor_all(self):
        raw = _frame([(0, [(.4, 10)], [(.39, 10)]),
                      (3600, [(.4, 10)], [(.39, 10)]),
                      (7200, [(.4, 10)], [(.39, 10)])])
        group = RawGroup([raw, raw])
        rows = []
        for offset in (0, 3600):
            when = BASE + pd.Timedelta(seconds=offset)
            rows.append(dict(event_slug='event', pair_id='p', window='development', time=when,
                start_time=when, end_time=when + pd.Timedelta(hours=1),
                monitoring_end_time=when + pd.Timedelta(hours=1)))
        signals = pd.DataFrame(rows)
        all_trades, _ = run_event(signals, {'p': group}, {'p': ('a', 'b')}, experiment(for_window(60)))
        selected_trades, _ = run_event(signals.iloc[1:], {'p': group}, {'p': ('a', 'b')}, experiment(for_window(60)))
        self.assertEqual(len(all_trades.loc[all_trades.decision_time.eq(signals.iloc[1].time)]), 0)
        self.assertEqual(len(selected_trades), 1)
        self.assertGreater(selected_trades.iloc[0].pnl_usd, 0)


    def test_first_observation_is_not_proof_of_a_new_gap(self):
        when = BASE + pd.Timedelta(minutes=5)
        detail = pd.DataFrame([dict(attempted=True, pnl_usd=1., pair_id='p', start_time=when,
            decision_time=BASE, status='complete', entry_cost_usd=9.)])
        signals = pd.DataFrame([dict(pair_id='p', start_time=when, original_spell_start=when,
            episode_id='e', start_observed=False)])
        stats = summarize_window(detail, signals, for_window(60))
        self.assertTrue(stats['positive'])
        self.assertFalse(stats['future_positive'])


