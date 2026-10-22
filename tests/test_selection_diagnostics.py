"""Accounting must distinguish missing observations, missed trades and shared history."""
import unittest
import numpy as np
import pandas as pd
from src.reporting.selection_diagnostics import onset_categories, selection_attribution, compare_attempts

T = pd.Timestamp('2025-11-01', tz='UTC')


class SelectionDiagnosticsTests(unittest.TestCase):
    def test_boundary_and_censored_onsets(self):
        f = pd.DataFrame({'decision_time': [T]*4,
            'original_spell_start': [T-pd.Timedelta(minutes=2), T,
                                     T+pd.Timedelta(minutes=2), T+pd.Timedelta(minutes=3)],
            'start_observed': [False, True, True, False]})
        self.assertEqual(onset_categories(f).tolist(), ['visible_at_scan', 'visible_at_scan',
            'new_observed_after_scan', 'first_seen_after_scan_onset_unknown'])
        f.loc[0, 'original_spell_start'] = pd.NaT
        with self.assertRaises(ValueError):
            onset_categories(f)

    def test_shared_selected_hours_keep_history_difference(self):
        selected = pd.DataFrame({'event_slug': ['shared', 'model'], 'time': [T, T]})
        reference = pd.DataFrame({'event_slug': ['shared', 'rule'], 'time': [T, T]})
        trades = pd.DataFrame({'event_slug': ['shared', 'model', 'shared', 'rule'],
            'decision_time': [T]*4, 'time': [T]*4, 'model': ['m', 'm', 'quoted_gap_rule', 'quoted_gap_rule'],
            'net_usd': [8., -7., 10., 3.], 'unknown_pnl': [False]*4})
        result = selection_attribution(selected, reference, trades, 'm').set_index('selection_group')
        self.assertEqual(result.loc['both', 'difference_usd'], -2.)
        self.assertEqual(result.loc['model_only', 'difference_usd'], -7.)
        self.assertEqual(result.loc['rule_only', 'difference_usd'], -3.)
        self.assertEqual(result.difference_usd.sum(), (8-7)-(10+3))

    def test_missing_trade_is_zero_but_unknown_pnl_is_not(self):
        rows = []
        for model, pair, net in [('m', 'shared', 4.), ('quoted_gap_rule', 'shared', 4.),
                                 ('m', 'loss', -3.), ('quoted_gap_rule', 'unknown', np.nan)]:
            rows.append(dict(model=model, event_slug='e', decision_time=T, pair_id=pair, start_time=T,
                net_usd=net, status='complete', quantity=5, onset='visible_at_scan',
                scan_state='positive_quote', quoted_gross_usd=4., n_filled_legs=2,
                n_members=2, episode_id=pair))
        result = compare_attempts(pd.DataFrame(rows), 'm').set_index('pair_id')
        self.assertEqual(result.loc['shared', 'difference_usd'], 0.)
        self.assertEqual(result.loc['loss', 'difference_usd'], -3.)
        self.assertTrue(pd.isna(result.loc['unknown', 'difference_usd']))
        self.assertEqual(result.loc['unknown', 'net_usd_model'], 0.)
        self.assertTrue(result.loc['unknown', 'unknown_comparison'])

    def test_duplicate_attempt_keys_are_rejected(self):
        row = dict(model='m', event_slug='e', decision_time=T, pair_id='p', start_time=T,
            net_usd=1., status='complete', quantity=5, onset='visible_at_scan',
            scan_state='positive_quote', quoted_gross_usd=1., n_filled_legs=2,
            n_members=2, episode_id='p')
        with self.assertRaises(pd.errors.MergeError):
            compare_attempts(pd.DataFrame([row, row]), 'm')
