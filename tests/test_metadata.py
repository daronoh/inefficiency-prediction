"""Point-in-time metadata restrictions and training-only category handling."""
import unittest
import numpy as np
import pandas as pd
from src.data.metadata import add_metadata, METADATA_COLUMNS
from src.models.features import topic, family
from src.models.estimators import estimator, predict


class MetadataTests(unittest.TestCase):
    def test_future_creation_is_missing_and_future_members_do_not_change_age(self):
        frame = pd.DataFrame({'event_slug': ['fed-decision-in-october'] * 2,
            'stratum': ['negrisk'] * 2, 'time': pd.to_datetime(['2025-10-01', '2025-11-01'], utc=True)})
        universe = pd.DataFrame({'event_slug': ['fed-decision-in-october'] * 2, 'market_id': ['1', '2']})
        markets = pd.DataFrame({'market_id': ['1', '2'], 'created_at': ['2025-10-02', '2026-01-01'],
                                'volume': [999., 1000.], 'closed_time': ['2026-01-01'] * 2})
        result = add_metadata(frame, universe, markets)
        self.assertTrue(np.isnan(result.iloc[0].m_age_days))
        self.assertEqual(result.iloc[1].m_age_days, 30.)
        self.assertEqual(set(result.columns) - set(frame.columns), set(METADATA_COLUMNS))
        changed = markets.assign(volume=0., closed_time='2027-01-01')
        pd.testing.assert_frame_equal(result, add_metadata(frame, universe, changed))
        pd.testing.assert_frame_equal(result, add_metadata(frame, universe.iloc[:1], markets.iloc[:1]))

    def test_unmatched_topic_does_not_encode_event_identity(self):
        self.assertEqual(topic('unseen-event-a'), 'other')
        self.assertEqual(topic('unseen-event-b'), 'other')
        self.assertEqual(family('unseen-event-a'), 'unseen-event-a')

    def test_unknown_categories_do_not_enter_fitted_encoder(self):
        train = pd.DataFrame({'m_age_days': [1., 2., 3., np.nan],
                              'm_topic': ['crypto'] * 4, 'm_event_type': ['ladder'] * 4})
        model = estimator('logistic', [0, 1, 0, 1], numeric=['m_age_days'],
                          categorical=['m_topic', 'm_event_type']).fit(train, [0, 1, 0, 1])
        categories = model[0].named_transformers_['categorical'].categories_
        test = train.iloc[:1].assign(m_topic='politics', m_event_type='negrisk', m_age_days=1000.)
        self.assertTrue(np.isfinite(predict(model, test)).all())
        self.assertEqual(categories[0].tolist(), ['crypto'])
        self.assertEqual(categories[1].tolist(), ['ladder'])
        self.assertEqual(model[0].named_transformers_['numeric'][0].statistics_[0], 2.)

    def test_single_class_metadata_model_still_has_constant_fallback(self):
        train = pd.DataFrame({'m_age_days': [np.nan] * 3, 'm_topic': ['other'] * 3})
        model = estimator('random_forest', [0, 0, 0], numeric=['m_age_days'],
                          categorical=['m_topic']).fit(train, [0, 0, 0])
        np.testing.assert_equal(predict(model, train), [0., 0., 0.])
