"""Restricted event context; no final status, volume, outcomes or closing dates."""
import pandas as pd
from src.models.features import topic
from src.paths import EVENT_UNIVERSE, MARKETS_PARQUET

METADATA_NUMERIC = ['m_age_days']
METADATA_CATEGORICAL = ['m_event_type', 'm_topic']
METADATA_COLUMNS = METADATA_NUMERIC + METADATA_CATEGORICAL


def add_metadata(frame, universe=None, markets=None):
    """Age is since first frozen member creation, not a reconstructed listing date.

    Creation timestamps are treated as stable facts. The frozen membership and
    text-derived topic classification share the cohort's retrospective limitation.
    Missing or future creation times stay missing; they never filter candidates.
    """
    if universe is None:
        universe = pd.read_parquet(EVENT_UNIVERSE, columns=['event_slug', 'market_id'])
    if markets is None:
        markets = pd.read_parquet(MARKETS_PARQUET, columns=['market_id', 'created_at'])
    members = universe.loc[universe.event_slug.isin(frame.event_slug.unique()),
                           ['event_slug', 'market_id']].copy()
    dates = markets[['market_id', 'created_at']].copy()
    members['market_id'] = members.market_id.astype(str)
    dates['market_id'] = dates.market_id.astype(str)
    dates['created_at'] = pd.to_datetime(dates.created_at, utc=True, format='mixed', errors='coerce')
    dates = members.merge(dates, on='market_id', how='left', validate='many_to_one')
    first = dates.groupby('event_slug').created_at.min()
    result = frame.copy()
    result['m_event_type'] = result.stratum.where(result.stratum.isin(['negrisk', 'ladder']), 'other')
    result['m_topic'] = result.event_slug.map(topic)
    age = (result.time - result.event_slug.map(first)).dt.total_seconds() / 86400.
    result['m_age_days'] = age.where(age.ge(0))
    return result
