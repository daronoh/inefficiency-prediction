"""Predeclared topic categories and feature subsets; independent of returns."""

def topic(slug):
    """Broad subject only; unmatched names share 'other', never an event ID."""
    for name, terms in [
        ('tweet_counts', ['elon-musk-of-tweets']),
        ('central_bank', ['fed-decision']),
        ('shutdown', ['government-shutdown']),
        ('crypto', ['bitcoin', 'ethereum', 'monad', 'gensyn', 'lighter-airdrop']),
        ('elections', ['election']),
        ('sports', ['australian-open', 'boxing-', 'f1-', 'world-series']),
        ('geopolitics', ['strikes', 'leader-out', 'trump-and-putin']),
        ('technology_business', ['largest-company', 'best-ai-model']),
        ('culture', ['google-this-year', 'avatar-', 'game-awards', 'person-of-the-year', 'spotify'])]:
        if any(term in slug for term in terms):
            return name
    return 'other'


def family(slug):
    """Preserve the original evaluation groups, including unmatched singleton events."""
    value = topic(slug)
    return slug if value == 'other' else value


def feature_groups(columns):
    return {
        'quoted_gap': [c for c in columns if 'quoted_net' in c],
        'history': [c for c in columns if 'snapshot_lag1_' in c or 'snapshot_change1_' in c],
        'freshness_quality': [c for c in columns if any(k in c for k in ['book_age', 'seen_fraction', 'clean_fraction'])],
        'spread': [c for c in columns if 'spread' in c],
        'depth_size': [c for c in columns if any(k in c for k in ['ask_depth', 'active_', 'size_feasible'])],
    }
