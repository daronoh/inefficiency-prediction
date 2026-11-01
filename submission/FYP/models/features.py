from __future__ import annotations
def topic(slug):
    """Broad subject only; unmatched names share 'other', never an event ID."""
    for name, terms in [('tweet_counts', ['elon-musk-of-tweets']), ('central_bank', ['fed-decision']), ('shutdown', ['government-shutdown']), ('crypto', ['bitcoin', 'ethereum', 'monad', 'gensyn', 'lighter-airdrop']), ('elections', ['election']), ('sports', ['australian-open', 'boxing-', 'f1-', 'world-series']), ('geopolitics', ['strikes', 'leader-out', 'trump-and-putin']), ('technology_business', ['largest-company', 'best-ai-model']), ('culture', ['google-this-year', 'avatar-', 'game-awards', 'person-of-the-year', 'spotify'])]:
        if any((term in slug for term in terms)):
            return name
    return 'other'

def family(slug):
    """Assign stable broad families without using observed targets."""
    value = topic(slug)
    return slug if value == 'other' else value
