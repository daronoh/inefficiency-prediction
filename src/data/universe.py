"""Frozen event membership and audited trading packages; no return-based selection."""
import pandas as pd
from src.config import SPEC
from src.paths import EVENT_UNIVERSE, ORDERBOOK_DIR, book_path
from src.data.rules import build_manifest

def build_plan(scope, ladder_scope='audited5'):
    """Membership is frozen retrospectively; never inferred from outcomes as a feature."""
    from src.paths import TOKENS_PARQUET
    universe = pd.read_parquet(EVENT_UNIVERSE)
    plans, excluded = [], []
    if scope in {'all', 'negrisk'}:
        tokens = pd.read_parquet(TOKENS_PARQUET)
        tokens.market_id = tokens.market_id.astype(str)
        yes = tokens.loc[tokens.outcome.str.upper().eq('YES')].set_index('market_id').token_id
        for slug, markets in universe.loc[universe.stratum.eq('negrisk')].groupby('event_slug'):
            mids = markets.market_id.astype(str).tolist()
            ids = [str(yes.loc[mid]) for mid in mids]
            paths = {t: ORDERBOOK_DIR / f'ob_{t}.parquet' for t in ids}
            reason = 'incomplete_frozen_membership' if not markets.supports_full_cover.all() else 'missing_member_history' if not all(p.exists() for p in paths.values()) else None
            if reason:
                excluded.append(dict(event_slug=slug, stratum='negrisk', reason=reason))
                continue
            plans.append(dict(event_slug=slug, stratum='negrisk', paths=paths,
                packages=[(ids, pd.Timestamp(SPEC.development_start), pd.Timestamp(SPEC.lockbox_start))]))
    if scope in {'all', 'ladders'}:
        pairs, _, rules = build_manifest(ladder_scope)
        for slug, group in pairs.groupby('event_slug'):
            ids = sorted(set(group.broad_yes_token) | set(group.narrow_no_token))
            paths = {t: book_path(t) for t in ids}
            if not all(p.exists() for p in paths.values()):
                excluded.append(dict(event_slug=slug, stratum='ladder', reason='missing_audited_history'))
                continue
            packages = [([p.broad_yes_token, p.narrow_no_token], p.window_start, p.window_end) for p in group.itertuples()]
            plans.append(dict(event_slug=slug, stratum='ladder', paths=paths, packages=packages))
    if scope not in {'all', 'negrisk', 'ladders'}:
        raise ValueError('Unknown scope')
    return plans, excluded
