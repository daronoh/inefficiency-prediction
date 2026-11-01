"""Verify frozen basket routes against supplied contract and token evidence."""
import hashlib
import pandas as pd
from FYP.paths import PROJECT_ROOT


def verify_metadata(plans):
    root=PROJECT_ROOT/'inputs'
    tokens=pd.read_csv(root/'tokens.csv',dtype=str).set_index('token_id')
    events=pd.read_parquet(root/'events.parquet')
    contracts=pd.read_parquet(root/'contracts.parquet');contracts.market_id=contracts.market_id.astype(str)
    contracts=contracts.set_index('market_id')
    pairs=pd.read_csv(root/'ladder_pairs.csv',dtype={'broad_yes_token':str,'narrow_no_token':str})
    valid={(r.event_slug,r.broad_yes_token,r.narrow_no_token,pd.Timestamp(r.window_start),pd.Timestamp(r.window_end)) for r in pairs.itertuples()}
    audit=pd.read_csv(root/'ladder_contract_audit.csv',dtype={'market_id':str})
    for row in audit.itertuples():
        text=contracts.loc[row.market_id,'description']
        if hashlib.sha256(text.encode()).hexdigest()!=row.description_sha256:
            raise ValueError('Audited contract text changed')
    for plan in plans:
        for ids,a,b in plan['packages']:
            if not set(ids).issubset(tokens.index):raise ValueError('Missing token mapping')
            if plan['stratum']=='negrisk':
                expected=set(events.loc[events.event_slug.eq(plan['event_slug']),'market_id'].astype(str))
                if set(tokens.loc[ids,'market_id'])!=expected or not tokens.loc[ids,'outcome'].str.upper().eq('YES').all():
                    raise ValueError('Incomplete YES basket')
            elif (plan['event_slug'],ids[0],ids[1],a,b) not in valid:
                raise ValueError('Ladder route or validity differs from frozen audit')
    return dict(token_mappings=len(tokens),ladder_contracts=len(audit),ladder_pairs=len(pairs),events=len(plans),frozen_metadata_verified=True)
