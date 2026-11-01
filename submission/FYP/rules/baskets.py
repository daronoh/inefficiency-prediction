"""Structural contracts supported by the frozen study, independent of prices."""
from __future__ import annotations


def validate_plan(plan):
    if plan['stratum'] not in {'negrisk','ladder'}:
        raise ValueError('Unsupported structural relationship')
    if not plan['packages']:
        raise ValueError('Event needs a validated basket')
    for tokens,start,end in plan['packages']:
        if len(tokens)<2 or len(set(tokens))!=len(tokens):
            raise ValueError('Basket requires distinct complete member tokens')
        if plan['stratum']=='ladder' and len(tokens)!=2:
            raise ValueError('Ladder basket is broader YES plus narrower NO')
        if not set(tokens).issubset(plan['paths']) or start>=end:
            raise ValueError('Missing leg or invalid contract interval')
    return plan


def ladder_payoff(broad, narrow):
    """A narrower outcome implies the broader one; payoff is at least $1."""
    if narrow and not broad:
        raise ValueError('Outcome violates the audited implication')
    return int(bool(broad))+int(not bool(narrow))


def complete_set_payoff(outcomes):
    """Exactly one member of an audited exhaustive set resolves YES."""
    if len(outcomes)<2 or sum(bool(x) for x in outcomes)!=1:
        raise ValueError('Not a valid mutually exclusive exhaustive outcome')
    return 1
