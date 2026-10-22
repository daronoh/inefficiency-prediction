"""Versioned, conservative contract grammars for monotone binary outcomes.

No event IDs, outcome prices or realised resolutions enter these parsers.
Unrecognised wording is not evidence of an implication.
"""
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import re
import unicodedata
import pandas as pd

VERSION = 'ladder_templates_v1'
AMOUNT = r'\$[\d,]+(?:\.\d+)?\s*[KMB]?'
MONTH = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'


def normalise(text):
    return ' '.join(unicodedata.normalize('NFKC', str(text)).translate(
        str.maketrans({'“':'"', '”':'"', '’':"'"})).split())


def amount(text):
    value = re.fullmatch(r'\$([\d,]+(?:\.\d+)?)\s*([KMB]?)', text)
    if value is None:
        raise ValueError('unsupported_amount')
    if ',' in value[1] and not re.fullmatch(r'\d{1,3}(?:,\d{3})+(?:\.\d+)?', value[1]):
        raise ValueError('invalid_thousands_separator')
    result = Decimal(value[1].replace(',', '')) * {'':1, 'K':1000, 'M':1000000, 'B':1000000000}[value[2]]
    if result <= 0:
        raise ValueError('nonpositive_threshold')
    return result


def fingerprint(*values):
    return hashlib.sha256('\x1f'.join(map(str, values)).encode()).hexdigest()


def explicit_year(row, text):
    """Never infer the year from a closing date or a realised outcome."""
    years = set(re.findall(r'(?<!\d)(20\d{2})(?!\d)', text))
    supplied = row.get('contract_year')
    if supplied is not None and not pd.isna(supplied):
        numeric = float(supplied)
        if not numeric.is_integer() or not 2000 <= numeric <= 2099:
            raise ValueError('invalid_contract_year')
        years.add(str(int(numeric)))
    if len(years) != 1:
        raise ValueError('missing_or_ambiguous_contract_year')
    return int(next(iter(years)))


@dataclass(frozen=True)
class Contract:
    template: str
    kind: str
    threshold: Decimal
    direction: str  # upper: lower threshold is broader; lower/deadline: higher is broader
    signature: str
    start: pd.Timestamp | None = None
    end: pd.Timestamp | None = None


def parse_contract(row):
    """Return an accepted grammar or raise a review reason. Full rules must match."""
    q, d = normalise(row.get('question', '')), normalise(row.get('description', ''))
    if not q or not d or d in {'None', 'nan', '<NA>'}:
        raise ValueError('missing_question_or_description')
    source = normalise(row.get('resolution_source') or '')

    # Threshold-independent title reference; exchange, pair, period and full
    # fallback wording remain in the signature. Reach and dip never mix.
    title = re.fullmatch(rf'Will (.+?) (reach|dip to) ({AMOUNT}) in ({MONTH})(?: (20\d{{2}}))?\?', q)
    if title:
        subject, verb, value, month, _ = title.groups()
        metric, comparator = ('High', 'greater') if verb == 'reach' else ('Low', 'lower')
        rule = re.fullmatch(
            rf'This market will immediately resolve to "Yes" if any (.+?) 1 minute candle for ([A-Z0-9]+/[A-Z0-9]+) '
            rf'during the month specified in the title \(from 00:00 AM ET on the first day to 11:59 PM ET on the last\), '
            rf'has a final {metric} price equal to or {comparator} than the price specified in the title\. '
            r'Otherwise, this market will resolve to "No\." '
            rf'The resolution source for this market is \1, specifically the \2 {metric} prices available at (https?://\S+), '
            r'with the chart settings on "1m" for one-minute candles selected on the top bar\. '
            r'Please note that the outcome of this market depends solely on the price data from the \1 \2 trading pair\. '
            r'Prices from other exchanges, different trading pairs, or spot markets will not be considered for the resolution of this market\.', d)
        if rule is None:
            raise ValueError('unsupported_reach_dip_resolution_rules')
        year = explicit_year(row, q + ' ' + str(row.get('event_slug', '')))
        start = pd.Timestamp(f'{month} 1, {year}').tz_localize('America/New_York')
        end = start + pd.offsets.MonthBegin(1)
        return Contract('monthly_candle_v1', 'reach' if verb == 'reach' else 'dip', amount(value),
            'upper' if verb == 'reach' else 'lower', fingerprint(subject, month, year, d, source),
            start.tz_convert('UTC'), end.tz_convert('UTC'))

    title = re.fullmatch(rf'(.+?) market cap \(FDV\) >({AMOUNT}) one day after launch\?', q)
    if title:
        subject, value = title.groups()
        rule = re.fullmatch(
            rf'This market will resolve to "Yes" if the Fully Diluted Valuation of {re.escape(subject)}\'s token '
            rf'is greater than ({AMOUNT}) 1 day after launch\. Otherwise, the market will resolve to "No\." '
            r'For the purposes of this market "locked" tokens or non-swappable tokens will not be considered a launch\. '
            r'"1 day after launch" is defined as 24 hours after launch\. '
            r'The resolution source for this market is the most liquid price source available\. '
            rf'If {re.escape(subject)} doesn\'t launch a token by ({MONTH} \d{{1,2}}, 20\d{{2}}), 11:59 PM ET, '
            r'this market will resolve to "No\."', d)
        if rule is None:
            raise ValueError('unsupported_fdv_resolution_rules')
        if amount(rule[1]) != amount(value):
            raise ValueError('title_description_threshold_mismatch')
        # Only substitute the captured payout threshold, never every number.
        canonical = d[:rule.start(1)] + '{threshold}' + d[rule.end(1):]
        # The fallback is a launch deadline, NOT the latest valuation time.
        return Contract('launch_fdv_v1', 'threshold', amount(value), 'upper', fingerprint(subject, canonical, source))

    title = re.fullmatch(rf'Over ({AMOUNT}) committed to the (.+?) public sale\?', q)
    if title:
        value, subject = title.groups()
        rule = re.fullmatch(
            rf'This market will resolve to "Yes" if total commitments for the {re.escape(subject)} raise on (.+?) '
            r'exceeds the number specified in the title before the raise closes\. Otherwise, it will resolve to "No\." '
            r'The resolution source is the official raise page available at (https?://\S+) '
            rf'If the final commitment amount cannot be verified by ({MONTH} \d{{1,2}}, 20\d{{2}}), 11:59 PM ET, '
            r'or if relevant information becomes permanently unavailable within the market timeframe, '
            r'this market will resolve to "No\."', d)
        if rule is None:
            raise ValueError('unsupported_commitment_resolution_rules')
        end = (pd.Timestamp(rule[3]) + pd.Timedelta(days=1)).tz_localize('America/New_York').tz_convert('UTC')
        return Contract('sale_commitments_v1', 'threshold', amount(value), 'upper', fingerprint(subject, d, source), end=end)

    title = re.fullmatch(rf'Will the Government shutdown end by ({MONTH}) (\d{{1,2}})(?:,? (20\d{{2}}))?\?', q)
    if title:
        expected = ('This market will resolve to "Yes" if the Office of Personnel Management (OPM) announces that '
            'the U.S. federal government is not shut down due to a lapse in appropriations by the listed date (ET). '
            'Otherwise, this market will resolve to "No". Partial shutdowns count as shutdowns; announcements of '
            'office closures due to holidays or inclement weather do not qualify as a shutdown. '
            "The resolution source for this market will be OPM's Operating Status page "
            '(https://www.opm.gov/policy-data-oversight/snow-dismissal-procedures/current-status/).')
        if d != expected:
            raise ValueError('unsupported_shutdown_resolution_rules')
        year = explicit_year(row, q)
        end = (pd.Timestamp(f'{title[1]} {title[2]}, {year}') + pd.Timedelta(days=1)).tz_localize('America/New_York').tz_convert('UTC')
        return Contract('opm_deadline_v1', 'deadline', Decimal(end.value), 'lower', fingerprint(d, source), end=end)

    title = re.fullmatch(r'Will (.+?) occur by (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\?', q)
    if title:
        subject, when = title.groups()
        expected = f'This market will resolve to "Yes" if {subject} occurs by {when}. Otherwise, this market will resolve to "No". Resolution source: '
        if not d.startswith(expected) or not re.fullmatch(r'https?://\S+', d[len(expected):]):
            raise ValueError('unsupported_occurrence_deadline_rules')
        end = pd.Timestamp(when)
        return Contract('occurrence_deadline_v1','deadline',Decimal(end.value),'lower',
            fingerprint(expected.replace(when,'{deadline}')+d[len(expected):],source),end=end)

    # A minimal generic grammar for contracts supplied by other data adapters.
    # Only a single measurement, explicit fixed observation time and source;
    # extra qualifications are rejected rather than silently discarded.
    title = re.fullmatch(rf'Will (.+?) be (above|below|at least|at most) ({AMOUNT}) on (\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}Z)\?', q)
    if title:
        subject, comparison, value, when = title.groups()
        expected = f'This market will resolve to "Yes" if {subject} is {comparison} {value} on {when}. Otherwise, this market will resolve to "No". Resolution source: '
        if not d.startswith(expected) or not re.fullmatch(r'https?://\S+', d[len(expected):]):
            raise ValueError('unsupported_fixed_measurement_resolution_rules')
        canonical = expected.replace(value, '{threshold}') + d[len(expected):]
        return Contract('fixed_measurement_v1', 'threshold', amount(value),
            'upper' if comparison in {'above', 'at least'} else 'lower',
            fingerprint(canonical, source), end=pd.Timestamp(when))
    raise ValueError('unsupported_contract_template')
