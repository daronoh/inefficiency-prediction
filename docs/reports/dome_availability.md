# Dome historical order-book availability

Checked **29 October 2026**.

## Finding

Dome is not a working, supported acquisition path for expanding this dataset.
Its [official documentation](https://docs.domeapi.io/) states that all Dome APIs
reached end of life on **28 April 2026**, following the Polymarket acquisition.
A bounded authenticated check of the historical endpoint returned **HTTP 503**
and no order-book data. The documentation site returned HTTP 200 in the same
network-enabled check.

The 503 alone could be temporary and does not establish whether a private
archive still exists or whether an API key is valid. Together with the official
retirement notice, the evidence does not support restoring Dome as a reliable
download source. No bulk acquisition or changes to frozen research data occurred.

## Check performed

The archived downloader uses:

```text
GET https://api.domeapi.io/v1/polymarket/orderbooks
token_id=<existing recorded token>
start_time=<known local snapshot time, milliseconds>
end_time=<one minute later, milliseconds>
limit=1
```

The selected saved token has a local recording at
**14 October 2025, 19:55:22.539 UTC**. The request used the existing local key
with the downloader's authentication headers. Credentials were not printed or
saved in this report. Redirects were disabled and request timeouts bounded.

Initial API and documentation requests both failed inside the network sandbox,
so those failures were not treated as evidence of a provider outage. The
network-enabled retry distinguished the endpoints:

| Request | Result |
| --- | --- |
| Authenticated historical order-book request | 503 Service Temporarily Unavailable; HTML response |
| Documentation HTTPS control | 200 |

The [historical endpoint documentation](https://docs.domeapi.io/api-reference/endpoint/get-orderbook-history)
still describes time-range snapshots containing bids, asks and metadata, with
history starting on 14 October 2025. Its continued publication does not establish
continued API operation.

## Implications for the experiment

The completed final evaluation has eight observed events, at most five competing
at a scan, and 16 known positive event-hours in two events. Every saved model and
quoted-gap captures all known positives at N=3. This ceiling prevents establishing
a model advantage on this sample. It does not prove that more data would make ML
outperform quoted-gap.

A useful expansion would add more simultaneously active, independently varying
events and families, more opportunity-bearing events, and complete member-book
coverage across training and a fresh later test. Additional histories of the same
few events provide less independent evidence. Adding inactive or unrecorded
events would not fix the comparison.

More events could expose more total opportunities and create a harder selection
problem, but neither higher net profitability nor ML superiority follows
automatically. The current model comparison and opportunity census are limited
to the recorded cohort.

The replacement must provide historical depth or reconstructible book changes,
not just historical prices/trades. Polymarket's documented
[order-book endpoint](https://docs.polymarket.com/api-reference/market-data/get-order-book)
accepts a token ID without a historical time-range parameter. Its
[price-history endpoint](https://docs.polymarket.com/api-reference/markets/get-prices-history)
returns timestamp/price points, which cannot supply the historical bid/ask sizes
needed by this simulation. These documented endpoints do not establish a direct
replacement for Dome's depth-history download.

An alternative archive, explicit historical export access, or prospective
collection must be validated separately. The existing final period has already
been examined, so expanded modelling needs a new untouched test rather than
silently replacing the completed result.
