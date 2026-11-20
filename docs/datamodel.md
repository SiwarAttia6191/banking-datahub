# Data model

Fully synthetic datasets that imitate the reference and transactional data of a small corporate
banking book. All names, IBANs and BICs are fake; IBAN check digits are computed correctly with
the ISO-13616 mod-97 algorithm so validation logic can be exercised for real.

## Entities

| entity | file | columns |
|---|---|---|
| clients | `clients/clients.csv` | client_id, name, client_type (corporate/retail), country, risk_level (low/medium/high), kyc_status (verified/pending) |
| accounts | `accounts/accounts.csv` | account_id, client_id, iban, currency, status (active/closed) |
| transactions | `transactions/business_date=…/transactions.csv` | txn_id, account_id, direction (in/out), amount, currency, channel (sepa/swift/domestic/cash), counterparty_name, counterparty_bic, counterparty_country, counterparty_country_name, value_date, txn_timestamp |

BICs are synthetic SWIFT codes: ~70% BIC8, ~30% BIC11 with a branch identifier (mostly `XXX`).
The country segment sits at positions 5–6 — that's what the detection rules parse.
| fx_rates | `fx_rates/business_date=…/fx_rates.csv` | rate_date, base_ccy (always EUR), quote_ccy, rate |
| restricted_countries | `restricted_countries/restricted_countries.csv` | country_code, country_name, list_source (sanctions_sim = FATF blacklist snapshot, fatf_grey_list = grey list), effective_date |

## FX semantics

Rates are quotes per 1 EUR (`base_ccy = EUR`). Converting an amount in currency X to EUR is
therefore `amount / rate(X)`. Missing-rate handling is a curated-layer rule (phase 2), not a
generator concern.

## Intentionally dirty data

The generator injects a small controlled share of problems (default ~2% + ~1% duplicates) so the
data-quality layer has real work to demonstrate:

- negative amounts
- unknown currency code `XX`
- transactions referencing a non-existent account (`ACCT000000`)
- transactions with a corrupted IBAN on the account side (check-digit failures)
- verbatim duplicate transactions (same txn_id) → deduplication by key, keep latest

## Column-level reference

Every column of every table (raw, curated, serving, cases, search) is explained in
[docs/data-dictionary.md](data-dictionary.md). Operating the pipeline step by step
(runs, logs, verification, the incident loop): [docs/runbook.md](runbook.md).

## Precision note

Amounts are `double` for demo simplicity. In a production ledger context amounts would be
`decimal` (or scaled longs) to avoid float rounding in financial sums.
