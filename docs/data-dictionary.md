# Data dictionary — every column, layer by layer

One row here = one column somewhere in the hub. Read top to bottom: the raw zone
stores what arrived, the curated zone decides what it means, the serving zone is
what the business sees. Types are the curated-layer types (the raw zone stores
everything as strings - see the raw section note).

## Raw zone (s3 `lake-raw`, parquet, all-string columns)

The landing zone: files exactly as the feeds delivered them. Raw transactions
partition by `ingestion_date` (the value date the feed row belongs to - one folder
per day); reference feeds (clients, accounts, restricted countries) have no business
date, so each upload lands in its own ingestion_date folder and the curated layer
deduplicates them to the latest version.

`inbox/transactions/...` and `raw/transactions/...`:

| column | meaning |
|---|---|
| `txn_id` | the transaction's business key; duplicated deliveries of the same id are collapsed by "keep latest version" |
| `account_id` | which bank account moved (FK to the accounts feed) |
| `direction` | `in` (money received) or `out` (money sent) |
| `amount` | the amount in the account's own currency |
| `currency` | ISO-4217 code of that currency (EUR, USD, CHF...) |
| `channel` | how the payment travelled: `domestic`, `sepa`, `swift`, or `cash` (branch/ATM deposit - the channel the structuring rule screens) |
| `counterparty_name` | who is on the other side |
| `counterparty_bic` | synthetic SWIFT/BIC code of the counterparty bank (country segment at positions 5-6) |
| `counterparty_country` | ISO-3166 alpha-2 code of the counterparty country |
| `counterparty_country_name` | the readable name of that country (dashboards show this, not the code) |
| `value_date` | the business day the transaction belongs to |
| `txn_timestamp` | when it happened during that day |

`raw/clients/...`: `client_id` · `name` (stays in raw only - curated carries hash+mask)
· `client_type` (`corporate`/`retail`) · `country` · `risk_level` (`low`/`medium`/`high`)
· `kyc_status` (`verified`/`pending`).

`raw/accounts/...`: `account_id` · `client_id` · `iban` (mod-97 valid, one deliberately
corrupted for the validity check) · `currency` · `status` (`active`/`closed`).

`raw/fx_rates/...`: `rate_date` · `base_ccy` (always EUR) · `quote_ccy` · `rate`
(units of the quote currency per 1 EUR - so EUR amount = amount / rate).

`raw/restricted_countries/...`: `country_code` · `country_name` · `list_source`
(`sanctions_sim` = FATF blacklist snapshot mapped to sanctions screening,
`fatf_grey_list` = FATF grey-list snapshot - see docs/detection-rules.md) ·
`effective_date` (from when the listing applies).

## Curated zone (s3 `lake-curated`, parquet, typed)

### `curated/transactions/` (partitioned by `business_date`)

| column | type | meaning |
|---|---|---|
| `txn_id` | string | business key, deduplicated - exactly one row per id per layer |
| `account_id` | string | the account that moved |
| `direction` | string | `in` / `out` |
| `amount` | double | amount in the account currency (validated: > 0) |
| `currency` | string | account currency (validated: known code) |
| `amount_eur` | double | the EUR-converted amount: `amount / rate_applied` |
| `fx_fallback_used` | boolean | true when no rate existed for that exact day and the latest available rate was used - an anomaly flag that announces itself |
| `rate_applied` | double | the FX rate actually used (day rate, else fallback) |
| `channel` | string | `domestic` / `sepa` / `swift` / `cash` |
| `counterparty_name` | string | counterparty |
| `counterparty_bic` | string | counterparty bank code |
| `counterparty_country` | string | ISO code |
| `counterparty_country_name` | string | readable country name |
| `business_date` | date | partition column - the value date, typed |

### `curated/rejects/` (partitioned by `business_date`)

Rows removed from the stream *visibly* instead of silently: `txn_id` · `account_id`
· `amount` · `currency` · `reject_reason` (`missing_account` - unknown account,
`invalid_amount` - zero/negative, `unknown_currency` - the injected `XX`) ·
`business_date`.

### `curated/accounts/`

`account_id` · `client_id` (FK) · `iban` · `iban_valid` (boolean from the ISO-13616
mod-97 check - the quarantine check counts the false ones) · `currency` · `status`.

### `curated/clients/` (pseudonymized)

`client_id` · `client_type` · `country` · `risk_level` · `kyc_status` ·
`client_name_sha2` (sha-256 of the name - identity leaves the raw zone only
pseudonymized) · `client_name_masked` (first letter + `***`).

### `curated/restricted_countries/`

`country_code` · `country_name` · `list_source` · `effective_date` (date, typed).

### `curated/account_day_features/` (partitioned by `business_date`)

One row per account-direction-day: how this account normally behaves, so rules can
compare "today" against "normal".

| column | type | meaning |
|---|---|---|
| `txn_count` | int | transactions that day (this account, this direction) |
| `day_sum_eur` | double | total EUR that day |
| `day_log` | double | log10(day_sum + 1) - the log transform before statistics (heavy-tailed amounts) |
| `sum_7d_eur` | double | rolling 7-day total ending on the day |
| `count_7d` | int | rolling 7-day transaction count |
| `baseline_points` | int | how many past days feed the baseline (tested only if >= `min-vector-length`) |
| `baseline_mean_log` | double | mean of the baseline window's day_log (31 days, offset 7 - the evaluated day and week before never enter their own baseline) |
| `baseline_std_log` | double | baseline standard deviation (0 std = no z-score - no false positives on flat accounts) |
| `q1` / `q3` | double | 25th/75th percentiles of the baseline - the IQR boxplot bounds |
| `boxplot_upper` | double | q3 + 1.5 * IQR - above this is an outlier |
| `zscore_day_log` | double | (day_log - mean) / std - how many sigmas above normal |
| `boxplot_outlier` | boolean | day_log > boxplot_upper |

### `curated/alerts/` (partitioned by `business_date`)

One row per (rule, transaction/account-day) hit:

| column | type | meaning |
|---|---|---|
| `alert_id` | string | sha-256 of rule+txn+account+date (first 16 chars) - deterministic, reruns never duplicate |
| `rule_id` | string | which rule fired (see docs/detection-rules.md) |
| `business_date` | date | the evaluated day |
| `account_id` | string | the account under suspicion |
| `txn_id` | string, nullable | the triggering transaction (null for account-day rules) |
| `amount_eur` | double | the amount the rule saw (the day total for account-day rules) |
| `detail` | string | human-readable trigger evidence, e.g. `list=sanctions_sim country=Iran`, `cash deposits in [9000,10000): n=3`, `log-zscore=2.6` |
| `score` | double | rule weight (85 structuring, 80 sanctions, 75 rapid, 65 grey list, 55 dormant, spike scales with z) |
| `severity` | string | `critical` >= 100 · `high` >= 70 · `medium` >= 40 · else `low` |

### `curated/risk_ratings/` (partitioned by `business_date`)

The triage rollup - one row per account-day that has at least one alert:
`business_date` · `account_id` · `client_id` · `risk_level` (the client's own
baseline risk) · `max_alert_score` · `alert_count` · `aml_risk_rating` (from the
day's alerts alone) · `final_rating` (AML rating wins when high/critical, else the
client baseline floors it) - this is "who needs a human today".

### `reports/dq/` (json, one per run)

`business_date` · `verdict` (`PASS`/`FAIL`) · `results[]`: each with
`check.name` (raw_vs_curated_reconciliation, dedup_completeness, reject_rate,
referential_integrity, fx_conversion_complete, iban_validity), `check.severity`
(`fail` blocks the verdict, `quarantine` records without blocking), `passed`,
`observed`, `expected`.

## Serving zone (postgres :5436, schema `serving`)

The consumption copy the business tools read - the lake stays the source of truth.

`serving.daily_volumes`: `business_date` (date) · `account_id` · `direction` ·
`txn_count` · `day_sum_eur` · `sum_7d_eur` · `count_7d` · `zscore_day_log` ·
`boxplot_outlier` · `published_at` (timestamp - the batch stamp).

`serving.aml_alerts`: `alert_id` · `rule_id` · `business_date` · `account_id` ·
`txn_id` · `amount_eur` · `detail` · `score` · `severity` · `published_at`.

Publication is append-by-day: republishing a day replaces that day's slice in the
analytics indexes (the Elasticsearch `_id` is the business key), so dashboards never
double-count.

## Case management (postgres :5436, schema `cases`)

`cases.alert_cases`: `case_id` (uuid) · `alert_id` (unique - one case per alert, ever)
· `rule_id` · `business_date` · `account_id` · `severity` · `score` · `status`
(`NEW` -> `IN_REVIEW` -> `CLOSED`, enforced) · `assignee` · `note` · `opened_at` ·
`updated_at`.

`cases.case_events`: `event_id` · `case_id` · `event` (`OPENED` / status moves) ·
`actor` · `note` · `at` - the audit trail.

## Search layer (elasticsearch indexes)

`aml_alerts`, `daily_volumes`, `risk_ratings` mirror the serving tables (same
fields, plus the same `_id` business keys); `scripts/publish_to_es.py` loads them
and registers the kibana data views.
