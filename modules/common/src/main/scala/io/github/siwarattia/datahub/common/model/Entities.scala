package io.github.siwarattia.datahub.common.model

/** Reference and transactional entities of the demo banking datasets.
  *
  * These mirror the CSV layouts produced by scripts/generate_data.py. Field names in the files
  * are snake_case; mapping to camelCase happens in the ingestion jobs.
  */

final case class Client(
    clientId: String,
    name: String,
    clientType: String,   // corporate | retail
    country: String,      // ISO-3166 alpha-2
    riskLevel: String,    // low | medium | high
    kycStatus: String     // verified | pending
)

final case class Account(
    accountId: String,
    clientId: String,
    iban: String,
    currency: String,
    status: String        // active | closed
)

final case class Transaction(
    txnId: String,
    accountId: String,
    direction: String,    // in | out
    amount: Double,
    currency: String,
    channel: String,      // sepa | swift | domestic
    counterpartyName: String,
    counterpartyBic: String,
    counterpartyCountry: String,
    valueDate: String,    // yyyy-MM-dd, the business date of the payment
    txnTimestamp: String  // ISO-8601 instant
)

final case class FxRate(
    rateDate: String,     // yyyy-MM-dd
    baseCcy: String,      // always EUR in this dataset
    quoteCcy: String,
    rate: Double          // quote units per 1 EUR
)

final case class RestrictedCountry(
    countryCode: String,
    listSource: String,   // internal_watchlist | sanctions_sim
    effectiveDate: String
)
