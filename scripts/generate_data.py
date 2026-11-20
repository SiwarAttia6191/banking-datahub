#!/usr/bin/env python3
"""Synthetic data generator for the banking data hub demo.

Creates small, realistic-ish corporate banking datasets, fully fake and
deterministic for a given seed:

  clients                reference data (one static file)
  accounts               reference data with IBANs (valid check digits)
  transactions           daily files, with a controlled share of dirty rows
                         (duplicates, unknown currency, missing account refs,
                         negative amounts) so the data-quality layer has work
  fx_rates               daily quotes, base EUR
  restricted_countries   reference watchlist used by detection rules

Standard library only. Output layout mirrors the raw lake:

  {out}/clients/clients.csv
  {out}/accounts/accounts.csv
  {out}/restricted_countries/restricted_countries.csv
  {out}/fx_rates/business_date=YYYY-MM-DD/fx_rates.csv
  {out}/transactions/business_date=YYYY-MM-DD/transactions.csv
"""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

# countries used for accounts (all have an ISO-13616 IBAN layout)
IBAN_LENGTHS = {
    "ES": 24, "FR": 27, "DE": 22, "IT": 27, "BE": 16,
    "NL": 18, "PT": 25, "TN": 24, "GB": 22, "CH": 21,
}
COUNTRY_CURRENCY = {
    "ES": "EUR", "FR": "EUR", "DE": "EUR", "IT": "EUR", "BE": "EUR",
    "NL": "EUR", "PT": "EUR", "TN": "TND", "GB": "GBP", "CH": "CHF",
}
EUR_COUNTRIES = [c for c, cc in COUNTRY_CURRENCY.items() if cc == "EUR"]

FX_BASE_RATES = {  # quote units per 1 EUR, rough 2026-ish values
    "USD": 1.08, "GBP": 0.86, "CHF": 0.94, "TND": 3.40, "SEK": 11.30, "JPY": 162.0,
}
FX_CURRENCIES = ["USD", "GBP", "CHF", "TND", "SEK", "JPY"]

# Restricted jurisdictions, grounded in the PUBLIC FATF snapshot (retrieved 2026-11-19,
# statement dated 30 Oct 2026 - https://www.fatf-gafi.org/en/countries/black-and-grey-lists.html):
# blacklist ("call for action") -> sanctions_sim, grey list ("increased monitoring")
# -> fatf_grey_list. Simulated screening: the lists evolve three times a year.
FATF_BLACKLIST = ["KP", "IR", "MM"]
FATF_GREYLIST = ["IQ", "YE", "SY", "LB", "VE", "HT", "CM", "KE", "AO", "BA", "KW", "SS"]
RESTRICTED = FATF_BLACKLIST + FATF_GREYLIST
RESTRICTED_LIST_SOURCE = {**{c: "sanctions_sim" for c in FATF_BLACKLIST},
                          **{c: "fatf_grey_list" for c in FATF_GREYLIST}}

SECTORS = ["Logistics", "Textiles", "Pharma", "Construction", "AgriFood",
           "Software", "Energy", "Retail", "Metalworks", "Printing"]
COMPANY_SUFFIX = ["SARL", "SA", "GmbH", "Ltd", "B.V.", "SpA"]
FIRST_NAMES = ["Amira", "Lucas", "Nadia", "Marco", "Sofia", "Youssef", "Elena",
               "Tomas", "Leila", "Jonas", "Carmen", "Hugo", "Ines", "Karim",
               "Marta", "Rafael", "Sonia", "Pablo", "Dalia", "Enzo"]
LAST_NAMES = ["Ben Salah", "Martin", "Haddad", "Rossi", "Lopez", "Trabelsi",
              "Dubois", "Kraus", "Silva", "Novak", "Moreau", "Garcia",
              "Baccouche", "Weber", "Costa", "Ferreira", "Mansour", "Keller"]


def iban_check_digits(country: str, bban: str) -> str:
    """ISO-13616 mod-97 check digits for a country code + BBAN."""
    rearranged = bban + country + "00"
    converted = "".join(str(ord(c) - 55) if c.isalpha() else c for c in rearranged)
    return f"{98 - (int(converted) % 97):02d}"


def make_iban(country: str, rng: random.Random) -> str:
    bban = "".join(rng.choice("0123456789") for _ in range(IBAN_LENGTHS[country] - 4))
    return f"{country}{iban_check_digits(country, bban)}{bban}"


def break_iban(iban: str, rng: random.Random) -> str:
    """Corrupt one digit after the check digits so validation fails."""
    i = rng.randrange(4, len(iban))
    other = rng.choice([d for d in "0123456789" if d != iban[i]])
    return iban[:i] + other + iban[i + 1:]


def make_bic(country: str, rng: random.Random) -> str:
    """Synthetic SWIFT/BIC code: BIC8, or BIC11 with a branch identifier."""
    bank = "".join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(4))
    loc = f"{rng.randrange(10)}{rng.randrange(10)}"
    bic8 = f"{bank}{country}{loc}"
    if rng.random() < 0.3:
        branch = "XXX" if rng.random() < 0.7 else "".join(
            rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(3)
        )
        return bic8 + branch
    return bic8


def company_name(rng: random.Random) -> str:
    last = rng.choice(LAST_NAMES)
    return f"{last} {rng.choice(SECTORS)} {rng.choice(COMPANY_SUFFIX)}"


def person_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def weighted_choice(rng: random.Random, options, weights):
    return rng.choices(options, weights=weights, k=1)[0]


def write_csv(path: Path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/output", help="output root directory")
    parser.add_argument("--samples", action="store_true",
                        help="also write tiny csv samples to data/samples/")
    parser.add_argument("--start-date", default=None, help="first business date (yyyy-mm-dd)")
    parser.add_argument("--days", type=int, default=45)
    parser.add_argument("--clients", type=int, default=30)
    parser.add_argument("--txns-per-day", type=int, default=300)
    parser.add_argument("--dirty-rate", type=float, default=0.02,
                        help="share of transactions with an injected data problem")
    parser.add_argument("--dup-rate", type=float, default=0.01,
                        help="share of transactions duplicated verbatim")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)

    days = args.days
    if args.start_date:
        y, m, d = (int(x) for x in args.start_date.split("-"))
        start = date(y, m, d)
    else:
        start = date.today() - timedelta(days=days - 1)
    business_dates = [(start + timedelta(days=i)).isoformat() for i in range(days)]

    # ---- reference data -------------------------------------------------
    clients = []
    for i in range(args.clients):
        is_corporate = rng.random() < 0.3
        clients.append({
            "client_id": f"CLT{i + 1:05d}",
            "name": company_name(rng) if is_corporate else person_name(rng),
            "client_type": "corporate" if is_corporate else "retail",
            "country": rng.choice(list(IBAN_LENGTHS)),
            "risk_level": weighted_choice(rng, ["low", "medium", "high"], [60, 30, 10]),
            "kyc_status": "verified" if rng.random() < 0.92 else "pending",
        })
    write_csv(out / "clients" / "clients.csv",
              ["client_id", "name", "client_type", "country", "risk_level", "kyc_status"],
              clients)

    accounts = []
    for client in clients:
        for _ in range(rng.randint(1, 3)):
            iban = make_iban(client["country"], rng)
            if rng.random() < 0.03:
                iban = break_iban(iban, rng)  # check-digit corruption for the iban_valid flag
            accounts.append({
                "account_id": f"ACCT{rng.randrange(1_000_000):06d}",
                "client_id": client["client_id"],
                "iban": iban,
                "currency": COUNTRY_CURRENCY[client["country"]],
                "status": "active" if rng.random() < 0.95 else "closed",
            })
    # guarantee the demo has at least one invalid iban: the iban_valid flag and the
    # quarantine check need a real case to catch
    # three scenario accounts drive the typology rules with injected red-team
    # patterns (how banks validate their own rule book): structuring, rapid
    # movement (pass-through) and a dormant reactivation - all deterministic,
    # all on the last day. They stay OUT of the random daily pool, so they must
    # not be picked for the iban corruption either.
    home = next(c for c in clients if c["country"] in EUR_COUNTRIES)
    scenario_accounts = [
        {"account_id": "ACCTSC00001", "client_id": home["client_id"], "iban": make_iban("ES", rng),
         "currency": "EUR", "status": "active"},
        {"account_id": "ACCTRM00001", "client_id": home["client_id"], "iban": make_iban("ES", rng),
         "currency": "EUR", "status": "active"},
        {"account_id": "ACCTSL00001", "client_id": home["client_id"], "iban": make_iban("ES", rng),
         "currency": "EUR", "status": "active"},
    ]
    accounts.extend(scenario_accounts)
    scenario_ids = {a["account_id"] for a in scenario_accounts}

    def iban_is_invalid(a):
        ib = a["iban"]
        return iban_check_digits(ib[:2], ib[4:]) != ib[2:4]

    if not any(iban_is_invalid(a) for a in accounts if a["account_id"] not in scenario_ids):
        victim = rng.choice([a for a in accounts if a["account_id"] not in scenario_ids])
        victim["iban"] = break_iban(victim["iban"], rng)

    write_csv(out / "accounts" / "accounts.csv",
              ["account_id", "client_id", "iban", "currency", "status"], accounts)

    restricted = [{
        "country_code": code,
        "list_source": RESTRICTED_LIST_SOURCE[code],
        "effective_date": business_dates[0],
    } for code in RESTRICTED]
    write_csv(out / "restricted_countries" / "restricted_countries.csv",
              ["country_code", "list_source", "effective_date"], restricted)

    # ---- daily data ------------------------------------------------------
    # the random pool EXCLUDES the scenario accounts: a dormant scenario account
    # must have zero history before its injected day, or the typology is theatre
    active_accounts = [a for a in accounts if a["account_id"] not in scenario_ids]
    account_ids = [a["account_id"] for a in active_accounts]
    total_txns = 0
    dirty_count = 0
    duplicates = 0

    for day in business_dates:
        rates = [{"rate_date": day, "base_ccy": "EUR", "quote_ccy": "EUR", "rate": 1.0}]
        for ccy in FX_CURRENCIES:
            drifted = FX_BASE_RATES[ccy] * (1 + rng.uniform(-0.004, 0.004))
            rates.append({"rate_date": day, "base_ccy": "EUR", "quote_ccy": ccy,
                          "rate": round(drifted, 6)})
        write_csv(out / "fx_rates" / f"business_date={day}" / "fx_rates.csv",
                  ["rate_date", "base_ccy", "quote_ccy", "rate"], rates)

        rows = []
        for _ in range(int(args.txns_per_day * rng.uniform(0.8, 1.2))):
            account = rng.choice(active_accounts)
            home = account["iban"][:2]
            others = [c for c in IBAN_LENGTHS if c != home]
            options = [home] + others + RESTRICTED
            weights = [80.0] + [15.0 / len(others)] * len(others) + [5.0 / len(RESTRICTED)] * len(RESTRICTED)
            counterparty_country = weighted_choice(rng, options, weights)
            same_country = counterparty_country == account["iban"][:2]
            if same_country:
                channel = "domestic"
            elif counterparty_country in EUR_COUNTRIES and account["currency"] == "EUR":
                channel = "sepa"
            else:
                channel = "swift"
            direction = rng.choice(["in", "out"])
            # a share of inbound credits arrive as branch/atm cash deposits - the
            # channel the structuring rule screens (cash has no counterparty country)
            if direction == "in" and rng.random() < 0.04:
                channel = "cash"
                counterparty_country = account["iban"][:2]

            row = {
                "txn_id": f"TXN{rng.randrange(10_000_000):08d}",
                "account_id": account["account_id"],
                "direction": direction,
                "amount": round(rng.lognormvariate(3.0, 1.0) * 20, 2),
                "currency": account["currency"],
                "channel": channel,
                "counterparty_name": company_name(rng) if rng.random() < 0.5 else person_name(rng),
                "counterparty_bic": make_bic(counterparty_country, rng),
                "counterparty_country": counterparty_country,
                "value_date": day,
                "txn_timestamp": f"{day}T{rng.randrange(6, 23):02d}:{rng.randrange(60):02d}:{rng.randrange(60):02d}Z",
            }

            if rng.random() < args.dirty_rate:
                kind = rng.choice(["negative", "bad_currency", "missing_account"])
                if kind == "negative":
                    row["amount"] = -abs(row["amount"])
                elif kind == "bad_currency":
                    row["currency"] = "XX"
                else:
                    row["account_id"] = "ACCT000000"  # no such account
                dirty_count += 1

            rows.append(row)
            total_txns += 1

        # ---- injected red-team scenarios, last day only (deterministic) ------
        if day == business_dates[-1]:
            def sc_row(txn_id, account_id, direction, amount, channel, ts):
                return {"txn_id": txn_id, "account_id": account_id,
                        "direction": direction, "amount": amount, "currency": "EUR",
                        "channel": channel, "counterparty_name": "Scenario Counterparty",
                        "counterparty_bic": "ESXXSCENXXX", "counterparty_country": "ES",
                        "value_date": day, "txn_timestamp": ts}

            # structuring: three cash deposits just under the 10k reporting cap
            rows.extend([
                sc_row("TXNSC000001", "ACCTSC00001", "in", 9300.00, "cash", f"{day}T09:15:00Z"),
                sc_row("TXNSC000002", "ACCTSC00001", "in", 9500.00, "cash", f"{day}T11:40:00Z"),
                sc_row("TXNSC000003", "ACCTSC00001", "in", 9800.00, "cash", f"{day}T16:05:00Z"),
            ])
            # rapid movement: a large amount arrives and leaves the same day
            rows.extend([
                sc_row("TXNRM000001", "ACCTRM00001", "in", 15000.00, "swift", f"{day}T08:30:00Z"),
                sc_row("TXNRM000002", "ACCTRM00001", "out", 14500.00, "swift", f"{day}T18:45:00Z"),
            ])
            # dormant reactivation: the sleepy account transacts for the first time
            rows.append(
                sc_row("TXNSL000001", "ACCTSL00001", "in", 8000.00, "sepa", f"{day}T14:20:00Z"))
            total_txns += 6

        rows.extend(dict(r) for r in rng.sample(rows, k=int(len(rows) * args.dup_rate)))
        duplicates += int(len(rows) * args.dup_rate)

        write_csv(out / "transactions" / f"business_date={day}" / "transactions.csv",
                  ["txn_id", "account_id", "direction", "amount", "currency", "channel",
                   "counterparty_name", "counterparty_bic", "counterparty_country",
                   "value_date", "txn_timestamp"], rows)

    print(f"generated: {len(clients)} clients, {len(accounts)} accounts, "
          f"{total_txns} transactions over {days} days "
          f"(dirty: {dirty_count}, duplicates: {duplicates})")
    print(f"output: {out.resolve()}")

    if args.samples:
        samples = Path("data/samples")
        write_csv(samples / "clients.csv",
                  ["client_id", "name", "client_type", "country", "risk_level", "kyc_status"],
                  clients[:5])
        write_csv(samples / "accounts.csv",
                  ["account_id", "client_id", "iban", "currency", "status"], accounts[:6])
        write_csv(samples / "restricted_countries.csv",
                  ["country_code", "list_source", "effective_date"], restricted)
        last_day_dir = out / "transactions" / f"business_date={business_dates[-1]}"
        with (last_day_dir / "transactions.csv").open(newline="", encoding="utf-8") as f:
            txn_rows = list(csv.DictReader(f))[:6]
        write_csv(samples / "transactions.csv",
                  ["txn_id", "account_id", "direction", "amount", "currency", "channel",
                   "counterparty_name", "counterparty_bic", "counterparty_country",
                   "value_date", "txn_timestamp"], txn_rows)
        write_csv(samples / "fx_rates.csv",
                  ["rate_date", "base_ccy", "quote_ccy", "rate"], rates)
        print(f"samples: {samples.resolve()}")


if __name__ == "__main__":
    main()
