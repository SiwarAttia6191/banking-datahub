# Detection rules: catalog and public sources

Every rule in this hub is grounded in **public** material only - FATF (Financial
Action Task Force) publications, US Treasury/FinCEN regulations, and the EU AML
framework. Nothing here comes from any employer's internal documentation: the
typologies are public knowledge, and the thresholds below cite where they come
from. This page exists so that every design choice survives the question
"why does this rule exist, and why these numbers?".

Honest framing: the data is synthetic and the lists are simulated snapshots -
the point is the rule logic and its documentation, not a production screen.

## The rule catalog

| rule | family | trigger | score | severity |
|---|---|---|---|---|
| `RULE_VOLUME_SPIKE` | statistical | the account-day sum is a log10 z-score outlier (config: 3.0) or an IQR (interquartile range) boxplot outlier vs the 31-day baseline | 60 x min(z/threshold, 2) | up to critical |
| `RULE_RESTRICTED_COUNTRY` | list-based | counterparty country sits on the restricted-countries feed (`sanctions_sim` = sanctions screening, `fatf_grey_list` = FATF increased monitoring) | 80 sanctions / 65 grey list | high / medium |
| `RULE_STRUCTURING` | typology | >= 2 inbound cash deposits in [9000, 10000) EUR on the same account-day (the window just below the reporting cap) | 85 | high |
| `RULE_RAPID_MOVEMENT` | typology | the same account-day receives >= 10,000 EUR inbound AND >= 10,000 EUR outbound - the funds pass through instead of staying | 75 | high |
| `RULE_DORMANT_REACTIVATION` | typology | an account with no activity for > 30 days suddenly moves >= 1,000 EUR | 55 | medium |

All thresholds live in `application.conf` (`datahub.rules`) - configuration, not
code, mirroring the way production rule books are tuned per geography.

## The typologies, and where they come from

### Structuring (smurfing)

Splitting cash amounts to stay under a reporting threshold is a distinct offence.
The US regime is the most explicit: structuring to evade a Currency Transaction
Report (CTR, filed for cash transactions over $10,000) is prohibited by
31 U.S.C. § 5324 and implemented in 31 CFR 1010.314; FinCEN and the IRS exam
manual treat "multiple transactions each below the threshold" as the canonical
pattern. The EU regime sets a 10,000 EUR cash cap for occasional transactions
(Anti-Money Laundering Directive (EU) 2015/849, art. 10). Hence the rule window
[9000, 10000) EUR with a minimum count of 2 on one account-day.

Sources:
- 31 U.S.C. § 5324 - structuring prohibition: <https://www.law.cornell.edu/uscode/text/31/5324>
- 31 CFR 1010.314 - structured transactions: <https://www.ecfr.gov/current/title-31/subtitle-B/chapter-X/part-1010/subpart-C/section-1010.314>
- IRS IRM 4.26.13 - structuring (examination guidance): <https://www.irs.gov/irm/part4/irm_04-026-013>
- Directive (EU) 2015/849 (4th AMLD), art. 10 - 10,000 EUR cash threshold: <https://eur-lex.europa.eu/eli/dir/2015/849/oj>

### Rapid movement / pass-through (layering)

Funds that arrive and leave an account within the same day are the classic
layering step of the placement-layering-integration model: the money "washes
through" without the account accumulating wealth. FATF typology work describes
the pattern across its reports on money laundering techniques. The rule keys on
both directions of the same account-day crossing a floor (10,000 EUR here).

Sources:
- FATF publications and typologies: <https://www.fatf-gafi.org/en/publications.html>
- FATF black & grey lists page (the list snapshot below): <https://www.fatf-gafi.org/en/countries/black-and-grey-lists.html>

### Dormant reactivation

An account that has been idle for months and suddenly transacts is a standard
scenario in transaction-monitoring catalogs: dormancy is often deliberate
(staged mule accounts kept quiet until used). The rule uses a 30-day idle window
and a minimum first-movement amount, both configurable.

### Volume spike (statistical)

Peer-baseline deviation: the account's own 31-day history (excluding the
evaluated week) provides the baseline; the day's log10-transformed sum is tested
as a z-score and against IQR boxplot bounds, with a minimum-history gate to
avoid testing accounts without enough datapoints. The statistical method itself
is standard (z-score / IQR) - see any statistics reference; the AML framing
(behavioral baselines per account) is the public typology above.

### Restricted country (list-based)

Sanctions screening and high-risk jurisdiction monitoring are regulatory
obligations across the US (OFAC), EU and UK regimes. The generator grounds the
restricted-countries feed in the public FATF snapshot (see below), so the
rule screens real high-risk jurisdictions rather than invented codes.

## The FATF list snapshot (retrieved 2026-11-19)

FATF reviews the lists three times a year (February, June, October plenaries).
This snapshot is the 30 October 2026 statement - the generator maps:
blacklist -> `sanctions_sim`, grey list -> `fatf_grey_list`.

| list | jurisdictions |
|---|---|
| High-Risk Jurisdictions (call for action) | DPRK (KP), Iran (IR), Myanmar (MM) |
| Jurisdictions under Increased Monitoring (18) | Angola (AO), Bolivia (BO), Bosnia and Herzegovina (BA), Cameroon (CM), Haiti (HT), Iraq (IQ), Kenya (KE), Kuwait (KW), Laos (LA), Lebanon (LB), Nepal (NP), Papua New Guinea (PG), South Sudan (SS), Syria (SY), Venezuela (VE), Vietnam (VN), British Virgin Islands (VG), Yemen (YE) |

Source: <https://www.fatf-gafi.org/en/countries/black-and-grey-lists.html>

## How the rules score into triage

Every alert carries a numeric score; `triage` rolls up per account-day: the max
alert score + the client's baseline risk level decide the final AML risk rating
(critical >= 100, high >= 70, medium >= 40, otherwise low). Alert ids are
deterministic (hash of rule + transaction + account + date), so rerunning a day
never duplicates an alert.

## What a production rule book adds (honest boundary)

Real institutions add: per-geography tuning, scenario backtesting against
historical alerts, hit-quality review loops feeding threshold tuning, watchlist
fuzzy matching (name screening, not just country), and case management
integration (see docs/case-handoff.md for that handoff). The five rules here are
the minimum set that still demonstrates every pattern honestly.
