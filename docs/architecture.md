# Architecture — sequence diagrams

Diagram-first reference: one mechanism per diagram. The stack is intentionally laptop-sized —
the local s3 service is a stand-in for any S3-compatible cloud object storage, so the same jobs move to a
managed environment by changing config only.

## 1. End-of-day flow: files → raw → curated → serving (one run)

```mermaid
sequenceDiagram
    autonumber
    participant SRC as Source feeds<br/>(lake-raw inbox)
    participant SPARK as Spark jobs<br/>(Scala 2.13)
    participant RAW as S3 lake-raw<br/>(local stand-in for cloud object storage)
    participant CUR as S3 lake-curated
    participant PG as PostgreSQL serving
    participant AIR as Airflow (phase 4)

    AIR->>SPARK: spark-submit ingestion for business_date=D
    SRC->>SPARK: read clients, accounts, transactions, fx_rates (csv from inbox)
    SPARK->>RAW: write parquet raw/{domain}/ingestion_date=D (as landed, strings only)
    AIR->>SPARK: spark-submit curated-transactions for D
    SPARK->>RAW: read the raw partitions of D
    SPARK->>CUR: write curated transactions + rejects path<br/>(typed, deduped, fx-converted)
    AIR->>SPARK: spark-submit features + detection for D
    SPARK->>CUR: write account-day features, alerts, risk ratings
    AIR->>SPARK: spark-submit data-quality for D
    SPARK-->>AIR: DQ report per check (pass/fail + counts), go/no-go verdict
    AIR->>SPARK: spark-submit publish for D
    SPARK->>PG: overwrite serving tables for D
```

## 2. Rerun of a single date (why the pipeline is idempotent)

```mermaid
sequenceDiagram
    autonumber
    participant OPS as Operator or Airflow retry
    participant SPARK as Spark job
    participant RAW as lake-raw
    participant CUR as lake-curated

    OPS->>SPARK: rerun business_date=2026-11-17 only
    SPARK->>RAW: read partitions where ingestion_date = 2026-11-17
    Note over SPARK: same input, same logic, same output (deterministic)
    SPARK->>CUR: dynamic overwrite of business_date=2026-11-17 only
    Note over CUR: other dates untouched - no side effects
    SPARK-->>OPS: done, partition-level reconciliation counts
```

## 3. Data-quality gate (one partition)

```mermaid
flowchart TD
    A[read curated partition] --> B{schema check}
    B -- fail --> F[fail the batch, alert]
    B -- pass --> C{row-level checks<br/>not-null keys, amount > 0, currency in ISO list}
    C -- bad rows --> R[route to rejects path<br/>rejects/business_date=D]
    C -- clean rows --> D{referential integrity<br/>+ reconciliation counts vs raw}
    D -- fail --> F
    D -- pass --> OK[write DQ report, mark partition published]
    R --> RPT[write DQ report with rejection stats]
```

## 4. Zones and write modes

```mermaid
flowchart LR
    RAW[(raw<br/>as landed)] --> CUR[(curated<br/>validated)] --> SRV[(serving<br/>aggregates, alerts)]
```

| zone | partitioned by | write mode | content |
|---|---|---|---|
| raw | `ingestion_date` | one partition per run, immutable | source data exactly as landed |
| curated | `business_date` | dynamic overwrite per date | validated, deduplicated, enriched |
| serving | `business_date` | overwrite per date | aggregates and alert tables |

## Dremio: bi queries straight on the lake

The curated layer lives as parquet on object storage. Dremio reads it **in place** -
no data copy, no ingestion step: connect the s3 source, define virtual views over the
curated folders, and business users get sql on the lake. This is the "dremio on the
lake" serving pattern - the bi tool queries the lake directly, while the postgres
serving layer keeps a small row-level copy for the fastest dashboards.

## Design notes

- **Parquet past ingestion** — columnar, compressed, schema embedded, predicate pushdown.
- **Date partitioning** — every downstream query filters on a date; a rerun overwrites exactly one
  partition, which is what makes the pipeline idempotent (diagram 2).
- **Fail vs quarantine** — schema and reconciliation failures stop the batch; row-level problems
  are routed to a rejects path with stats in the DQ report (diagram 3).
- **Config, not code, separates environments** — local, staging and production differ only by
  `application.conf` (endpoint, credentials, bucket names).
