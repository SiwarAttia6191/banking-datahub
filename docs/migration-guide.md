# Migration guide — from a Hadoop estate to the cloud-native stack

The scenario demonstrated in this repo: a classic bank data platform — HDFS + Hive + Oozie
on-prem, coupled compute and storage — moving to object storage, Spark on Kubernetes and
Airflow. Every step is backed by an artifact in the repo; nothing is hand-waved.

## Free-tool mapping (why no Hadoop cluster is needed)

| real estate | in this repo | why this is enough |
|---|---|---|
| HDFS | SeaweedFS (S3-compatible) with a legacy CSV layout + an inbox prefix | the migration discipline is what transfers; a running HDFS proves nothing extra |
| Hive processing | `migration/legacy/hive/daily_load.hql` kept verbatim + its Spark port | files only |
| Oozie orchestration | `migration/legacy/oozie/` artifacts (workflow, coordinator, properties) | Oozie needs a Hadoop cluster to run; for a migration study the artifact is the point |
| YARN compute | Spark 3.5 (local or container) | Apache Spark, free |
| target: Spark on Kubernetes | `k8s/` manifests, runnable on Docker Desktop or kind | free |
| target: COS / S3 | SeaweedFS again — same S3 API | free |

Cloudera CDP is a paid subscription, and the old free HDP sandboxes are end-of-life and heavy
(12–32 GB RAM). Neither adds anything to the methodology demonstrated here.

## The steps

### 0 — inventory the estate

Catalog every workflow, schedule, table and downstream consumer before touching anything:
`migration/legacy/` is that inventory (workflow.xml, coordinator.xml, job.properties, the Hive
script). The modern pipeline must reproduce **behavior**, not just syntax — so the legacy logic
stays as the reference throughout.

### 1 — move the data: HDFS → object storage, text → parquet

Feeds land on an object-storage inbox (`s3://lake-raw/inbox/...`, uploaded by
`scripts/upload_sources.py`) and the ingestion job converts them to date-partitioned parquet
under `raw/` — the same move as distcp + format conversion, minus the cluster. Reference
snapshots and daily feeds land separately; both become immutable raw partitions.

### 2 — port the logic: HiveQL → Spark

`migration/legacy/hive/daily_load.hql` is the legacy processing: staging external table over the
day's feed, deduplication by `row_number` on the business key, business-rule filters, then
`insert overwrite` into the day's partition. The curated Spark job reimplements it in typed
Scala. Behavior must match exactly — that is what the next step proves.

### 3 — run both sides in parallel + reconcile

The legacy baseline (a Spark SQL port of the Hive script) and the modern curated job write to
separate locations. A reconciliation job compares them **per partition** — row counts, checksums
and a key-level diff — and emits a go/no-go report. No big-bang cutovers: a domain moves only
when its reconciliation report is clean.

### 4 — re-platform orchestration: Oozie → Airflow

| Oozie | Airflow |
|---|---|
| workflow.xml actions (fs, hive, shell) | tasks (BashOperator, SparkSubmitOperator) |
| coordinator frequency / start / end | schedule + catchup |
| datasets + data-in events (feed availability) | deferrable Sensors, Airflow datasets |
| EL parameters `${jobDate}` | Jinja templating `{{ ds }}` (logical date) |
| fork / join / decision nodes | task groups, branching |
| bundle (many coordinators) | multiple DAGs |

Strategy: run both schedulers over the same inputs, reconcile per domain, cut over domain by
domain, keep a rollback path, decommission last.

### 5 — move compute: YARN → Kubernetes

Spark jobs become containerized SparkApplications (`k8s/`): a driver pod schedules executor pods
per run, the image carries the dependencies, storage stays on object storage. Compute becomes
ephemeral and scales per job — the "as a service" model.

### 6 — cutover, rollback, decommission

Domain by domain, with the reconciliation gate as the cutover condition. Rollback means
switching scheduling back while the legacy paths still exist. Legacy artifacts are decommissioned
only after a full quiet period.

## Artifact index

| step | artifact |
|---|---|
| 0 | `migration/legacy/**` |
| 1 | `scripts/upload_sources.py`, `modules/ingestion` |
| 2 | `migration/legacy/hive/daily_load.hql`, `modules/transform` |
| 3 | reconciliation job (phase 3), reports under `lake-curated/reconciliation/` |
| 4 | `airflow/dags` (phase 4) |
| 5 | `k8s/` (phase 5) |
| 6 | this document — cutover checklist finalized in phase 5 |
