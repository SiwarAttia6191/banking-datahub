# Migration — Hadoop estate → cloud-native, step by step

This folder holds the **legacy side** and the migration artifacts. The target stack (object
storage, Spark, Airflow, Kubernetes) is built in the rest of the repo; this track shows how
we get there from a classic Hadoop estate.

**No Hadoop cluster is needed anywhere in this repo**: the legacy side is represented by real
artifacts (Oozie workflow, Hive script) plus a legacy layout on object storage, and the
methodology is demonstrated by free, runnable jobs. Cloudera is a paid subscription and the
old HDP sandboxes are end-of-life and heavy — the migration discipline is what transfers.

| stage | what happens | artifact | status |
|---|---|---|---|
| 0 — inventory | catalog the estate: workflows, schedules, dependencies | `legacy/oozie/`, `legacy/hive/` | done |
| 1 — legacy logic as reference | the processing logic kept verbatim | `legacy/hive/daily_load.hql` | done |
| 2 — storage move + format conversion | feeds → object storage inbox → date-partitioned parquet | `scripts/upload_sources.py`, `modules/ingestion` | done |
| 3 — logic port: HiveQL → Spark | same rules, typed Scala | `modules/transform` | phase 2 |
| 4 — parallel run + reconciliation | legacy baseline vs modern outputs, per partition, go/no-go report | reconciliation job | phase 3 |
| 5 — orchestration re-platform | Oozie workflow/coordinator → Airflow TaskFlow DAGs + deferrable sensor | `airflow/dags` | phase 4 |
| 6 — compute migration | YARN/VMs → ephemeral Spark on Kubernetes | `k8s/` | phase 5 |
| 7 — cutover + decommission | domain by domain, rollback plan, decommission checklist | `docs/migration-guide.md` | phase 5 |

Full narrative, Oozie→Airflow concept mapping and cutover strategy:
[docs/migration-guide.md](../docs/migration-guide.md)
