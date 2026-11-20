# Airflow orchestration

The pipeline is orchestrated by two TaskFlow DAGs — the code counterpart of the
Oozie-to-Airflow mapping in `docs/migration-guide.md` (step 4 of the migration guide).

## Dags

| dag | schedule | what it does |
|---|---|---|
| `banking_eod` | @daily | the end-of-day chain: wait for the day's feeds → generate+upload → ingest → curate → features → detect → dq (with a verdict gate that fails the dag on a non-PASS report) |
| `reconciliation_chain` | @daily | the parallel-run validation: legacy baseline (spark sql port of the hive script) → reconciliation → verdict gate |

## Run it

```bash
HOST_PROJECT_DIR="$(pwd -W)" docker compose -f docker/airflow-compose.yml up -d --build airflow
# ui: http://localhost:8080 - admin / AIRFLOW_ADMIN_PASSWORD (from the gitignored .env)
# (the container pins the admin password to that value on every start)
# (standalone-mode services over POSTGRES metadata (airflow_metadata db) + LocalExecutor:
#  the production-shaped config - sequential+sqlite is demo-only and banners a ui warning;
#  the next step up would be KubernetesExecutor, one pod per task, on a cluster)
```

The dag tasks submit the spark jobs as one-off containers on the compose network -
the same commands `scripts/run_pipeline.sh` uses. The pattern is the laptop stand-in
for the production swap: SparkSubmitOperator against the managed spark service, or
KubernetesPodOperator against the cluster - the job jars are identical.

## Oozie → Airflow mapping (implemented)

| Oozie (legacy) | Airflow (this repo) |
|---|---|
| `coordinator.xml` - daily frequency, gated on the feed dataset | `banking_eod` schedule `@daily` + the `wait_for_feeds` sensor task |
| `workflow.xml` fs → hive → shell actions | `ingest` → `curate` → `detect` tasks (spark-submit via container) |
| EL parameters `${jobDate}` | Jinja `{{ ds }}` - the logical date each run processes |
| error → kill node | `retries` + the dq/reconciliation verdict gates (`AirflowFailException`) |
| bundle (many coordinators) | two dags on one project board |

The full migration narrative (inventory → storage move → logic port → parallel run →
orchestration re-platform → compute move → cutover) lives in `docs/migration-guide.md`.
