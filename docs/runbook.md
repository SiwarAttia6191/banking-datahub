# Runbook: operating the pipeline day to day

How YOU run, watch, verify and repair the hub from your own terminal - no helpers,
no hidden tooling. Git Bash on Windows; every command below was run against the
live stack. This is the answer to "how would you monitor the DataHub each morning?"

## 0. The morning check (10 minutes)

Work down the list; each step has a green state:

```bash
docker ps --format "{{.Names}} {{.Status}}" | grep banking
# green: s3, postgres, dremio, elasticsearch, kibana (and airflow, when running) all "Up"

curl -s -u elastic:datahub-dev http://localhost:9200/_cat/indices/aml_alerts,daily_volumes,risk_ratings?v&h=index,docs.count
# green: three indices, doc counts moving with each processed day

# the orchestrator's view of last night (when airflow is running):
curl -s "http://localhost:8080/api/v1/dags" -u admin:<password-from-logs> | head -c 300
```

Then the human questions: did the dq (data-quality) verdict PASS? how many alerts
fired? any rejects spike? - see step 3 for where each answer lives.

**Every credential lives in the gitignored `.env` at the repo root** - the table in
its header maps each tool to its url, user and password. One file, never a hunt.

## 1. Start the stack

```bash
docker compose up -d                          # s3 + postgres + dremio + elasticsearch + kibana
docker ps --format "{{.Names}} {{.Status}}"   # verify all five Up
```

## 2. Run the pipeline - two ways

### Way A: the runner script (one command, verbose console output)

```bash
./scripts/run_pipeline.sh                     # everything for today's date
./scripts/run_pipeline.sh 2026-11-20          # a specific day
```

The console prints each spark job's logs directly (that IS your log feed - scroll it,
grep it, or capture it: `./scripts/run_pipeline.sh > /tmp/eod.log 2>&1; tail -50 /tmp/eod.log`).
The important lines look like:

```
INFO CuratedTransactionsJob$: [all] curated: 14042, rejects: 283
INFO DetectionJob$: [2026-11-20] alerts: 14, risk ratings: 12
INFO DataQualityJob$: [2026-11-20] dq verdict: PASS
INFO ServingJob$: [2026-11-20] serving published: 125 daily_volumes, 14 aml_alerts
```

### Way B: Airflow (the orchestrator - scheduled, gated, retry-able)

```bash
HOST_PROJECT_DIR="$(pwd -W)" docker compose -f docker/airflow-compose.yml up -d
# open http://localhost:8080 - log in admin / AIRFLOW_ADMIN_PASSWORD (from .env)
```

Two dags: `banking_eod` (sensor → generate → ingest → curate → features → detect →
**dq gate** → publish - publish is *blocked* unless the verdict is PASS) and
`reconciliation_chain` (legacy baseline → reconcile → verdict gate). Trigger from the
UI (play button → trigger) and watch the grid view turn green task by task; every
task's log is one click away. This is the same chain as Way A plus the enforcement:
the gate that "Way A" only prints is here a real barrier.

The executor is LocalExecutor over a postgres metadata db (`airflow_metadata` in the
serving container) - the production-shaped configuration; airflow banners the demo
default (SequentialExecutor + sqlite) in the ui, and the ladder above it is
CeleryExecutor (worker nodes over a broker) then KubernetesExecutor (one pod per task,
elastic - the natural fit once the spark jobs move to the k8s manifests). The compute
sits outside airflow either way: the executor only orchestrates, spark runs in
one-off containers.

Why both ways exist: Way A is what you use to debug (verbose, immediate); Way B is
what production looks like (scheduled, gated, retried, auditable).

## 3. Verify each layer after a run

| layer | command | green looks like |
|---|---|---|
| raw landing | `python scripts/peek_parquet.py raw/transactions/ingestion_date=2026-11-20 3` | strings, exactly as fed |
| curated | `python scripts/peek_parquet.py curated/transactions/business_date=2026-11-20 3` | typed columns incl. `amount_eur`, `counterparty_country_name` |
| dq report | `aws --profile lake-local s3 ls s3://lake-curated/reports/dq/` then read the newest json | `"verdict":"PASS"` |
| alerts | `python scripts/peek_parquet.py curated/alerts/business_date=2026-11-20` | rule ids + details + scores |
| serving | `docker exec banking-datahub-postgres-1 psql -U datahub -d datahub -c "select business_date, count(*) from serving.daily_volumes group by 1"` | the processed days |
| dashboards | http://localhost:5601 → Discover → data view, time range **Last 7 days** | the new day's rows |
| whole lake | `python scripts/browse_lake.py` | every bucket/partition |

## 4. Republish / refresh the dashboards

```bash
python scripts/publish_to_es.py               # idempotent: business keys are the _ids
python scripts/publish_to_es.py --dry-run     # counts only
```

## 4b. The migration: the same task, before (Oozie) and after (Airflow)

The daily load existed BEFORE Airflow - as an Oozie (the Hadoop-era scheduler) workflow,
kept verbatim in `migration/legacy/` as the migration reference. Same task, two eras:

| concern | BEFORE - oozie (`migration/legacy/`) | AFTER - airflow (`airflow/dags/banking_eod_dag.py`) |
|---|---|---|
| schedule once a day | `coordinator.xml`: `<coordinator-app frequency="${coordMinutes}">` + timezone | the `@dag(schedule="@daily")` decorator |
| wait for the feed | `<datasets><dataset name="txn_feed">` - dataset availability gates the run | the `wait_for_feeds` task - polls the s3 inbox, fails after 10 minutes |
| create the staging folder | `<action name="prepare-staging"><fs><mkdir .../>` | gone - object storage needs no mkdir (s3 has no real folders) |
| run the load logic | `<action name="load-transactions"><hive><script>daily_load.hql</script><param>jobDate=${jobDate}</param>` | `ingest`/`curate` tasks: spark-submit of the ported jar, `--date` from `ds` |
| pass the business date | EL expression `${jobDate}` filled by the coordinator | Jinja context variable `{{ ds }}` (TaskFlow injects it as the `ds` parameter) |
| a step fails | `<error to="kill"/>` - the workflow dies, no retry | `default_args={"retries": 2}` + the dq gate raising `AirflowFailException` |
| see what ran | oozie web ui on the hadoop cluster | the airflow ui: grid view, per-task logs, one click each |
| prove equivalence before cutover | - | `reconciliation_chain` dag: the legacy hive logic ported to spark (`BaselineJob`) vs the new pipeline, per-partition diff, verdict gate |

Where to look, in reading order:
1. `migration/legacy/oozie/workflow.xml` + `coordinator.xml` + `job.properties` - the
   "before", verbatim XML as it ran on Hadoop
2. `migration/legacy/hive/daily_load.hql` - the load logic that `CuratedTransactionsJob`
   is a line-by-line port of
3. `airflow/dags/banking_eod_dag.py` - the "after", the same chain in python
4. `docs/migration-guide.md` - the full study (inventory → storage move → logic port →
   parallel run → orchestration re-platform → cutover)
5. `docs/detection-rules.md` - what the ported pipeline now does that the legacy one didn't

Why there is no oozie image or container: oozie only runs on a hadoop cluster (it needs
hdfs, yarn and the mapreduce/hive2 services) - there is no realistic local oozie. The
"before" is represented by its real artifacts, the "after" by code you can actually run
and watch - which is exactly what a migration project hands you: the legacy artifacts
plus the ported implementation, with the reconciliation verdict as the equivalence proof.

## 4c. Where the daily data comes from (and what to prepare)

Nothing to prepare - the hub is self-feeding:

1. the scheduled dag run's logical date (`ds`) becomes the business date
2. `generate_and_upload` regenerates the synthetic feeds (45-day window ending today)
   and uploads them to the inbox - **in production this task disappears**: the bank's
   source systems push the day's extracts, and the `wait_for_feeds` sensor becomes the
   real gate
3. `ingest` lands them in the raw zone; every later step overwrites its own
   date partition - reruns are idempotent, so a daily rerun never duplicates anything

Two modes worth knowing:

- **bootstrap** (what happens today): the full 45-day backlog, so the statistical
  baselines have history from day one
- **incremental** (production-like): `python scripts/generate_data.py --days 1` produces
  only the new day; the lake keeps the history, so baselines keep working. The runner
  and the dag currently bootstrap every time (heavier but idempotent) - switching the
  dag's generate task to `--days 1` after the first bootstrap is the documented next step

What you must NOT count on: old days changing. In production, once a day is processed
it becomes history; only its reruns (same inputs) may touch it. If you change the
transformation code, downstream days must be reprocessed deliberately - that discipline
is a deliberate operation - see the dq incident story in the git history
(commits around 2026-11-19/20: the reference-feed fan-out fix).

## 4d. The kubernetes path (issues #35/#36/#37)

The same pipeline, deployed as cluster pods. The cluster is **kind** (kubernetes in
docker - free): one container running a full single-node control plane.

**bring it up (once):**

```bash
kind create cluster --name banking-datahub         # or: kubectl get ns (already there?)
docker network connect banking-datahub_default banking-datahub-control-plane
# pods resolve s3 / postgres by name - this join is what connects the two worlds
docker build -f k8s/spark.Dockerfile -t banking-datahub/spark:3.5.3 .
kind load docker-image banking-datahub/spark:3.5.3 --name banking-datahub
kubectl apply -f k8s/                              # namespace + spark rbac + smoke pod
```

**run the pipeline on pods (without airflow):** apply the six jobs ONE AT A TIME -
six local[*] spark apps at once thrash a single node:

```bash
kubectl apply -f k8s/30-spark-jobs.yaml            # defines all six
kubectl wait --for=condition=complete job/banking-spark-ingest -n banking-datahub --timeout=15m
kubectl delete job banking-spark-ingest -n banking-datahub      # then the next...
```

**run it orchestrated (airflow):** trigger `banking_eod` from the ui - the spark
tasks are KubernetesPodOperator specs: airflow creates a pod per task, streams the
pod logs into its own task log, and deletes the pod after completion.

**watch:**

```bash
kubectl get pods,jobs -n banking-datahub
kubectl logs -n banking-datahub <pod-name>         # the output lives in the ...-driver pod
k9s -n banking-datahub                             # the terminal dashboard: :jobs, :pods, l = logs
```

**troubleshooting - each line is a real incident from the first runs:**

- `certificate verify failed: Hostname mismatch` on host.docker.internal → the
  kubeconfig must point at a name ON the api server certificate:
  `https://banking-datahub-control-plane:6443` (check with
  `openssl s_client ... | openssl x509 -noout -ext subjectAltName`)
- driver pod hung with 0% cpu, no logs, no s3 requests → silent s3a retries; take a
  thread dump (`kill -QUIT <pid>` inside the pod) and fast-fail with
  `fs.s3a.attempts.maximum=1` to surface the real error
- `Connection refused` to localhost:8333 → the submitter's env does not reach the
  driver pod: pass `--conf spark.kubernetes.driverEnv.S3A_ENDPOINT=...`
- `No FileSystem for scheme "local"` → missing `--class` on the spark-submit
- `Forbidden ... cannot patch resource "services"` → the rbac role needs the
  patch/update verbs (spark patches its driver service)
- the image must exist in the cluster: build + `kind load docker-image` after every
  jar change, and keep `image_pull_policy: IfNotPresent`

the learning doc (docs-local/learning-airflow-kubernetes.md) explains each of these
in depth; docs/kubernetes.md carries the architecture and the yaml walkthrough.

## 5. The incident loop (when the dq gate says FAIL)

1. Read the verdict json (step 3) - the failing checks say WHAT (`dedup_completeness:
   rows=614 distinct_txn_id=307` was the real 2026-11-19 incident: a re-ingest
   doubled the reference feeds and every join fanned out).
2. Count at the layer the check names - `peek_parquet.py` and duckdb over the lake
   (see docs/data-dictionary.md for what each column should hold).
3. Fix the cause (that one was: latest-version dedup on the reference feeds -
   commit history has it), then rerun **only the affected day(s)** - that is why
   everything is date-partitioned.
4. Delete the bad serving slice for that day (`psql ... delete from serving.aml_alerts
   where business_date='...'`), rerun the chain for the day, re-publish.
5. Keep the FAIL report - evidence of the gate working, next to the PASS that follows.

## 6. Case handoff and the data assistant

```bash
python scripts/alert_cases.py open            # critical/high alerts -> NEW cases
python scripts/alert_cases.py list --status NEW
python scripts/data_assistant.py "alerts by rule"
```

## 7. Ports and credentials

One distinct port per service (s3 8333 · master ui 9333 · postgres 5436 · dremio 9047 ·
es 9200 · kibana 5601 · airflow 8080). **Credentials live in the gitignored `.env`** -
its header table is the single reference (kibana/es, postgres, s3, dremio, airflow).
Never commit or paste them.
