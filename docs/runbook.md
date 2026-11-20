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
docker logs banking-datahub-airflow 2>&1 | grep -i password   # the admin password
# open http://localhost:8080 - log in admin / <that password>
```

Two dags: `banking_eod` (sensor → generate → ingest → curate → features → detect →
**dq gate** → publish - publish is *blocked* unless the verdict is PASS) and
`reconciliation_chain` (legacy baseline → reconcile → verdict gate). Trigger from the
UI (play button → trigger) and watch the grid view turn green task by task; every
task's log is one click away. This is the same chain as Way A plus the enforcement:
the gate that "Way A" only prints is here a real barrier.

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
es 9200 · kibana 5601 · airflow 8080). Credentials live in the gitignored `.env`
(elastic/datahub-dev for kibana and es, datahub/datahub for postgres, dremio admin
created at first run). Never commit or paste them.
