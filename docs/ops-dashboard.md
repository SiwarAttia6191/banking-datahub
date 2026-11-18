# The ops dashboard: kibana over the serving layer

The postgres serving layer holds the small tables the business looks at every
morning. Kibana is the dashboard tool on top of them: `scripts/publish_to_es.py`
indexes the three serving sources into Elasticsearch once, registers the data
views, and everything after that is kibana panels.

This is deliberately a one-off index load, not a second pipeline - the lake and
the postgres serving layer stay the sources of truth.

## Security (dev grade, per the stack-security guide)

Authentication is **on**: elasticsearch runs with `xpack.security.enabled=true`
(transport tls auto-configured, http kept plain for the laptop) and kibana logs
into it with credentials. The browser session asks you to log in.

| credential | value | where |
|---|---|---|
| elastic superuser (your browser login) | `elastic` / `datahub-dev` | docker-compose env (`ELASTIC_PASSWORD`) |
| kibana's server-to-server account | `kibana_system` / `datahub-dev` | built-in service user; password set once via `POST /_security/user/kibana_system/_password` (8.x forbids `elastic` as kibana's own account) |
| publisher auth | `elastic` / `datahub-dev` | `scripts/publish_to_es.py` envs (`ES_USER`/`ES_PASSWORD`) |

The dev password and the static kibana encryption keys live in the compose file
**on purpose**: this stack never leaves the laptop, and a demo stack nobody can
log into demos nothing. Anything real needs real secrets in a secrets manager -
the wiring (auth on, least-privilege roles per consumer) would not change.

Machine-specific values live in a gitignored `.env` at the repo root (compose
reads it automatically; the compose defaults are `${VAR:-default}` fallbacks -
see `.env.example` for the shape). The `.env` itself is never committed.

## Sources

| index | from | granularity |
|---|---|---|
| `aml_alerts` | postgres `serving.aml_alerts` | one doc per alert (`_id` = alert_id) |
| `daily_volumes` | postgres `serving.daily_volumes` | one doc per account-direction-day |
| `risk_ratings` | lake `curated/risk_ratings` parquet (duckdb over the s3 api, no copy) | one doc per account-day |

Alerts and volumes come from postgres because they are the published consumption
copies. Risk ratings are read straight from the curated parquet on the object
store - the same "bi on the lake" pattern as dremio, just programmatic.

## Run it

```bash
docker compose up -d elasticsearch kibana   # ui: http://localhost:5601 (log in: elastic / datahub-dev)
python scripts/publish_to_es.py             # add --dry-run to see the counts only
```

The script is idempotent: docs carry their business key as `_id`, so re-running
overwrites instead of duplicating - same rule as the serving publication itself.
(The indices show yellow health on a single-node dev cluster - that is just the
unassigned replica shard, there is no second node to hold it.)

## What to look at (morning check)

- **Discover on `aml_alerts`** - filter `severity: critical|high`; count by `rule_id`
  shows which detection rule fired overnight.
- **Discover on `risk_ratings`** - `final_rating` distribution; any account that moved
  from `low` to `high` between days is the first thing to explain.
- **Lens on `daily_volumes`** - `day_sum_eur` by `business_date`, split by `direction`;
  the rolling window columns (`sum_7d_eur`, `count_7d`) are already indexed.
- **Data views** are created automatically (`aml-alerts`, `daily-volumes`,
  `risk-ratings`, time field `business_date`).

Dashboards themselves are built by hand in the kibana UI (lens + tagcloud of the
rule ids) - the indexing and the data views are the automated part.

## Environment

| variable | default | what it is |
|---|---|---|
| `ES_URL` | `http://localhost:9200` | elasticsearch api |
| `ES_USER` / `ES_PASSWORD` | `elastic` / `datahub-dev` | elasticsearch basic auth |
| `KIBANA_URL` | `http://localhost:5601` | kibana api (data views) |
| `KIBANA_USER` / `KIBANA_PASSWORD` | `elastic` / `datahub-dev` | kibana basic auth |
| `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` | localhost / 5436 / datahub / datahub | serving postgres |
| `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` | local s3 store, dev keys | risk_ratings read |

Dependencies: `pip install psycopg2-binary duckdb` (see `scripts/requirements.txt`).
