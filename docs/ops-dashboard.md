# The ops dashboard: kibana over the serving layer

The postgres serving layer holds the small tables the business looks at every
morning. Kibana is the dashboard tool on top of them: `scripts/publish_to_es.py`
indexes the three serving sources into Elasticsearch once, registers the data
views, and everything after that is kibana panels.

This is deliberately a one-off index load, not a second pipeline - the lake and
the postgres serving layer stay the sources of truth.

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
docker compose up -d elasticsearch kibana   # ui: http://localhost:5601
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
| `KIBANA_URL` | `http://localhost:5601` | kibana api (data views) |
| `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` | localhost / 5436 / datahub / datahub | serving postgres |
| `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` | local s3 store, dev keys | risk_ratings read |

Dependencies: `pip install psycopg2-binary duckdb` (see `scripts/requirements.txt`).
