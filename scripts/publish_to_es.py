#!/usr/bin/env python3
"""Index the serving layer into Elasticsearch for the kibana ops dashboard.

Three indexes, rebuilt idempotently (each doc carries its business key as _id,
so re-running a day overwrites instead of duplicating):

  aml_alerts     <- postgres serving.aml_alerts   (_id = alert_id)
  daily_volumes  <- postgres serving.daily_volumes (_id = date|account|direction)
  risk_ratings   <- lake curated/risk_ratings parquet, read in place with duckdb
                    over the s3 api (_id = date|account)

It also registers the kibana data views, so the dashboard has its sources
right after the first run.

usage:
  python scripts/publish_to_es.py            # index everything + create data views
  python scripts/publish_to_es.py --dry-run  # counts only, no writes
"""

import base64
import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request
from decimal import Decimal

import duckdb
import psycopg2

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
KIBANA_URL = os.getenv("KIBANA_URL", "http://localhost:5601")
# stack security is ON (see docker-compose.yml): elastic / datahub-dev, dev grade
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "datahub-dev")
KIBANA_USER = os.getenv("KIBANA_USER", "elastic")
KIBANA_PASSWORD = os.getenv("KIBANA_PASSWORD", "datahub-dev")

PG = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5436")),
    dbname=os.getenv("PGDATABASE", "datahub"),
    user=os.getenv("PGUSER", "datahub"),
    password=os.getenv("PGPASSWORD", "datahub"),
)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:8333")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "dev-access-key")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "dev-secret-key")
S3_BUCKET = os.getenv("S3_BUCKET", "lake-curated")

# time-series fields: kibana needs real date types on both sides
MAPPINGS = {
    "aml_alerts": {"business_date": "date", "published_at": "date"},
    "daily_volumes": {"business_date": "date", "published_at": "date"},
    "risk_ratings": {"business_date": "date"},
}

DATA_VIEWS = {
    "aml-alerts": ("aml_alerts", "business_date"),
    "daily-volumes": ("daily_volumes", "business_date"),
    "risk-ratings": ("risk_ratings", "business_date"),
}


def basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def http(method: str, url: str, body: dict | str | None = None,
         content_type: str = "application/json", expect: tuple = (200, 201),
         auth: str | None = None) -> dict:
    data = None
    if body is not None:
        data = (body if isinstance(body, str) else json.dumps(body)).encode()
    headers = {"Content-Type": content_type}
    if auth:
        headers["Authorization"] = auth
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        payload = e.read().decode()[:200]
        if e.code == 400 and "already_exists" in payload:
            return {}
        raise RuntimeError(f"{method} {url} -> {e.code}: {payload}")


def wait_for(url: str, name: str, tries: int = 60) -> None:
    """Elasticsearch and kibana take a minute or two to boot after compose up;
    a 401 also means the service is up (it answered, with security on)."""
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                if r.status == 200:
                    print(f"{name} is up ({url})")
                    return
        except urllib.error.HTTPError as e:
            if e.code == 401:
                print(f"{name} is up ({url}, security on)")
                return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(2)
    raise RuntimeError(f"{name} did not come up at {url} - check the container logs")


def jsonable(v):
    """Dates as iso strings (elasticsearch's strict parser wants the T separator),
    decimals as floats."""
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def ensure_index(index: str) -> None:
    fields = ", ".join(f'"{k}": {{"type": "{v}"}}' for k, v in MAPPINGS[index].items())
    http("PUT", f"{ES_URL}/{index}",
         {"mappings": {"properties": json.loads("{" + fields + "}")}},
         auth=basic(ES_USER, ES_PASSWORD))


def bulk_index(index: str, docs: list[dict]) -> int:
    if not docs:
        return 0
    lines = []
    for doc in docs:
        meta = json.dumps({"index": {"_index": index, "_id": doc.pop("_id")}})
        lines.append(meta)
        lines.append(json.dumps(doc, default=str))
    body = "\n".join(lines) + "\n"
    res = http("POST", f"{ES_URL}/_bulk?refresh=true", body,
               content_type="application/x-ndjson", auth=basic(ES_USER, ES_PASSWORD))
    errors = [i for i, item in enumerate(res.get("items", [])) if item.get("index", {}).get("error")]
    if errors:
        raise RuntimeError(f"{index}: {len(errors)} bulk items failed: {res['items'][errors[0]]}")
    return len(docs)


def fetch_pg(conn, table: str, key_fn) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        "select column_name from information_schema.columns "
        "where table_schema='serving' and table_name=%s order by ordinal_position", (table,))
    cols = [r[0] for r in cur.fetchall()]
    cur.execute(f"select * from serving.{table}")
    out = []
    for row in cur.fetchall():
        doc = {k: jsonable(v) for k, v in zip(cols, row)}
        doc["_id"] = key_fn(doc)
        out.append(doc)
    cur.close()
    return out


def fetch_risk_ratings() -> list[dict]:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT.split('//')[1]}'")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_use_ssl=false")
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}'")
    path = f"s3://{S3_BUCKET}/curated/risk_ratings/*/*.parquet"
    cur = con.execute(f"SELECT * FROM read_parquet('{path}')")
    cols = [d[0] for d in cur.description]
    rows = [{k: jsonable(v) for k, v in zip(cols, row)} for row in cur.fetchall()]
    for doc in rows:
        doc["_id"] = f"{doc['business_date']}|{doc['account_id']}"
    return rows


def create_data_views() -> None:
    for view_id, (index, time_field) in DATA_VIEWS.items():
        req = urllib.request.Request(
            f"{KIBANA_URL}/api/saved_objects/index-pattern/{view_id}?overwrite=true",
            data=json.dumps({"attributes": {"title": index,
                                            "timeFieldName": time_field}}).encode(),
            method="POST",
            headers={"Content-Type": "application/json", "kbn-xsrf": "true",
                     "Authorization": basic(KIBANA_USER, KIBANA_PASSWORD)})
        with urllib.request.urlopen(req, timeout=60) as r:
            r.read()
        print(f"  data view '{view_id}' -> {index} (time field: {time_field})")


def main() -> None:
    dry = "--dry-run" in sys.argv
    conn = psycopg2.connect(**PG)

    sources = {
        "aml_alerts": fetch_pg(conn, "aml_alerts", lambda d: d["alert_id"]),
        "daily_volumes": fetch_pg(
            conn, "daily_volumes",
            lambda d: f"{d['business_date']}|{d['account_id']}|{d['direction']}"),
        "risk_ratings": fetch_risk_ratings(),
    }
    conn.close()

    for name, docs in sources.items():
        print(f"{name}: {len(docs)} docs")

    if dry:
        return

    wait_for(f"{ES_URL}/", "elasticsearch")
    wait_for(f"{KIBANA_URL}/api/features", "kibana")

    for name, docs in sources.items():
        ensure_index(name)
        n = bulk_index(name, docs)
        print(f"  indexed {n} docs into {name}")

    create_data_views()
    print("kibana ui: http://localhost:5601 -> Discover -> pick a data view")


if __name__ == "__main__":
    main()
