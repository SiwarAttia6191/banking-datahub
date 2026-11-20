#!/usr/bin/env python3
"""Peek inside parquet objects in the lake, straight over the S3 API.

Uses duckdb + httpfs: no download step - sql runs directly against the
S3-compatible endpoint. Partition columns live in the folder name, not in
the files, so each row shows the path's business_date separately.

usage:
  python scripts/peek_parquet.py curated/transactions/business_date=2026-11-17 [ROWS]
  python scripts/peek_parquet.py curated/alerts/business_date=2026-11-17 [ROWS]
  # another bucket (e.g. the raw zone): override S3_BUCKET
  S3_BUCKET=lake-raw python scripts/peek_parquet.py raw/transactions/ingestion_date=2026-11-20 [ROWS]
"""

import os
import sys

import duckdb

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:8333")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("S3_BUCKET", "lake-curated")


def main() -> None:
    prefix = sys.argv[1] if len(sys.argv) > 1 else "curated/transactions/business_date=2026-11-17"
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    prefix = prefix.rstrip("/")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{ENDPOINT.split('//')[1]}'")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_use_ssl=false")
    con.execute(f"SET s3_access_key_id='{ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{SECRET_KEY}'")

    path = f"s3://{BUCKET}/{prefix}/*.parquet"
    count = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
    print(f"{count} rows in {path}\n")

    df = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT {rows}").fetchdf()
    print(df.to_string())


if __name__ == "__main__":
    main()
