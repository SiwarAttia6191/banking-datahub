#!/usr/bin/env python3
"""Upload the generated source files to the raw inbox on the local s3 store.

The ingestion job reads exclusively from object storage, never from the local
disk - mirroring how bank feeds land on a cloud inbox before processing.
The inbox mirrors the data/output layout:

  inbox/clients/clients.csv
  inbox/accounts/accounts.csv
  inbox/restricted_countries/restricted_countries.csv
  inbox/transactions/business_date=YYYY-MM-DD/transactions.csv
  inbox/fx_rates/business_date=YYYY-MM-DD/fx_rates.csv
"""

import os
import sys
from pathlib import Path

try:
    import boto3
    from botocore.client import Config as BotoConfig
except ImportError:
    sys.exit("boto3 is required: pip install -r scripts/requirements.txt")

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:8333")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "dev-access-key")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "dev-secret-key")
BUCKET = os.getenv("S3A_RAW_BUCKET", "lake-raw")
SOURCE_ROOT = Path("data/output")


def main() -> None:
    if not SOURCE_ROOT.exists():
        sys.exit("no data found: run scripts/generate_data.py first")

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )

    count = 0
    for path in sorted(SOURCE_ROOT.rglob("*.csv")):
        key = f"inbox/{path.relative_to(SOURCE_ROOT).as_posix()}"
        s3.upload_file(str(path), BUCKET, key)
        print(f"uploaded {key}")
        count += 1

    print(f"done: {count} files -> s3://{BUCKET}/inbox/")


if __name__ == "__main__":
    main()
