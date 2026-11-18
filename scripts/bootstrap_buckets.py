#!/usr/bin/env python3
"""Create the lake-raw and lake-curated buckets on the local S3-compatible endpoint
(SeaweedFS in docker-compose).

Dev credentials default to dev-access-key/dev-secret-key (accepted by the local store);
override with S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY env vars.
"""

import os
import sys

try:
    import boto3
    from botocore.client import Config as BotoConfig
except ImportError:
    sys.exit("boto3 is required: pip install -r scripts/requirements.txt")

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:8333")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "dev-access-key")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "dev-secret-key")

BUCKETS = ["lake-raw", "lake-curated", "lake-legacy"]


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )

    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for bucket in BUCKETS:
        if bucket in existing:
            print(f"bucket exists: {bucket}")
        else:
            s3.create_bucket(Bucket=bucket)
            print(f"bucket created: {bucket}")

    print("lake layout:")
    print("  lake-raw/raw/{domain}/ingestion_date=YYYY-MM-DD/     (as landed, immutable)")
    print("  lake-curated/curated/{domain}/business_date=YYYY-MM-DD/ (validated, conformed)")
    print("  lake-legacy/cur_transactions/business_date=YYYY-MM-DD/ (baseline job output)")


if __name__ == "__main__":
    main()
