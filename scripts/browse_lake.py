#!/usr/bin/env python3
"""Browse the lake over the S3 API, without any UI.

usage:
  python scripts/browse_lake.py buckets             # list all buckets
  python scripts/browse_lake.py ls [prefix]         # list a folder (default: lake-curated/)
  python scripts/browse_lake.py cat KEY             # print a small text object
                                                    # (e.g. the data-quality report)

examples:
  python scripts/browse_lake.py ls
  python scripts/browse_lake.py ls transactions/
  python scripts/browse_lake.py ls reports/dq/
  python scripts/browse_lake.py cat reports/dq/part-00000-....json
"""

import argparse
import os
import sys

import boto3
from botocore.client import Config as BotoConfig

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:8333")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
DEFAULT_BUCKET = os.getenv("S3_BUCKET", "lake-curated")


def client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_buckets(s3):
    for b in sorted(s3.list_buckets().get("Buckets", []), key=lambda x: x["Name"]):
        print(b["Name"])


def ls(s3, bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    for p in resp.get("CommonPrefixes", []):
        print(f"dir  {p['Prefix']}")
    for o in sorted(resp.get("Contents", []), key=lambda x: x["Key"]):
        print(f"{o['Size']:>10}  {o['Key']}")


def cat(s3, bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        print(body.decode("utf-8"))
    except UnicodeDecodeError:
        sys.exit(f"'{key}' is binary (parquet) - download it and read it with pandas/pyarrow")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["buckets", "ls", "cat"])
    parser.add_argument("target", nargs="?", default="")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    args = parser.parse_args()

    s3 = client()

    if args.command == "buckets":
        list_buckets(s3)
    elif args.command == "ls":
        ls(s3, args.bucket, args.target)
    elif args.command == "cat":
        if not args.target:
            sys.exit("cat needs a key, e.g. reports/dq/part-....json")
        cat(s3, args.bucket, args.target)


if __name__ == "__main__":
    main()
