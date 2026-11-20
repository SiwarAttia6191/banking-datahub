# Browsing the lake: four verified ways

The lake lives on an S3-compatible object store (SeaweedFS) at
`http://localhost:8333`, with three buckets: `lake-raw` (as-landed feeds),
`lake-curated` (typed, cleaned) and `lake-legacy` (the legacy-style baseline
output). This page lists the four ways to look inside - all verified working on
the reference machine. Prerequisite for all of them: the stack is up
(`docker compose up -d`); everything else here needs no spark-submit.

Which one when:

| you want | use |
|---|---|
| file listings, sizes, download a file | AWS CLI (method 1) |
| see actual ROWS of a parquet file, no download | peek (method 2) |
| quick bucket/folder listing + read a small text file (dq report) | browse_lake (method 3) |
| point-and-click browsing | master UI or WinSCP (method 4) |

## 1. AWS CLI with a local profile

The `aws` command reads the lake through an AWS CLI profile that points at the
local endpoint. Set it up once per machine:

```bash
aws configure set region us-east-1 --profile lake-local
aws configure set aws_access_key_id dev-access-key --profile lake-local
aws configure set aws_secret_access_key dev-secret-key --profile lake-local
aws configure set endpoint_url http://localhost:8333 --profile lake-local
```

(SeaweedFS dev mode accepts the dummy keys; a production store would enforce
real IAM-style credentials and TLS (transport layer security).)

Verified commands:

```bash
aws --profile lake-local s3 ls s3://lake-raw/                                # inbox/ + raw/
aws --profile lake-local s3 ls s3://lake-raw/raw/transactions/               # one folder per ingestion day
aws --profile lake-local s3 ls --recursive --summarize s3://lake-raw/raw/ | tail -5   # every file + total size
aws --profile lake-local s3 cp s3://lake-raw/raw/transactions/ingestion_date=2026-11-20/<file> .   # download one
```

Parquet files are binary - the CLI can list and copy them, but to SEE the rows
use method 2.

## 2. peek - read parquet rows in place (duckdb, no download)

`scripts/peek_parquet.py` opens parquet (the columnar file format the pipeline
writes) straight over the S3 API with duckdb (an embedded sql engine) and prints
rows as a table. Default bucket is `lake-curated`; the raw zone needs the
`S3_BUCKET` override:

```bash
# curated layer (default bucket):
.venv/Scripts/python.exe scripts/peek_parquet.py curated/transactions/business_date=2026-11-20 5
# raw zone (override the bucket):
S3_BUCKET=lake-raw .venv/Scripts/python.exe scripts/peek_parquet.py raw/transactions/ingestion_date=2026-11-20 5
```

Notes:
- run it with the project venv (`.venv/Scripts/python.exe`): plain python usually
  lacks numpy/pandas, which the table printing needs
- partition columns (`business_date`, `ingestion_date`) live in the FOLDER NAME,
  not inside the files - each row shows the path's date separately
- the raw zone shows all-string columns, exactly as the feeds landed

## 3. browse_lake - listings and small text objects

`scripts/browse_lake.py` lists buckets/folders and prints small text objects
(the data-quality report is the classic one):

```bash
python scripts/browse_lake.py buckets
python scripts/browse_lake.py ls lake-raw/raw/transactions/
python scripts/browse_lake.py cat lake-curated/reports/dq/<the-newest-json>
```

## 4. GUI: master UI and WinSCP

- **SeaweedFS master UI**: <http://localhost:9333> - volumes and buckets at a glance.
- **WinSCP** (S3-capable file-transfer GUI): session = File protocol `S3`,
  host `localhost`, port `8333`, access key `dev-access-key`,
  secret key `dev-secret-key`. Two settings are mandatory:
  - Advanced -> Connection -> Encryption: **No encryption** - port 8333 speaks
    plain HTTP locally; TLS would produce "ssl error: wrong version number"
  - Advanced -> S3: **Use path style requests** - virtual-host style builds
    `bucket.localhost` URLs that do not resolve on a laptop

## Troubleshooting

| symptom | cause and fix |
|---|---|
| connection refused (all methods) | the containers are down: `docker compose up -d` |
| WinSCP: ssl error wrong version number | encryption set to TLS on an HTTP-only port: set "No encryption" |
| WinSCP: bucket not found / wrong host | enable path-style requests |
| peek: No module named numpy | use the project venv python (method 2) |
| peek: No files found that match | wrong bucket - remember `S3_BUCKET=lake-raw` for the raw zone |
