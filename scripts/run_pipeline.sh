#!/usr/bin/env bash
set -euo pipefail

# End-to-end local run:
# generate synthetic data -> create buckets -> upload feeds to the inbox ->
# build the three job jars -> submit the six jobs in order:
#   ingest (backlog: all dates) -> curate (all dates) ->
#   features -> detect -> data quality (for the requested date)
#   -> publish to the postgres serving layer (after the dq gate)
#
# Spark jobs run as one-off containers on the compose network (apache/spark image,
# local[*] mode); the same jars go to a cluster unchanged.
#
# usage: ./scripts/run_pipeline.sh [yyyy-MM-dd]

DATE=${1:-$(date +%F)}

# sbt command override: Windows installs expose sbt.bat, which Git Bash does not
# resolve by bare name - run with SBT_CMD="/c/Program Files (x86)/sbt/bin/sbt.bat"
SBT_CMD=${SBT_CMD:-sbt}

python scripts/generate_data.py
python scripts/bootstrap_buckets.py
python scripts/upload_sources.py

"$SBT_CMD" "ingestion/assembly" "transform/assembly" "quality/assembly"

# project dir as a windows-style path for the bind mount
PROJDIR=$(pwd -W 2>/dev/null || pwd)

submit() {
  # S3A_ENDPOINT points the job at the s3 service inside the compose network instead
  # of localhost. --master local[*] keeps everything in one jvm; switch to
  # spark://host:7077 when running against a cluster.
  MSYS_NO_PATHCONV=1 docker run --rm \
    --network banking-datahub_default \
    -v "$PROJDIR:/project" -w /project \
    -e S3A_ENDPOINT=http://s3:8333 \
    apache/spark:3.5.3 \
    /opt/spark/bin/spark-submit \
    --master local[*] \
    --class "$1" \
    "/project/$2" \
    --date "$3"
}

submit io.github.siwarattia.datahub.ingestion.RawIngestionJob \
  modules/ingestion/target/scala-2.12/datahub-ingestion-assembly-0.1.0.jar all

submit io.github.siwarattia.datahub.transform.CuratedTransactionsJob \
  modules/transform/target/scala-2.12/datahub-transform-assembly-0.1.0.jar all

submit io.github.siwarattia.datahub.transform.AccountFeaturesJob \
  modules/transform/target/scala-2.12/datahub-transform-assembly-0.1.0.jar "$DATE"

submit io.github.siwarattia.datahub.transform.DetectionJob \
  modules/transform/target/scala-2.12/datahub-transform-assembly-0.1.0.jar "$DATE"

submit io.github.siwarattia.datahub.quality.DataQualityJob \
  modules/quality/target/scala-2.12/datahub-quality-assembly-0.1.0.jar "$DATE"

submit io.github.siwarattia.datahub.transform.ServingJob \
  modules/transform/target/scala-2.12/datahub-transform-assembly-0.1.0.jar "$DATE"

echo "pipeline done for $DATE"
echo "  lake-raw/raw/    -> landed feeds (parquet)"
echo "  lake-curated/    -> transactions, rejects, accounts, clients, features, alerts, risk_ratings"
echo "  reports/dq       -> the go/no-go data-quality report (check the verdict)"
echo "  serving (psql)   -> daily_volumes, aml_alerts (postgres :5436, schema serving)"
