"""reconciliation_chain: the parallel-run validation, orchestrated.

Chained after the end-of-day dag: the legacy baseline (the spark sql port of the
hive script) runs against the same inputs, the reconciliation job compares the two
outputs per partition, and the verdict gate FAILS the dag unless the legacy output
and the curated layer agree - strict key equivalence, the parallel-validation
pattern from production, as a real go/no-go gate.
"""

from __future__ import annotations

import datetime as dt

import airflow  # noqa: F401
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

NETWORK = "banking-datahub_default"
SPARK_IMAGE = "apache/spark:3.5.3"
BASELINE_JAR = "modules/baseline/target/scala-2.12/datahub-baseline-assembly-0.1.0.jar"
BASELINE_CLASS = "io.github.siwarattia.datahub.baseline"


def submit(module: str, date: str) -> str:
    cls = {"baseline": f"{BASELINE_CLASS}.BaselineJob",
           "reconcile": f"{BASELINE_CLASS}.ReconciliationJob"}[module]
    return (
        f"/opt/spark/bin/spark-submit --master local[*] --class {cls} "
        f'/project/{BASELINE_JAR} --date {date}'
    )


@dag(
    dag_id="reconciliation_chain",
    description="Legacy baseline vs curated layer, per-partition go/no-go (parallel validation)",
    start_date=dt.datetime(2026, 11, 17),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=2)},
    doc_md=__doc__,
    tags=["banking", "reconciliation", "migration"],
)
def reconciliation_chain():

    @task(retries=0)
    def legacy_baseline(ds: str | None = None):
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", "/project:/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", f"{BASELINE_CLASS}.BaselineJob",
             f"/project/{BASELINE_JAR}", "--date", ds],
            check=True, cwd="/project")

    @task(retries=0)
    def reconcile(ds: str | None = None) -> str:
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", "/project:/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", f"{BASELINE_CLASS}.ReconciliationJob",
             f"/project/{BASELINE_JAR}", "--date", ds],
            check=True, cwd="/project")
        # read the verdict from the report - the go/no-go gate
        import boto3, json
        from botocore.client import Config
        s3 = boto3.client("s3", endpoint_url="http://s3:8333",
                          aws_access_key_id="dev-access-key",
                          aws_secret_access_key="dev-secret-key",
                          config=Config(signature_version="s3v4"), region_name="us-east-1")
        keys = s3.list_objects_v2(Bucket="lake-curated", Prefix="reports/reconciliation/")
        newest = max((k["Key"] for k in keys.get("Contents", []) if k["Key"].endswith(".json")),
                     default=None)
        if newest is None:
            raise AirflowFailException("no reconciliation report found")
        body = s3.get_object(Bucket="lake-curated", Key=newest)["Body"].read().decode()
        verdict = json.loads(body).get("verdict")
        if verdict != "PASS":
            raise AirflowFailException(f"reconciliation verdict {verdict} - cutover blocked")
        return f"reconciliation verdict {verdict}"

    legacy_baseline() >> reconcile()


reconciliation_chain()
