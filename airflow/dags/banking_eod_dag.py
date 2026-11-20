"""banking_eod: the end-of-day chain, orchestrated.

The Airflow port of the legacy Oozie workflow (migration/legacy/oozie/workflow.xml):
wait for the day's feeds -> ingest -> curate -> features -> detect -> data quality.
Same order, same business intent; the mapping table lives in docs/migration-guide.md.

Laptop pattern: each task submits a spark job as a one-off container on the compose
network (the same commands scripts/run_pipeline.sh uses). Production swap: SparkSubmitOperator
against the managed spark service, or KubernetesPodOperator - the job jars are identical.
"""

from __future__ import annotations

import airflow   # noqa: F401  (ensures the airflow import succeeds first)
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
import datetime as dt

import os

NETWORK = "banking-datahub_default"
SPARK_IMAGE = "apache/spark:3.5.3"
# bind mounts are resolved by the docker daemon on the HOST, not inside this
# container - so the tasks need the repo's host path (compose passes it via
# HOST_PROJECT_DIR); /project stays the in-container working dir
HOST_PROJECT = os.environ.get("HOST_PROJECT_DIR", "/project")
JARS = {
    "ingestion": "modules/ingestion/target/scala-2.12/datahub-ingestion-assembly-0.1.0.jar",
    "transform": "modules/transform/target/scala-2.12/datahub-transform-assembly-0.1.0.jar",
    "quality": "modules/quality/target/scala-2.12/datahub-quality-assembly-0.1.0.jar",
    "baseline": "modules/baseline/target/scala-2.12/datahub-baseline-assembly-0.1.0.jar",
}
CLASSES = {
    "ingestion": "io.github.siwarattia.datahub.ingestion.RawIngestionJob",
    "curate": "io.github.siwarattia.datahub.transform.CuratedTransactionsJob",
    "features": "io.github.siwarattia.datahub.transform.AccountFeaturesJob",
    "detect": "io.github.siwarattia.datahub.transform.DetectionJob",
    "dq": "io.github.siwarattia.datahub.quality.DataQualityJob",
    "baseline": "io.github.siwarattia.datahub.baseline.BaselineJob",
    "reconcile": "io.github.siwarattia.datahub.baseline.ReconciliationJob",
    "publish": "io.github.siwarattia.datahub.transform.ServingJob",
}


def spark_submit(module: str, date: str) -> str:
    """The spark-submit command for one module - the same call the runner script makes."""
    return (
        f"/opt/spark/bin/spark-submit --master local[*] --class {CLASSES[module]} "
        f'/project/{JARS[module]} --date {date}'
    )


@dag(
    dag_id="banking_eod",
    description="End-of-day chain: feeds land -> raw -> curated -> features -> detection -> dq gate",
    start_date=dt.datetime(2026, 11, 17),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": dt.timedelta(minutes=2)},
    doc_md=__doc__,
    tags=["banking", "eod", "spark"],
)
def banking_eod():

    @task
    def wait_for_feeds(ds: str | None = None) -> str:
        """Oozie coordinator gate, ported: the day's feed must be in the inbox before
        anything runs. Polls the s3 api via the upload container's boto3."""
        import boto3
        from botocore.client import Config

        endpoint = "http://s3:8333"
        s3 = boto3.client("s3", endpoint_url=endpoint,
                          aws_access_key_id="dev-access-key",
                          aws_secret_access_key="dev-secret-key",
                          config=Config(signature_version="s3v4"), region_name="us-east-1")
        deadline = __import__("time").time() + 600
        while __import__("time").time() < deadline:
            keys = s3.list_objects_v2(Bucket="lake-raw",
                                      Prefix=f"inbox/transactions/business_date={ds}")
            if keys.get("KeyCount", 0) > 0:
                return f"inbox ready for {ds}"
            __import__("time").sleep(10)
        raise AirflowFailException(f"feed for {ds} never arrived within 10 minutes")

    @task(retries=0)
    def generate_and_upload() -> str:
        """The demo feeds are synthetic: generate the day and push it to the inbox.
        In production this task is replaced by the real feed arrival (the sensor above)."""
        import subprocess, os
        env = {**os.environ, "S3_ENDPOINT": "http://s3:8333"}
        for cmd in ["python scripts/generate_data.py", "python scripts/upload_sources.py"]:
            subprocess.run(cmd.split(), check=True, cwd="/project", env=env)
        return "feeds generated and uploaded"

    @task(retries=1)
    def ingest(ds: str | None = None):
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", HOST_PROJECT + ":/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", CLASSES["ingestion"],
             f"/project/{JARS['ingestion']}", "--date", ds],
            check=True, cwd="/project")

    @task(retries=1)
    def curate(ds: str | None = None):
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", HOST_PROJECT + ":/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", CLASSES["curate"],
             f"/project/{JARS['transform']}", "--date", ds],
            check=True, cwd="/project")

    @task(retries=1)
    def features(ds: str | None = None):
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", HOST_PROJECT + ":/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", CLASSES["features"],
             f"/project/{JARS['transform']}", "--date", ds],
            check=True, cwd="/project")

    @task(retries=1)
    def detect(ds: str | None = None):
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", HOST_PROJECT + ":/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", CLASSES["detect"],
             f"/project/{JARS['transform']}", "--date", ds],
            check=True, cwd="/project")

    @task(retries=1)
    def data_quality(ds: str | None = None) -> str:
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", HOST_PROJECT + ":/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", CLASSES["dq"],
             f"/project/{JARS['quality']}", "--date", ds],
            check=True, cwd="/project")
        # read the verdict from the report - the go/no-go gate
        import boto3, json
        from botocore.client import Config
        s3 = boto3.client("s3", endpoint_url="http://s3:8333",
                          aws_access_key_id="dev-access-key",
                          aws_secret_access_key="dev-secret-key",
                          config=Config(signature_version="s3v4"), region_name="us-east-1")
        keys = s3.list_objects_v2(Bucket="lake-curated", Prefix="reports/dq/")
        newest = max((k["Key"] for k in keys.get("Contents", []) if k["Key"].endswith(".json")),
                     default=None)
        if newest is None:
            raise AirflowFailException("no dq report found")
        body = s3.get_object(Bucket="lake-curated", Key=newest)["Body"].read().decode()
        verdict = json.loads(body).get("verdict")
        if verdict != "PASS":
            raise AirflowFailException(f"dq verdict {verdict} - publish blocked")
        return f"dq verdict {verdict}"


    @task(retries=1)
    def publish(ds: str | None = None):
        """The serving step: curated aggregates + alerts published to postgres - only
        runs when the dq gate passed."""
        import subprocess
        subprocess.run(
            ["docker", "run", "--rm", "--network", NETWORK,
             "-v", HOST_PROJECT + ":/project", "-w", "/project",
             "-e", "S3A_ENDPOINT=http://s3:8333",
             SPARK_IMAGE, "/opt/spark/bin/spark-submit",
             "--master", "local[*]", "--class", CLASSES["publish"],
             f"/project/{JARS['transform']}", "--date", ds],
            check=True, cwd="/project")
        return f"serving published for {ds}"

    # the tasks take NO arguments: `ds` (the logical date) is injected by airflow's
    # context - passing upstream values positionally into a reserved context key
    # is rejected by the taskflow api. dependencies are expressed with >> only.
    feeds = wait_for_feeds()
    uploaded = generate_and_upload()
    ingested = ingest()
    curated = curate()
    featured = features()
    detected = detect()
    dq = data_quality()
    published = publish()
    feeds >> uploaded >> ingested >> curated >> featured >> detected >> dq >> published


banking_eod()
