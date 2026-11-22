"""banking_eod: the end-of-day chain, orchestrated.

The Airflow port of the legacy Oozie workflow (migration/legacy/oozie/workflow.xml):
wait for the day's feeds -> ingest -> curate -> features -> detect -> data quality.
Same order, same business intent; the mapping table lives in docs/migration-guide.md.

Issue #37: each spark task is a POD on the local kind cluster (KubernetesPodOperator,
the cncf provider) - one pod per task, no docker socket, no host bindings. The pods
run the same fat jars the compose path uses (baked into banking-datahub/spark, see
k8s/spark.Dockerfile), in local[*] mode inside the pod; the pod itself is scheduled
by kubernetes with its own environment. The dq gate is a separate task that reads
the verdict report and blocks publish on anything but PASS.
"""

from __future__ import annotations

import airflow  # noqa: F401  (ensures the airflow import succeeds first)
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import datetime as dt

SPARK_IMAGE = "banking-datahub/spark:3.5.3"  # jars baked in, see k8s/spark.Dockerfile
K8S_NAMESPACE = "banking-datahub"

# every spark task becomes a pod in the kind cluster: the operator talks to the
# cluster api with the mounted kubeconfig (server rewritten to host.docker.internal,
# 127.0.0.1 inside this container is not the host)
KPO_COMMON = dict(
    namespace=K8S_NAMESPACE,
    image=SPARK_IMAGE,
    image_pull_policy="IfNotPresent",  # the image lives in the kind node, not a registry
    in_cluster=False,
    config_file="/home/airflow/.kube/config-airflow.yaml",
    cluster_context="kind-banking-datahub",
    get_logs=True,
    is_delete_operator_pod=True,
)

JOBS = {
    "ingest": ("io.github.siwarattia.datahub.ingestion.RawIngestionJob",
               "local:///opt/spark/jobs/datahub-ingestion-assembly-0.1.0.jar"),
    "curate": ("io.github.siwarattia.datahub.transform.CuratedTransactionsJob",
               "local:///opt/spark/jobs/datahub-transform-assembly-0.1.0.jar"),
    "features": ("io.github.siwarattia.datahub.transform.AccountFeaturesJob",
                 "local:///opt/spark/jobs/datahub-transform-assembly-0.1.0.jar"),
    "detect": ("io.github.siwarattia.datahub.transform.DetectionJob",
               "local:///opt/spark/jobs/datahub-transform-assembly-0.1.0.jar"),
    "dq": ("io.github.siwarattia.datahub.quality.DataQualityJob",
           "local:///opt/spark/jobs/datahub-quality-assembly-0.1.0.jar"),
    "publish": ("io.github.siwarattia.datahub.transform.ServingJob",
                "local:///opt/spark/jobs/datahub-transform-assembly-0.1.0.jar"),
}


def spark_pod(task_id: str, step: str, date_expr: str = "{{ ds }}",
              extra_env: dict | None = None) -> KubernetesPodOperator:
    """One pipeline step as a pod: the pod IS the spark driver (local[*] mode
    inside its own container), so the pod environment is the job environment."""
    class_name, jar = JOBS[step]
    env = {"S3A_ENDPOINT": "http://s3:8333"}
    if extra_env:
        env.update(extra_env)
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"banking-eod-{step}",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=["--master", "local[*]", "--class", class_name, jar,
                   "--date", date_expr],
        env_vars=env,
        **KPO_COMMON,
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
    tags=["banking", "eod", "spark", "kubernetes"],
)
def banking_eod():

    @task
    def wait_for_feeds(ds: str | None = None) -> str:
        """Oozie coordinator gate, ported: the day's feed must be in the inbox before
        anything runs. Polls the s3 api via boto3."""
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

    @task
    def dq_gate() -> str:
        """The go/no-go gate: reads the newest dq report from the lake and fails the
        dag unless the verdict is PASS - publish is blocked behind this task."""
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

    # the tasks take NO arguments: `ds` (the logical date) is injected by airflow's
    # context and templated into the pod arguments. dependencies via >> only.
    feeds = wait_for_feeds()
    uploaded = generate_and_upload()
    ingest_op = spark_pod("ingest", "ingest")
    curate_op = spark_pod("curate", "curate")
    features_op = spark_pod("features", "features")
    detect_op = spark_pod("detect", "detect")
    dq_op = spark_pod("dq", "dq")
    gate = dq_gate()
    publish_op = spark_pod("publish", "publish",
                           extra_env={"SERVING_JDBC_URL": "jdbc:postgresql://postgres:5432/datahub"})
    feeds >> uploaded >> ingest_op >> curate_op >> features_op >> detect_op >> dq_op >> gate >> publish_op


banking_eod()
