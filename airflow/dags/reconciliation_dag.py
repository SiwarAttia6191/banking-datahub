"""reconciliation_chain: the parallel-run validation, orchestrated.

Chained after the end-of-day dag: the legacy baseline (the spark sql port of the
hive script) runs against the same inputs, the reconciliation job compares the two
outputs per partition, and the verdict gate FAILS the dag unless the legacy output
and the curated layer agree - strict key equivalence, the parallel-validation
pattern from production, as a real go/no-go gate.

Issue #37: both spark steps run as pods on the kind cluster (KubernetesPodOperator,
same image and rbac as the banking_eod dag); the verdict gate is a separate task
that reads the reconciliation report and blocks on anything but PASS.
"""

from __future__ import annotations

import datetime as dt

import airflow  # noqa: F401
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

SPARK_IMAGE = "banking-datahub/spark:3.5.3"
K8S_NAMESPACE = "banking-datahub"

KPO_COMMON = dict(
    namespace=K8S_NAMESPACE,
    image=SPARK_IMAGE,
    image_pull_policy="IfNotPresent",
    in_cluster=False,
    config_file="/home/airflow/.kube/config-airflow.yaml",
    cluster_context="kind-banking-datahub",
    get_logs=True,
    is_delete_operator_pod=True,
)

BASELINE_JAR = "local:///opt/spark/jobs/datahub-baseline-assembly-0.1.0.jar"
BASELINE_CLASS = "io.github.siwarattia.datahub.baseline"


def baseline_pod(task_id: str, step: str) -> KubernetesPodOperator:
    cls = {"baseline": f"{BASELINE_CLASS}.BaselineJob",
           "reconcile": f"{BASELINE_CLASS}.ReconciliationJob"}[step]
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"banking-recon-{step}",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=["--master", "local[*]", "--class", cls, BASELINE_JAR,
                   "--date", "{{ ds }}"],
        env_vars={"S3A_ENDPOINT": "http://s3:8333"},
        **KPO_COMMON,
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

    @task
    def reconciliation_gate() -> str:
        """The go/no-go gate: reads the newest reconciliation report and fails the
        dag unless the legacy output and the curated layer agree."""
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

    baseline = baseline_pod("baseline", "baseline")
    reconcile_op = baseline_pod("reconcile", "reconcile")
    gate = reconciliation_gate()
    baseline >> reconcile_op >> gate


reconciliation_chain()
