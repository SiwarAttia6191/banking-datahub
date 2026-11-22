FROM apache/spark:3.5.3

# the pipeline's fat jars baked into the image: pods reference them with
# local:///opt/spark/jobs/... (local:// = already inside the image, no upload step).
# the assemblies carry hadoop-aws (s3a) and the postgres driver - the same jars
# the compose-path spark-submit uses, so job behaviour is identical.
COPY modules/ingestion/target/scala-2.12/datahub-ingestion-assembly-0.1.0.jar /opt/spark/jobs/
COPY modules/transform/target/scala-2.12/datahub-transform-assembly-0.1.0.jar /opt/spark/jobs/
COPY modules/quality/target/scala-2.12/datahub-quality-assembly-0.1.0.jar /opt/spark/jobs/
COPY modules/baseline/target/scala-2.12/datahub-baseline-assembly-0.1.0.jar /opt/spark/jobs/
