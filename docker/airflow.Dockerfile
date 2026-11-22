FROM apache/airflow:2.9.3-python3.11

# docker cli kept for compatibility; the kubernetes provider turns the dag tasks
# into pods (issue #37), and boto3 feeds the sensor + the generate/upload task.
# pip MUST run as the airflow user (the image refuses root) with umask 0002 so
# root can still read the installed files.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER ${AIRFLOW_UID:-50000}
RUN umask 0002; pip install --no-cache-dir apache-airflow-providers-cncf-kubernetes boto3
