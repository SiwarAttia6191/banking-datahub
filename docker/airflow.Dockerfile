FROM apache/airflow:2.9.3-python3.11

# docker cli: the dag tasks submit spark containers via subprocess (the docker
# provider package is NOT needed - no DockerOperator in use)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER ${AIRFLOW_UID:-50000}
