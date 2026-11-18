FROM apache/airflow:2.9.3-python3.11

# docker cli + provider: the dag tasks submit spark containers
USER root
RUN apt-get update && apt-get install -y --no-install-recommends docker.io \
    && pip install --no-cache-dir apache-airflow-providers-docker \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER ${AIRFLOW_UID:-50000}
