ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}

WORKDIR /opt/airflow

USER root

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER root

RUN rm /tmp/requirements.txt

USER airflow