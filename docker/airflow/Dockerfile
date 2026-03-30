ARG AIRFLOW_VERSION=3.1.7
ARG PYTHON_VERSION=3.10
ARG JAVA_VERSION=17
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG JAVA_VERSION
ARG CONSTRAINT_URL

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/opt/airflow:/opt/project:/opt/project/spark"

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-${JAVA_VERSION}-jdk-headless \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/airflow/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
        apache-airflow-providers-postgres \
        apache-airflow-providers-apache-spark \
        --constraint "${CONSTRAINT_URL}" && \
    pip install --no-cache-dir \
        -r /tmp/requirements.txt \
        --constraint "${CONSTRAINT_URL}" && \
    rm -f /tmp/requirements.txt

WORKDIR ${AIRFLOW_HOME}