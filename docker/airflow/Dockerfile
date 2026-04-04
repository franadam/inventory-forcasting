ARG AIRFLOW_VERSION=3.1.7
ARG PYTHON_VERSION=3.10
ARG JAVA_VERSION=17
ARG SPARK_VERSION=4.0.1
ARG HADOOP_VERSION=3
ARG POSTGRES_JDBC_VERSION=42.7.8

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG JAVA_VERSION
ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG POSTGRES_JDBC_VERSION

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="/opt:/opt/project"

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-${JAVA_VERSION}-jdk-headless \
        curl \
        procps \
        bash && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

USER airflow

COPY --chown=airflow:root docker/airflow/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-postgres

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements.txt

RUN rm -rf /tmp/requirements.txt

WORKDIR /opt/orchestration