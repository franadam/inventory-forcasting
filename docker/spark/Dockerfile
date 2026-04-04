ARG PYTHON_VERSION=3.10
ARG SPARK_VERSION=4.0.1
ARG HADOOP_VERSION=3
ARG POSTGRES_JDBC_VERSION=42.7.8

FROM python:${PYTHON_VERSION}-slim-bookworm

ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG POSTGRES_JDBC_VERSION

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/opt/project:/opt"

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        openjdk-17-jdk-headless \
        curl \
        bash \
        tini \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY docker/spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

RUN useradd -ms /bin/bash spark && chown -R spark:spark /opt/spark
USER spark

WORKDIR /opt/project/spark

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["spark-submit", "--version"]