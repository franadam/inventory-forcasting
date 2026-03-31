ARG POSTGRES_JDBC_VERSION=42.7.8

FROM spark:python3-java17

USER root

ARG POSTGRES_JDBC_VERSION

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        curl && \
    mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
        https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    rm -rf /var/lib/apt/lists/*

COPY docker/spark/requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/opt/project:/opt/project/spark"
ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar"

USER spark

WORKDIR /opt/project/spark