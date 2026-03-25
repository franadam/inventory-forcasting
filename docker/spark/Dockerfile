FROM spark:python3-java17

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

USER spark

WORKDIR /opt/spark