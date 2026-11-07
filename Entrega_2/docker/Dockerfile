# Imagen local de OceanWatch: Spark 4.2 + Delta Lake + MLflow sobre Linux.
# En Windows, Spark 4.x exige winutils/HADOOP_HOME; el contenedor evita binarios de terceros.
FROM python:3.12-slim-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PYTHONPATH=/workspace/src \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

COPY requirements-local.txt /tmp/requirements-local.txt
RUN pip install --no-cache-dir -r /tmp/requirements-local.txt

WORKDIR /workspace
