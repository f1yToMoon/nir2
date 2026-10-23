ARG SPARK_IMAGE=apache/spark:4.0.1-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ARG ICEBERG_VERSION=1.12.0
ARG ICEBERG_SPARK_RUNTIME_ARTIFACT=iceberg-spark-runtime-4.0_2.13
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Iceberg Spark runtime + AWS bundle (S3FileIO)
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/${ICEBERG_SPARK_RUNTIME_ARTIFACT}/${ICEBERG_VERSION}/${ICEBERG_SPARK_RUNTIME_ARTIFACT}-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Python-зависимости runner (версия pyspark совпадает с образом — используется встроенная)
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends python3-pip curl; rm -rf /var/lib/apt/lists/*; \
    pip3 install --no-cache-dir --break-system-packages \
      "pydantic>=2.7" "pyyaml>=6" "requests>=2.32" "psycopg[binary]>=3.2" \
      "confluent-kafka>=2.5" "numpy>=1.26" "pandas>=2.2" "pyarrow>=16" "pyiceberg[pyarrow]>=0.10" \
      "tenacity>=8.5" "structlog>=24.1" "typer>=0.12" "pytest>=8.3" "matplotlib>=3.9" || \
    pip3 install --no-cache-dir \
      "pydantic>=2.7" "pyyaml>=6" "requests>=2.32" "psycopg[binary]>=3.2" \
      "confluent-kafka>=2.5" "numpy>=1.26" "pandas>=2.2" "pyarrow>=16" "pyiceberg[pyarrow]>=0.10" \
      "tenacity>=8.5" "structlog>=24.1" "typer>=0.12" "pytest>=8.3" "matplotlib>=3.9"

COPY infra/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY infra/spark/fairscheduler.xml /opt/spark/conf/fairscheduler.xml

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=python3 \
    PYSPARK_PIN_THREAD=true
