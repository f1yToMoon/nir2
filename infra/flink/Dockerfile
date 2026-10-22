ARG FLINK_IMAGE=flink:1.20.3-scala_2.12-java17
FROM ${FLINK_IMAGE}

ARG FLINK_MINOR=1.20
ARG ICEBERG_VERSION=1.12.0
ARG ICEBERG_FLINK_RUNTIME_ARTIFACT=iceberg-flink-runtime-1.20
ARG FLINK_KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG HADOOP_CLIENT_VERSION=3.3.6
ARG MAVEN=https://repo1.maven.org/maven2

# Iceberg Flink runtime, AWS bundle (S3FileIO), Kafka SQL connector.
# iceberg-flink-runtime требует классы Hadoop (Configuration) даже при S3FileIO —
# добавляются shaded-клиенты Hadoop (VERIFY на этапе 0: smoke-тест записи в таблицу).
RUN set -eux; \
    cd /opt/flink/lib; \
    wget -q ${MAVEN}/org/apache/iceberg/${ICEBERG_FLINK_RUNTIME_ARTIFACT}/${ICEBERG_VERSION}/${ICEBERG_FLINK_RUNTIME_ARTIFACT}-${ICEBERG_VERSION}.jar; \
    wget -q ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    wget -q ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar; \
    wget -q ${MAVEN}/org/apache/hadoop/hadoop-client-api/${HADOOP_CLIENT_VERSION}/hadoop-client-api-${HADOOP_CLIENT_VERSION}.jar; \
    wget -q ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/${HADOOP_CLIENT_VERSION}/hadoop-client-runtime-${HADOOP_CLIENT_VERSION}.jar

COPY infra/flink/sql /opt/flink/sql
