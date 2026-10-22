#!/usr/bin/env bash
# Проверка доступности всех зафиксированных образов и jar (этап 0, шаг 0.2).
# Использование: bash scripts/versions_check.sh infra/.env
set -uo pipefail
ENV_FILE="${1:-infra/.env}"
set -a; source "$ENV_FILE"; set +a
MAVEN=https://repo1.maven.org/maven2
fail=0

check_image() {
  if docker manifest inspect "$1" >/dev/null 2>&1; then echo "OK    image $1"; else echo "FAIL  image $1"; fail=1; fi
}
check_jar() {
  local url="$MAVEN/$1"
  if curl -fsI "$url" >/dev/null; then echo "OK    jar   $1"; else echo "FAIL  jar   $1"; fail=1; fi
}

check_image "chrislusf/seaweedfs:${SEAWEEDFS_VERSION}"
check_image "amazon/aws-cli:${AWSCLI_VERSION}"
check_image "ghcr.io/shopify/toxiproxy:${TOXIPROXY_VERSION}"
check_image "apache/iceberg-rest-fixture:${ICEBERG_VERSION}"
check_image "quay.io/lakekeeper/catalog:${LAKEKEEPER_VERSION}"
check_image "${SPARK_IMAGE}"
check_image "${FLINK_IMAGE}"
check_image "${KAFKA_IMAGE}"
check_image "postgres:${POSTGRES_VERSION}"
check_image "prom/prometheus:${PROMETHEUS_VERSION}"
check_image "grafana/grafana:${GRAFANA_VERSION}"
check_image "${PYTHON_IMAGE}"

check_jar "org/apache/iceberg/${ICEBERG_SPARK_RUNTIME_ARTIFACT}/${ICEBERG_VERSION}/${ICEBERG_SPARK_RUNTIME_ARTIFACT}-${ICEBERG_VERSION}.jar"
check_jar "org/apache/iceberg/${ICEBERG_FLINK_RUNTIME_ARTIFACT}/${ICEBERG_VERSION}/${ICEBERG_FLINK_RUNTIME_ARTIFACT}-${ICEBERG_VERSION}.jar"
check_jar "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
check_jar "org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"
check_jar "org/apache/hadoop/hadoop-client-api/${HADOOP_CLIENT_VERSION}/hadoop-client-api-${HADOOP_CLIENT_VERSION}.jar"
check_jar "org/apache/hadoop/hadoop-client-runtime/${HADOOP_CLIENT_VERSION}/hadoop-client-runtime-${HADOOP_CLIENT_VERSION}.jar"

if [ "$fail" -ne 0 ]; then
  echo "Есть недоступные версии: исправьте infra/.env (строки с VERIFY)."; exit 1
fi
echo "Все версии доступны."
