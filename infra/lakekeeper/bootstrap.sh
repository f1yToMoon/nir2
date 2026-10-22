#!/bin/sh
# Инициализация Lakekeeper (профиль lakekeeper): bootstrap сервера и создание warehouse на SeaweedFS.
# VERIFY на этапе 9: формат тела management API соответствует зафиксированной версии Lakekeeper.
set -eu
LK=http://lakekeeper:8181

for i in $(seq 1 60); do
  curl -sf "$LK/health" >/dev/null 2>&1 && break
  sleep 2
done

curl -s -X POST "$LK/management/v1/bootstrap" \
  -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}' || true

curl -sf -X POST "$LK/management/v1/warehouse" \
  -H 'Content-Type: application/json' \
  -d "{
    \"warehouse-name\": \"${LAKEKEEPER_WAREHOUSE_NAME}\",
    \"storage-profile\": {
      \"type\": \"s3\",
      \"bucket\": \"${S3_BUCKET}\",
      \"key-prefix\": \"lakekeeper\",
      \"endpoint\": \"http://toxiproxy:18333\",
      \"region\": \"${AWS_REGION}\",
      \"path-style-access\": true,
      \"flavor\": \"s3-compat\",
      \"sts-enabled\": false
    },
    \"storage-credential\": {
      \"type\": \"s3\",
      \"credential-type\": \"access-key\",
      \"aws-access-key-id\": \"${AWS_ACCESS_KEY_ID}\",
      \"aws-secret-access-key\": \"${AWS_SECRET_ACCESS_KEY}\"
    }
  }" || echo "warehouse may already exist"

echo "lakekeeper bootstrap done"
