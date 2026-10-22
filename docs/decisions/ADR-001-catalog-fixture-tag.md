---
id: ADR-001
title: Образ apache/iceberg-rest-fixture использует тег latest вместо 1.12.0
status: Принято
date: 2026-11-02
---

## Контекст

ТЗ зафиксировало версию Iceberg 1.12.0 (октябрь 2026) как основную. Образ
`apache/iceberg-rest-fixture` на Docker Hub не имеет тега `1.12.0`:
последний опубликованный числовой тег — `1.10.1` (2025-12-22), тег `latest`
пересобирался 2026-04-29. Тег `1.12.0` отсутствует на момент развёртывания стенда.

```
$ docker manifest inspect apache/iceberg-rest-fixture:1.12.0
no such manifest
```

Тег `latest` содержит эндпоинты `/plan` и `/tasks`, добавленные в Iceberg 1.11+,
что подтверждает версию ≥ 1.11.0.

## Решение

Использовать `apache/iceberg-rest-fixture:latest` через переменную
`CATALOG_FIXTURE_TAG` вместо `ICEBERG_VERSION`.

- `ICEBERG_VERSION=1.12.0` управляет jar-артефактами (Spark/Flink runtime,
  AWS bundle), которые существуют на Maven Central.
- `CATALOG_FIXTURE_TAG=latest` управляет только образом сервера каталога.

Изменения:
- `infra/.env.example`, `infra/.env`: добавлена переменная `CATALOG_FIXTURE_TAG=latest`.
- `infra/docker-compose.yml`: образ каталога изменён с
  `apache/iceberg-rest-fixture:${ICEBERG_VERSION}` на
  `apache/iceberg-rest-fixture:${CATALOG_FIXTURE_TAG:-latest}`.
- `scripts/versions_check.sh`: проверяет `CATALOG_FIXTURE_TAG`, а не `ICEBERG_VERSION`
  для образа каталога.

## Последствия

**Положительные:**
- Стенд разворачивается без ручных модификаций Docker-образа.
- REST Catalog API (v1) обратно совместим: клиент 1.12.0 работает с сервером ≥ 1.11.


**Отрицательные / риски:**
- Сервер каталога может содержать незначительные отличия в поведении валидаций
  или ответах. Матрица конфликтов проверяется на этапе 1, что компенсирует этот риск.
- Тег `latest` нестабилен. При повторном развёртывании в будущем версия может смениться.
  Рекомендуется при выходе 1.12.0-образа закрепить тег и обновить ADR.

**Действие:** При появлении `apache/iceberg-rest-fixture:1.12.0` на Docker Hub
обновить `CATALOG_FIXTURE_TAG=1.12.0` и переопубликовать матрицу конфликтов
при необходимости.
