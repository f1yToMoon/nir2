---
id: ADR-001
title: Образ apache/iceberg-rest-fixture использует тег latest вместо 1.12.0
status: Принято
date: 2026-10-22
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

### Фактическая версия в образе

Образ digest-пиннингован:

```
CATALOG_FIXTURE_DIGEST=sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d
```

Jar `iceberg-rest-adapter.jar` внутри образа содержит `iceberg-build.properties`:

```
git.build.version=b0df3ca
git.commit.id=b0df3ca01d61b2f7ae7143ac660c6b16e33b6e46
git.commit.time=2026-04-29T02:05:57+0000
git.closest.tag.name=          (пусто — нет числового релизного тега)
```

Фактическая версия образа: **нерелизный снимок `main` от 29.04.2026 (`b0df3ca`)**.
Числовой релизный тег отсутствует (`git.closest.tag.name` пустой).
Формальная строка версии из кода: `Apache Iceberg b0df3ca (commit b0df3ca01d61b2f7ae7143ac660c6b16e33b6e46)`.

## Решение

Использовать `apache/iceberg-rest-fixture:latest` через переменную
`CATALOG_FIXTURE_TAG` вместо `ICEBERG_VERSION`. Образ закреплён по digest.

- `ICEBERG_VERSION=1.12.0` управляет jar-артефактами (Spark/Flink runtime,
  AWS bundle), которые существуют на Maven Central.
- `CATALOG_FIXTURE_TAG=latest` + `CATALOG_FIXTURE_DIGEST` управляют образом
  сервера каталога.

Изменения:
- `infra/.env.example`, `infra/.env`: добавлены переменные `CATALOG_FIXTURE_TAG=latest`
  и `CATALOG_FIXTURE_DIGEST=sha256:db8de90b...`.
- `infra/docker-compose.yml`: образ каталога изменён с
  `apache/iceberg-rest-fixture:${ICEBERG_VERSION}` на
  `apache/iceberg-rest-fixture:${CATALOG_FIXTURE_TAG:-latest}@${CATALOG_FIXTURE_DIGEST}`.
- `scripts/versions_check.sh`: проверяет `CATALOG_FIXTURE_TAG`, а не `ICEBERG_VERSION`
  для образа каталога.

## Почему допустимо

REST Catalog API (v1) является версионированным протоколом. Все операции, задействованные
в исследовании (commit, loadTable, listSnapshots), стабилизированы начиная с Iceberg 1.4
и не менялись в 1.11–1.12.

Критически важно: **детектирование конфликтов OCC (K1, K2) выполняется на стороне
клиента** (библиотека Iceberg 1.12.0). Сервер каталога лишь хранит метаданные и
выполняет CAS-операцию при коммите. Снимок `b0df3ca` (2026-04-29) из ветки `main`
включает все изменения протокола 1.11 и предшествует только ещё не влитым патчам 1.12.0,
которые не затрагивают CommitTable API.

## Последствия

**Положительные:**
- Стенд разворачивается без ручных модификаций Docker-образа.
- Digest-пиннинг обеспечивает воспроизводимость: при pull всегда используется
  один и тот же слой байтов.

**Отрицательные / риски:**
- Сервер каталога собран из нерелизного снимка `main`. В теории возможна несовместимость
  с финальным 1.12.0 в деталях ответов. Этап 1 (матрица конфликтов) проверяет
  поведение на реальном стеке и компенсирует этот риск.

**Действие:** При появлении `apache/iceberg-rest-fixture:1.12.0` на Docker Hub
обновить `CATALOG_FIXTURE_TAG=1.12.0`, пересчитать digest и переопубликовать
матрицу конфликтов при необходимости.
