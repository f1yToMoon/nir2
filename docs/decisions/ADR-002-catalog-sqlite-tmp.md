---
id: ADR-002
title: Каталог использует persistent volume с catalog-init вместо SQLite в /tmp
status: Принято
date: 2026-10-22
---

## Контекст

ТЗ не специфицировало путь к базе данных каталога. Образ
`apache/iceberg-rest-fixture:latest` запускается от пользователя `iceberg`
(uid=1000). При первой попытке использовать `jdbc:sqlite:/var/lib/iceberg-catalog/catalog.db`
контейнер падал с ошибкой `SQLITE_CANTOPEN`:

```
org.sqlite.SQLiteException: [SQLITE_CANTOPEN] Unable to open the database file
```

Причина: при прямом монтировании именованного тома Docker создаёт точку монтирования
с правами root. Пользователь `iceberg` (uid=1000) не имеет прав на запись.

Промежуточное решение (не задокументированное ранее) — использование `/tmp/catalog.db` —
устраняло симптом, но данные каталога не переживали пересоздание контейнера.

## Решение

Добавлен init-контейнер `catalog-init`, который выполняется от root до запуска `catalog`
и устанавливает правильные права на именованный том:

```yaml
catalog-init:
  image: alpine:3.20
  user: root
  command: ["sh", "-c", "mkdir -p /catalog-data && chown -R 1000:1000 /catalog-data"]
  volumes:
    - catalog-data:/catalog-data
  restart: "no"
```

Сервис `catalog` монтирует тот же том и использует постоянный путь:

```yaml
catalog:
  image: apache/iceberg-rest-fixture:${CATALOG_FIXTURE_TAG:-latest}@${CATALOG_FIXTURE_DIGEST}
  depends_on:
    catalog-init: {condition: service_completed_successfully}
    ...
  environment:
    CATALOG_URI: jdbc:sqlite:file:/var/lib/iceberg-catalog/catalog.db
  volumes:
    - catalog-data:/var/lib/iceberg-catalog
```

Именованный том `catalog-data` добавлен в секцию `volumes` docker-compose.yml.

## Последствия

**Положительные:**
- Данные каталога переживают пересоздание контейнера (`docker compose rm catalog`).
- Стенд не требует `make seed` после рестарта — метаданные таблиц сохраняются.
- Образ запускается без ошибок доступа.

**Отрицательные / риски:**
- Для чистого пересоздания каталога с нуля необходимо удалить том явно:
  `docker compose down -v catalog` или `docker volume rm nir_catalog-data`.
- Init-контейнер добавляет ~1 с ко времени старта стека (незначимо).

**Экспериментальная целостность:** данные Iceberg хранятся в S3 (SeaweedFS).
SQLite хранит только метаданные каталога (список таблиц/namespace). Персистентность
обеспечивает стабильность стенда между прогонами экспериментов.
