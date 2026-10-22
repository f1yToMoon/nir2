---
id: ADR-002
title: Каталог использует SQLite в /tmp вместо /var/lib/iceberg-catalog
status: Принято
date: 2026-10-22
---

## Контекст

ТЗ не специфицировало путь к базе данных каталога. Образ
`apache/iceberg-rest-fixture:latest` запускается от пользователя `iceberg`
(uid=1000). При попытке использовать `jdbc:sqlite:/var/lib/iceberg-catalog/catalog.db`
контейнер падал с ошибкой `SQLITE_CANTOPEN`:

```
org.sqlite.SQLiteException: [SQLITE_CANTOPEN] Unable to open the database file
```

Причина: `/var/lib/iceberg-catalog/` создаётся при сборке образа от root, а пользователь
`iceberg` не имеет прав на запись. Named volume Docker монтируется также от root.

## Решение

Использовать `/tmp/catalog.db`:

```yaml
CATALOG_URI: jdbc:sqlite:file:/tmp/catalog.db
```

Директория `/tmp` доступна на запись любому пользователю (sticky bit 1777).

Именованный том `catalog-data` удалён — он не нужен при использовании `/tmp`.

## Последствия

**Положительные:**
- Образ запускается без ошибок.
- Отсутствует зависимость от именованного тома и его прав доступа.

**Отрицательные:**
- Данные каталога не переживают пересоздание контейнера (`docker compose rm catalog`).
- Перед каждым новым прогоном стенд требует `make seed` для воссоздания таблиц.

Это свойство документировано в комментарии в `docker-compose.yml` и является
ожидаемым поведением для одноразового стенда.

**Экспериментальная целостность:** данные Iceberg хранятся в S3 (SeaweedFS).
Потеря SQLite-метаданных при рестарте каталога не влияет на уже записанные
Parquet/Avro-файлы; `make seed` восстанавливает указатели.
