# 04. Интерфейсы

## 1. Прокси REST-каталога

### 1.1. Маршрутизация

| Запрос | Поведение |
| --- | --- |
| Любой метод, любой путь | Проксируется на `CAPL_UPSTREAM_URI` с сохранением метода, query, заголовков (кроме hop-by-hop: `Connection`, `Keep-Alive`, `Transfer-Encoding`, `Upgrade`, `Proxy-*`, `TE`, `Trailer`) и тела; ответ возвращается без изменений |
| `POST /v1/{prefix}/namespaces/{ns}/tables/{table}` | Коммит таблицы: разбор тела, журналирование, проверка удержания, форвардинг, журналирование ответа, постановка в очередь обогащения при 200 |
| `POST /v1/namespaces/{ns}/tables/{table}` | То же (каталоги без prefix) |
| `POST /v1/{prefix}/transactions/commit` | Мульти-табличный коммит: только журналирование по каждой таблице, без удержания |
| `POST .../tables/{table}/metrics` | Отчёты клиентов (`CommitReport`, `ScanReport`): проксируются и дополнительно сохраняются в `client_reports` (содержат число попыток коммита) |

`prefix` может отсутствовать или быть произвольным (Lakekeeper использует идентификатор warehouse); разбор пути — по регулярному выражению, а не по фиксированному числу сегментов. Имена namespace могут содержать разделитель `%1F`.

### 1.2. Разбор тела коммита

Пример тела `CommitTableRequest` (сокращённо):

```json
{
  "identifier": {"namespace": ["bench"], "name": "events"},
  "requirements": [
    {"type": "assert-table-uuid", "uuid": "8f0c..."},
    {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 512334}
  ],
  "updates": [
    {"action": "add-snapshot", "snapshot": {
      "snapshot-id": 774412, "parent-snapshot-id": 512334, "sequence-number": 97,
      "timestamp-ms": 1793022000000,
      "manifest-list": "s3://warehouse/bench/events/metadata/snap-774412-1-....avro",
      "summary": {"operation": "replace", "added-data-files": "12", "deleted-data-files": "143",
                  "changed-partition-count": "4", "capl.op-id": "S2-r3-op-0147", "spark.app.id": "app-..."},
      "schema-id": 0}},
    {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 774412}
  ]
}
```

Извлекаемые поля: `table` = `{ns}.{name}`, `base_snapshot_id` (из `assert-ref-snapshot-id` для `main`), `new_snapshot_id`, `sequence_number`, `operation` (`append`/`overwrite`/`replace`/`delete`), `manifest_list`, `summary` (целиком, JSONB), `op_id` (из заголовка/слота или `summary["capl.op-id"]`). Операция Iceberg `replace` соответствует компакции, `overwrite` — CoW DML/overwrite, `delete` — удалениям, `append` — добавлению; MoR DML даёт `overwrite` с добавленными delete-файлами (`added-delete-files > 0`) — классификация в `capl.proxy.commit_parser.classify()`.

### 1.3. Заголовки

| Заголовок | Источник | Значение |
| --- | --- | --- |
| `X-CAPL-Slot` | Spark-каталог слота: `spark.sql.catalog.s<k>.header.X-CAPL-Slot=s<k>` | Идентификатор слота runner |
| `X-CAPL-Writer` | Конфигурация каталога писателя | `stream` / `batch_dml` / `maintenance` / `user` |
| `X-CAPL-Run` | runner | `run_id` текущего прогона |
| `Idempotency-Key` | Клиент Iceberg ≥ 1.12 | Сохраняется в журнале, используется для распознавания повторной отправки после таймаута |

### 1.4. Конфигурация (переменные окружения `capl`)

| Переменная | Пример | Назначение |
| --- | --- | --- |
| `CAPL_UPSTREAM_URI` | `http://catalog:8181` | Каталог |
| `CAPL_CONFIG` | `/config/capl.yaml` | Основная конфигурация |
| `CAPL_PG_DSN` | `postgresql://capl:capl@postgres:5432/telemetry` | Телеметрия |
| `CAPL_S3_ENDPOINT` | `http://toxiproxy:18333` | Чтение манифестов |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | из `.env` | Доступ к S3 |
| `CAPL_POLICY` | `P6` | Активная политика по умолчанию (переопределяется runner через API) |
| `CAPL_MODEL_DIR` | `/models/current` | Каталог модели |

## 2. Advisor API

Полная спецификация — [../api/advisor.openapi.yaml](../api/advisor.openapi.yaml). Сводка:

| Метод и путь | Назначение |
| --- | --- |
| `POST /v1/runs` | Начать прогон: `run_id`, политика, параметры; сбрасывает состояние |
| `POST /v1/runs/{run_id}/finish` | Завершить прогон: сброс буферов телеметрии |
| `POST /v1/intents` | Регистрация намерения → решение |
| `POST /v1/intents/{intent_id}/reevaluate` | Повторная оценка после задержки |
| `POST /v1/intents/{intent_id}/start` | Операция запущена на слоте |
| `POST /v1/intents/{intent_id}/heartbeat` | Прогресс (доля выполненных задач Spark) |
| `POST /v1/intents/{intent_id}/finish` | Исход и стоимость |
| `GET /v1/risk/{namespace}/{table}` | Карта риска по партициям |
| `GET /v1/state` | Отладочный снимок состояния |
| `PUT /v1/policy` | Смена политики и параметров (только между прогонами) |
| `GET /healthz`, `GET /metrics` | Служебные |

Контракт runner ↔ advisor:

1. Для каждой управляемой операции runner вызывает `POST /v1/intents`; при `delay` ждёт до `retry_at` и вызывает `reevaluate`; при `run_now`/`narrow` вызывает `start` с назначенным слотом **до** первого Spark-действия.
2. Во время выполнения runner раз в `heartbeat_interval_s` отправляет прогресс (из `SparkStatusTracker`).
3. После завершения runner вызывает `finish` в любом исходе (в `finally`), затем освобождает слот.
4. Слот не может быть назначен двум операциям одновременно; advisor проверяет это и возвращает 409.

## 3. Схема телеметрии

DDL — [../infra/postgres/init/01_telemetry.sql](../infra/postgres/init/01_telemetry.sql). Таблицы:

| Таблица | Гранулярность | Пишет |
| --- | --- | --- |
| `runs` | прогон | runner |
| `ops` | управляемая операция | runner (исход, стоимость) + advisor (решение, прогноз) |
| `decisions` | решение advisor | advisor |
| `commit_attempts` | HTTP-запрос коммита | proxy |
| `commit_events` | успешный снапшот с восстановленным следом | enricher |
| `commit_partitions` | (снапшот, партиция) | enricher |
| `client_reports` | `CommitReport`/`ScanReport` клиента | proxy |
| `table_health` | снимок раз в 30 с по партиции | runner |
| `run_summary` | агрегаты прогона | analysis |

Экспорт в Parquet: `make export RUN=<run_id>` → `data/traces/<run_id>/<table>.parquet`.

## 4. События

JSON Schema — [../schemas/](../schemas/):

- `commit_event.schema.json` — успешный коммит с восстановленным следом (вход для интенсивностей и признаков);
- `op_event.schema.json` — жизненный цикл управляемой операции (`intent`, `decision`, `start`, `heartbeat`, `finish`).

События хранятся в Postgres; для симулятора и офлайн-сборки признаков используются те же схемы, загруженные из Parquet.

## 5. Конфигурация Spark-слотов в runner

```python
# runner/slots.py (эскиз)
for k in range(n_slots):
    name = f"s{k}"
    spark.conf.set(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{name}.type", "rest")
    spark.conf.set(f"spark.sql.catalog.{name}.uri", catalog_uri)          # http://capl:8180
    spark.conf.set(f"spark.sql.catalog.{name}.warehouse", warehouse)
    spark.conf.set(f"spark.sql.catalog.{name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set(f"spark.sql.catalog.{name}.s3.endpoint", s3_endpoint)  # http://toxiproxy:18333
    spark.conf.set(f"spark.sql.catalog.{name}.s3.path-style-access", "true")
    spark.conf.set(f"spark.sql.catalog.{name}.header.X-CAPL-Slot", name)
    spark.conf.set(f"spark.sql.catalog.{name}.header.X-CAPL-Writer", writer_class)
    spark.conf.set(f"spark.sql.catalog.{name}.cache-enabled", "false")     # всегда свежий снапшот
```

Операция на слоте `s3` обращается к таблице как `s3.bench.events`. Проверка работоспособности заголовков — шаг 0.6 этапа 0; при неудаче используется резервный канал `capl.op-id` в summary (`.option("snapshot-property.capl.op-id", op_id)` для DataFrame-записи; для SQL-операций — Java-вызов `org.apache.iceberg.spark.CommitMetadata.withCommitProperties` через py4j в потоке операции).

Job group для учёта стоимости: `spark.sparkContext.setJobGroup(op_id, op_type, interruptOnCancel=True)` в потоке операции (PySpark pinned thread mode включён по умолчанию).
