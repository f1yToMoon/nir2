# 02. Архитектура

## 1. Компоненты стенда

| Сервис (compose) | Образ / сборка | Порты (host) | Роль |
| --- | --- | --- | --- |
| `s3` | `chrislusf/seaweedfs:${SEAWEEDFS_VERSION}` | 8333 (S3), 9333 (master UI) | Объектное хранилище (S3 API) |
| `s3-init` | `amazon/aws-cli:${AWSCLI_VERSION}` | — | Создание бакета `warehouse` (one-shot) |
| `toxiproxy` | `ghcr.io/shopify/toxiproxy:${TOXIPROXY_VERSION}` | 8474 (API), 18333 (S3 через задержку) | Эмуляция задержки объектного хранилища |
| `catalog` | `apache/iceberg-rest-fixture:${ICEBERG_VERSION}` | 8181 | Основной REST-каталог (JdbcCatalog на SQLite-файле) |
| `lakekeeper`, `lakekeeper-migrate`, `lakekeeper-bootstrap` | `quay.io/lakekeeper/catalog:${LAKEKEEPER_VERSION}` | 8182 | Альтернативный REST-каталог (профиль `lakekeeper`) |
| `capl` | `infra/capl/Dockerfile` | 8180 (прокси), 8190 (advisor), 9101 (метрики) | Предмет работы: прокси + advisor + модель |
| `spark-master` | `infra/spark/Dockerfile` | 8080 (UI), 7077 | Spark standalone master |
| `spark-worker-1`, `spark-worker-2` | `infra/spark/Dockerfile` | 8081, 8082 | Spark-воркеры |
| `runner` | `infra/spark/Dockerfile` (+ код) | 4040 (UI драйвера) | Исполнитель нагрузки (драйвер Spark в client mode) |
| `kafka` | `apache/kafka:${KAFKA_VERSION}` | 9092 | Источник событий (KRaft, один узел) |
| `producer` | `infra/capl/Dockerfile` (модуль `runner.producer`) | — | Генератор событий в Kafka |
| `flink-jobmanager`, `flink-taskmanager` | `infra/flink/Dockerfile` | 8085 (UI) | Потоковый писатель (профиль `streaming`) |
| `flink-sql` | `infra/flink/Dockerfile` | — | Отправка Flink SQL job (one-shot) |
| `postgres` | `postgres:${POSTGRES_VERSION}` | 5432 | Телеметрия CAPL/runner, БД Lakekeeper |
| `prometheus` | `prom/prometheus:${PROMETHEUS_VERSION}` | 9090 | Метрики |
| `grafana` | `grafana/grafana:${GRAFANA_VERSION}` | 3000 | Дашборды |

Профили Compose: без профиля — ядро (s3, toxiproxy, catalog, capl, spark, runner, postgres); `streaming` — kafka, producer, flink; `observability` — prometheus, grafana; `lakekeeper` — альтернативный каталог. `make up` = ядро + streaming + observability; `make up-lite` = ядро + observability (потоковая запись — Spark Structured Streaming внутри runner, Kafka заменяется генератором rate-source).

## 2. Схема развёртывания

```mermaid
graph LR
  subgraph Writers["Писатели"]
    FL["Flink SQL job<br/>(streaming append / upsert)"]
    RN["runner<br/>(Spark driver, слоты s0..sN)"]
  end
  subgraph CAPL["capl"]
    PX["proxy :8180"]
    ADV["advisor :8190"]
    ENR["enricher<br/>(чтение манифестов)"]
    ST["state store<br/>(in-memory + Postgres)"]
    MDL["models<br/>(analytic + LightGBM)"]
    PX <--> ST
    ADV <--> ST
    ENR --> ST
    ADV --> MDL
    PX -. hold decision .-> ADV
  end
  CAT["catalog :8181<br/>(REST fixture / Lakekeeper)"]
  TP["toxiproxy :18333"]
  S3["s3 (SeaweedFS) :8333"]
  PG["postgres"]
  SP["Spark master + 2 workers"]
  K["kafka"]
  PR["producer"]
  PROM["prometheus"] --> GF["grafana"]

  PR --> K --> FL
  RN -->|intent / start / finish| ADV
  RN -->|REST catalog API| PX
  FL -->|REST catalog API| PX
  PX --> CAT
  RN --- SP
  SP -->|data / metadata files| TP
  FL -->|data / metadata files| TP
  CAT -->|metadata.json| TP
  ENR -->|manifests| TP
  TP --> S3
  ST --> PG
  RN --> PG
  PROM --> PX
```

## 3. Ключевые взаимодействия

### 3.1. Жизненный цикл управляемой операции

```mermaid
sequenceDiagram
  autonumber
  participant R as runner
  participant A as advisor
  participant M as модели
  participant P as proxy
  participant C as catalog
  R->>A: POST /v1/intents {table, type, footprint, est_bytes, sla_ms}
  A->>M: признаки на момент t и кандидаты действий (now, +Δ1, +Δ2, narrow)
  M-->>A: p_k2, p_k1, D̂, cost для каждого кандидата
  A-->>R: decision {action: delay, retry_at: t+Δ1, decision_id}
  Note over R: ожидание Δ1
  R->>A: POST /v1/intents/{id}/reevaluate
  A-->>R: decision {action: run_now, footprint: F'}
  R->>A: POST /v1/intents/{id}/start {slot: s3}
  R->>R: Spark job (job group = op_id) на слоте s3
  R->>P: POST commit (header X-CAPL-Slot: s3)
  P->>A: hold? {slot→op_id, table, type}
  A-->>P: pass (или hold ≤ max_ms)
  P->>C: forward
  C-->>P: 200 / 409
  P-->>R: тот же ответ
  R->>A: POST /v1/intents/{id}/finish {outcome, attempts, cpu_ms, bytes}
```

### 3.2. Обогащение коммита следом

```mermaid
sequenceDiagram
  participant P as proxy
  participant Q as очередь обогащения
  participant E as enricher
  participant S as S3 (через toxiproxy)
  participant ST as state store
  P->>Q: commit_ok {table, new_snapshot_id, manifest_list, summary, op_id, writer}
  E->>Q: взять задачу
  E->>S: прочитать manifest list
  E->>S: прочитать манифесты, добавленные в этом снапшоте (added_snapshot_id = new)
  E->>E: собрать по партициям: added/deleted data, added delete files
  E->>ST: commit_event + обновить интенсивности λ[k, p]
```

Обогащение асинхронное: задержка не влияет на коммит. Для потоковых писателей с частыми коммитами чтение ограничивается манифестами со статусом ADDED/DELETED текущего снапшота (несколько небольших Avro-файлов).

### 3.3. Удержание коммита (выбор проигравшего)

```mermaid
sequenceDiagram
  participant A as runner: MERGE (дёшево, слот s1)
  participant B as runner: компакция (дорого, слот s2)
  participant P as proxy
  participant D as advisor
  participant C as catalog
  A->>P: commit (s1)
  P->>D: hold? op_A
  D->>D: найден B: симметричный К2, след пересекается, ETA(B) ≤ hold.max_ms, cost(B) ≥ κ·cost(A)
  D-->>P: hold until B commits or deadline
  B->>P: commit (s2)
  P->>C: forward B
  C-->>P: 200
  P-->>B: 200
  P->>D: event: B committed → release A
  P->>C: forward A
  C-->>P: 409
  P-->>A: 409
  Note over A: refresh + validation → ValidationException (проигрывает дешёвая операция)
```

## 4. Внутренняя архитектура сервиса `capl`

Один процесс Python (asyncio, uvicorn), три ASGI-приложения на разных портах и фоновые задачи:

| Модуль | Ответственность |
| --- | --- |
| `capl.proxy.app` | Starlette/FastAPI-приложение catch-all `/{path:path}`; потоковое проксирование через `httpx.AsyncClient` (пул соединений, таймауты из конфигурации); перехват коммитов |
| `capl.proxy.commit_parser` | Разбор `CommitTableRequest` и `CommitTransactionRequest` (requirements, updates, add-snapshot summary) |
| `capl.proxy.hold` | Асинхронные «ворота»: `asyncio.Event` на ожидаемую операцию, дедлайн, метрики удержаний |
| `capl.enricher` | Пул воркеров обогащения; чтение Avro-манифестов через `pyiceberg.io` / `fastavro` |
| `capl.advisor.app` | REST API по [../api/advisor.openapi.yaml](../api/advisor.openapi.yaml) |
| `capl.state` | Состояние в памяти: активные intents, назначение слотов, кольцевые буферы коммитов по таблице и партиции, интенсивности; периодический сброс в Postgres |
| `capl.features` | Вычисление признаков по `config/features.yaml`; общий для online/offline/sim |
| `capl.models.analytic` | Пуассоновская модель $P_{K2}$, $P_{K1}$ |
| `capl.models.ml` | Загрузка и инференс LightGBM, калибровка |
| `capl.policies` | P0–P6, `explore`; интерфейс `decide(intent, state, models) -> Decision` и `hold(commit, state) -> HoldDecision` |
| `capl.conflicts` | Загрузка `conflict_matrix.yaml`, функции `conflicts(a, b, isolation) -> {none, K1, K2_a_loses, K2_b_loses, K2_symmetric}` |
| `capl.telemetry` | Асинхронная запись в Postgres (`asyncpg`), батчами |
| `capl.metrics` | Prometheus-клиент |

Модель конкуренции: прокси не сериализует коммиты; каждое удержание — отдельная корутина. Состояние изменяется только в event loop (без блокировок). Тяжёлый инференс выполняется в `ThreadPoolExecutor`.

Отказоустойчивость: обращение прокси к advisor внутри процесса — вызов функции с таймаутом 20 мс; при исключении — `pass`. Если сервис `capl` недоступен целиком, runner может быть переключён в режим `bypass` (`CATALOG_URI` напрямую на каталог) — это и есть контрольная конфигурация для AC-5.

## 5. Структура репозитория

```text
nir/
├── README.md                     # тема (версия 2)
├── AGENTS.md                     # инструкции для нейросетевого ассистента
├── Makefile
├── pyproject.toml
├── docs/
│   ├── 00_validation_report.md
│   ├── CHANGELOG.md
│   ├── decisions/                # ADR
│   └── stage-N-report.md
├── tz/                           # это ТЗ
├── api/advisor.openapi.yaml
├── schemas/                      # JSON Schema событий
├── config/
│   ├── capl.yaml
│   ├── conflict_matrix.yaml
│   ├── features.yaml
│   ├── simulator.yaml
│   ├── workloads/S0_baseline.yaml … S6_mixed_drift.yaml
│   └── experiments/matrix.yaml
├── infra/
│   ├── docker-compose.yml
│   ├── .env.example
│   ├── capl/Dockerfile
│   ├── spark/{Dockerfile,spark-defaults.conf,fairscheduler.xml}
│   ├── flink/{Dockerfile,sql/streaming_append.sql,sql/streaming_upsert.sql}
│   ├── seaweedfs/s3.json
│   ├── toxiproxy/toxiproxy.json
│   ├── postgres/init/01_telemetry.sql
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/...
├── src/
│   ├── capl/                     # сервис (см. §4)
│   ├── runner/
│   │   ├── main.py               # CLI: run --scenario --policy --seed
│   │   ├── scenario.py           # загрузка и валидация YAML (pydantic)
│   │   ├── datagen.py            # детерминированная генерация данных
│   │   ├── ops/                  # append, merge_cow, merge_mor, delete, overwrite, rewrite, expire, orphan
│   │   ├── slots.py              # пул Spark-каталогов со своими заголовками
│   │   ├── streaming.py          # Flink-отправка или Spark Structured Streaming
│   │   ├── producer.py           # генератор Kafka
│   │   ├── cost.py               # Spark REST API → CPU/run time по job group
│   │   ├── health.py             # снимки table_health
│   │   ├── invariants.py         # проверка корректности
│   │   └── orphans.py            # подсчёт orphan-файлов
│   ├── sim/
│   │   ├── engine.py             # SimPy-модель
│   │   ├── table.py              # партиции, файлы, sequence numbers
│   │   ├── catalog.py            # CAS-сервер, повторы
│   │   ├── workload.py           # те же сценарии YAML
│   │   └── calibrate.py
│   ├── ml/
│   │   ├── dataset.py            # point-in-time сборка обучающей выборки
│   │   ├── train.py
│   │   ├── evaluate.py
│   │   └── registry.py
│   └── analysis/
│       ├── metrics.py
│       ├── stats.py              # bootstrap CI, Mann–Whitney
│       └── figures.py
├── tests/
│   ├── unit/
│   ├── integration/              # требуют поднятого стенда
│   └── e2e/
├── data/traces/                  # экспорт телеметрии (Parquet), не в git (DVC/архив)
├── models/
└── results/
```

## 6. Ключевые архитектурные решения (ADR, кратко)

| ADR | Решение | Причина |
| --- | --- | --- |
| ADR-001 | Iceberg + REST-каталог как объект | Открытый протокол коммита; единая точка для прокси; поддержка всеми движками |
| ADR-002 | Двухуровневое управление (advisor + proxy) | Потери предотвращаются только при управлении запуском; прокси даёт наблюдаемость и выбор проигравшего |
| ADR-003 | Прокси только задерживает | Гарантирует сохранение изоляции; корректность Iceberg не зависит от времени доставки запросов |
| ADR-004 | Метки исходов — на стороне runner | Конфликт данных обнаруживается клиентом, каталог его не видит |
| ADR-005 | Привязка коммитов через слоты с заголовками `header.X-CAPL-Slot` | Свойства снапшота задаются не для всех SQL-операций; статический заголовок на экземпляр каталога поддерживается REST-клиентом Iceberg |
| ADR-006 | Один долгоживущий Spark-драйвер с FAIR-пулами вместо множества приложений | Экономия памяти на одном ПК; семантика конфликтов не зависит от того, в одном ли JVM выполняются операции (каждая имеет свой базовый снапшот и валидацию) |
| ADR-007 | SeaweedFS вместо MinIO | Community-редакция MinIO не поддерживается с 2025–2026 гг. |
| ADR-008 | Один код политик для advisor и симулятора | Исключает расхождение поведения «стенд/симуляция» |
| ADR-009 | Python для CAPL | Частота коммитов (до десятков в секунду) не требует нативной производительности; общий стек с ML |
