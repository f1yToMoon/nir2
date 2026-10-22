# CHANGELOG

## Этап 0 — итерация 3 (2026-10-23) · пред-этап-1

**Измерение памяти с рабочей конфигурацией:**

Повторный запуск `scripts/peak_load.py` с `executor.memory=2g, driver.memory=3g, cores.max=8`,
нагрузка: 10 М строк `PARTITIONED BY (bucket(8,id))` + `MERGE` 1 М строк (shuffle), одновременно
Kafka→Flink→Iceberg (3 чекпойнта). Пиковый RSS: **~9 715 МиБ (~9,5 ГиБ)**. Таблица
рекомендаций по RAM: lite ≥ 8 ГиБ, full-тест ≥ 16 ГиБ, full-work ≥ 16 ГиБ (рек. 32 ГиБ).

**Lakekeeper v0.14.0:**

Подтверждён `docker manifest inspect quay.io/lakekeeper/catalog:v0.14.0` — manifest list
amd64: `sha256:1f566a70…`, arm64: `sha256:8f9872941c…`.

**Smoke-тест расширен до 11 проверок:**

Добавлен `0.4b s3_put_get` — PUT/GET объекта через Toxiproxy (PyArrow S3FileSystem).

**CAPL proxy — hold-механизм:**

`POST /v1/debug/hold` и `DELETE /v1/debug/hold/{slot}` на advisor (port 8190):
удержание коммита слота до сигнала теста. Используется в интеграционных тестах этапа 1.

**ADR-001 обновлён:**

Удалена фраза о «≥ 1.11.0»; версия сформулирована как «нерелизный снимок `main` от 29.04.2026 (`b0df3ca`)».

**CHANGELOG перемещён:**

Корневой `CHANGELOG.md` удалён, файл перемещён в `docs/CHANGELOG.md` (в соответствии с ТЗ).

**Исправления отчёта:**

- Расхождение 5 611 / 5 527 объяснено (сумма поконтейнерных максимумов vs одновременный пик).
- Py4j: описание исправлено — захардкожен как `py4j-0.10.9.9-src.zip` в docker-compose.yml.

---

## Этап 0 — итерация 2 (2026-10-23)

**Smoke-тест расширен до 10 проверок (было 9):**

- **0.8 Flink**: добавлены assert `len(snapshots) >= 3` и проверка медианного интервала
  между снапшотами (5 с ± 50 %); исправлен заголовок `X-CAPL-Writer = 'stream'`
  (было `'flink-stream'`, не соответствовало enum в `schemas/commit_event.schema.json`).
- **0.9 Bucket**: после `rewrite_data_files` проверяются счётчики файлов по бакетам
  через `smoke.rewrite_test.files` — бакет 1: 1 файл, остальные 7: по 4 файла.
- **0.10 Spark-кластер** (новая проверка): запись в Iceberg и `rewrite_data_files`
  через `spark://spark-master:7077`; валидация участия воркеров по Spark REST API
  (`/api/v1/applications/{appId}/executors`).

**Пиковая память под конкурентной нагрузкой:**

Одновременный запуск Kafka→Flink→Iceberg (3 чекпойнта, 5 с) и тяжёлой Spark-задачи
(5 М строк, `spark://spark-master:7077`). Docker stats сэмплированы каждые 5 с (105
снимков). Пик: **~5 527 МиБ (~5,4 ГиБ)** суммарно; детали — в отчёте, раздел 4.1.

**ADR-001 обновлён:**

Добавлены: digest образа `sha256:db8de90b…`, фактическая версия Iceberg в образе
(`b0df3ca`, 2026-04-29, нерелизный снимок `main`), обоснование допустимости
(детектирование конфликтов OCC на стороне клиента 1.12.0).

**Вспомогательные изменения:**

- `scripts/peak_load.py` — скрипт конкурентной нагрузки для измерения памяти.
- `scripts/smoke.py`: добавлен `import statistics`; константа `SPARK_MASTER`.

---

## Этап 0 — итерация 1 (2026-10-22)

Первоначальный запуск этапа 0. Smoke 9/9, тег `stage-0` @ `fa7a74d`.

Ключевые исправления стека: права чекпойнтов Flink (`flink-init`, chown → uid=9999);
постоянный том каталога (`catalog-init`, chown → uid=1000); SeaweedFS 3.97 → 4.48
(ADR-003); реальный поток Kafka→Flink→Iceberg с assert `flink.job-id`; подтверждены
имена таймаут-свойств REST-клиента Iceberg 1.12.0 по `HTTPClient.java`.

ADR: ADR-001 (образ каталога без числового тега), ADR-002 (постоянный том SQLite),
ADR-003 (SeaweedFS 4.48).
