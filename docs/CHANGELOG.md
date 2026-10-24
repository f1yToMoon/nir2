# CHANGELOG

## 2026-10-24 — этап 1, итерация 5 (ответ на ревью stage-1-r3)

- SQL `INSERT OVERWRITE … WITH ('isolation-level', 'validate-from-snapshot-id')` (`insert_overwrite_opts`): K2 20/20, контроль K1; утверждение «SQL не передаёт опции записи» снято (ADR-006, отчёт G1).
- `insert_overwrite(_opts)` сопоставлены классу `overwrite` в метрике; контроль сценарных пар засевается двумя файлами на партицию.
- Полный прогон `results/stage-1/iter5/` (188 парных кейсов, 4700/4700 повторов валидны); G1-2 на исходных классах — 72,9 % / 80,0 %.
- S1-R3-02 решено автором (`1885bc5`).

## 2026-10-24 — этап 1, итерация 4 (ответ на ревью stage-1-r2)

- Механизм `overwrite` исправлен: валидацию `ReplacePartitions` включает одна опция `isolation-level`; без `validate-from-snapshot-id` — ложный К2 без параллельных операций (опыт `test_overwrite_iso_only`, 10/10). Риск S4 в отчёте G1 переписан.
- Основная метрика G1-2 — на исходных классах операций гипотезы: 72,9 % (`c6e9a4e`), 80,0 % (`fd593a5`).
- Новые кейсы: SQL `INSERT OVERWRITE … PARTITION` (K1), компакция ⇄ MERGE / MoR UPDATE на схеме сценариев (K2 20/20); класс исхода `base_expired`; попытки коммита в сценарных повторах.
- Полный прогон `results/stage-1/iter4/` (186 кейсов, 4597/4600 повторов валидны) воспроизводит исходы `iter3`.

## 2026-10-23 — этап 1, итерация 3 (ответ на ревью stage-1-r1)

- Валидность повтора — по артефактам: тело коммита в журнале прокси (`src/capl/digest.py`; тело пересылается без изменений), summary снапшота (`src/runner/ops/effects.py`), следы из манифестов (py4j `SnapshotParser`), окно удержания по меткам времени прокси.
- Тест матрицы: частичный MoR DELETE, MERGE с целевым предикатом, `expire` с `older_than`, `append_expire`, `stream_upsert` через Java RowDelta с equality deletes, `overwrite_opts` от текущего снапшота, `rewrite_deletes` на MoR-таблице, контроль `K2 == 0`, кластер Spark, `conflictDetectionFilter` из commit message, сценарные пары S2/S3/S5; v2 и v3.
- Классификатор исходов — по корневому исключению Iceberg (`src/runner/ops/errors.py`).
- Прогон `make matrix-verify NAME=iter3`: 174 кейса, 4350 повторов, все валидны; G1-1 4/4, G1-2 84,0 % против исходной гипотезы (`c6e9a4e`), 88,9 % против `fd593a5`.
- ADR-006 — исправления матрицы; `docs/gate-G1-report.md` — рекомендация CORRECT, решение автора не принято.
- Результаты итераций 2 и 3 и пилотов в `results/stage-1/iter{2,3}/`; unit-тесты (19).

## 2026-10-23 — автоматизированное ревью этапов

- `docs/REVIEWER.md`: принципы приёмки P1–P13 (собраны из ручных ревью этапов 0–1), уровни замечаний, чек-листы этапов 0–9, процедура и шаблон ревью.
- Субагент `.claude/agents/stage-reviewer.md`, скиллы `/stage <N>` и `/review <N>`.
- Хуки `.claude/settings.json` → `scripts/hooks/`: запрет тега `stage-N` без принятого ревью текущего HEAD и без решения автора на G1/G2; запрет агенту отмечать решение автора и менять ревью; статус этапов при старте сессии.
- `scripts/fetch_iceberg_src.sh`: исходники Iceberg версии стенда в `third_party/` для проверки утверждений по коду.
- `docs/reviews/stage-1-r1.md`: ручное ревью итерации 2 этапа 1 (CHANGES_REQUESTED, 6 BLOCKER, 3 MAJOR).

## Этап 1 (2026-10-23)

**Матрица конфликтов v2 (20 повторов на пару):**

Итерация 1 завершена. G1 не принят — ADR-004 и ADR-005 отклонены (противоречат SparkWrite.java:347-358 и SparkPositionDeltaWrite.java:266-284). Тест не устанавливал опции overwrite (isolation-level + validate-from-snapshot-id) и не обеспечивал попадание committed-данных под conflictDetectionFilter mor_dml. Match rate против исходной гипотезы: 63.3% (19/30, 9 not-conclusive). Критерий 1 PASS (3/4 групп). Требуется итерация 2 (§7 stage-1-report.md). Новая находка: expire→success (подтверждено, матрица обновлена). ADR-004, ADR-005 переведены в статус «Отклонено».

**Компоненты этапа 1:**

- `tests/integration/test_conflict_matrix.py`: параметризованный тест (hold-механизм, timestamp-based K1-detection, validity tracking).
- `src/runner/ops/errors.py`: классификатор K1/K2/unknown.
- `scripts/update_matrix.py`: заполнение `verified:` + тепловая карта.
- `src/capl/main.py`: `ts`, `status`, `hold_ms` в `_last_commits`; `DELETE /_capl/debug/last-commits`.
- `infra/spark/Dockerfile`: `pytest>=8.3`, `matplotlib>=3.9`.

**ADR-004:** `ReplacePartitions` (INSERT OVERWRITE) не производит data-level валидацию в retry; `write.delete.isolation-level=serializable` влияет только на `OverwriteFiles`.

---

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
