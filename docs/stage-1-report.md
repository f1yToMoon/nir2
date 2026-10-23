# Отчёт по этапу 1: Эмпирическая матрица конфликтов — итерация 1

| Поле | Значение |
| --- | --- |
| Даты | 2026-10-23 |
| Коммит / тег | `1db94e2` / `stage-1` |
| Версии | Клиент Iceberg 1.12.0, сервер каталога b0df3ca (2026-04-29 nightly) |
| Статус | Итерация 1 завершена. G1 не принят — требуется итерация 2 (§7) |

## 1. Цель этапа

Подтвердить или исправить `config/conflict_matrix.yaml` для Iceberg 1.12.0 (формат v2) —
зафиксировать тексты исключений; оценить критерии G1.

## 2. Что сделано

- `src/capl/main.py`: hold-механизм (`POST /v1/debug/hold`, `DELETE /v1/debug/hold/{slot}`);
  `_last_commits` с полями `ts`, `status`, `hold_ms`; `DELETE /_capl/debug/last-commits`.
- `tests/integration/test_conflict_matrix.py`: параметризованный тест (20 overlap + 5 control);
  синхронизация по timestamp; K1-детекция по HTTP 409 в прокси; validity tracking.
- `src/runner/ops/errors.py`: классификатор K1/K2/unknown.
- `scripts/update_matrix.py`: заполнение `verified:`, тепловая карта, G1-оценка.
- `infra/spark/Dockerfile`: `pytest>=8.3`, `matplotlib>=3.9`.

## 3. Ограничения итерации 1 (причина отклонения G1)

### 3.1 overwrite — опции не установлены

Spark INSERT OVERWRITE (`DataFrameWriterV2.overwritePartitions()`) использует `DynamicOverwrite`.
`SparkWrite.java:347-358`: data-level валидация включается **только** при наличии опций
`.option("isolation-level","serializable").option("validate-from-snapshot-id", <id снапшота чтения>)`.
Итерация 1 не устанавливала эти опции → overwrite всегда выполнялся без `validateFromSnapshot` → K1.

### 3.2 mor_dml — conflictDetectionFilter не покрывал committed-данные

`SparkPositionDeltaWrite.java:266-284`: при `isolation=serializable` вызывается
`validateNoConflictingDataFiles()` с `conflictDetectionFilter` из `WHERE`-предиката запроса.
K2 возникает **только если** committed-данные попадают под этот фильтр.
В итерации 1 committed-операции (append/su/overwrite) добавляли данные в тот же bucket,
но не обязательно под `WHERE`-предикат pending `mor_dml` → K1 (артефакт данных, не поведение Iceberg).

### 3.3 Другие ограничения

- Snapshot isolation: тест не устанавливал `write.delete.isolation-level=SNAPSHOT` →
  `cow_dml×{append,su}(snapshot)` прогонялся с serializable-default → K2 вместо K1.
- rewrite_deletes: таблица без prior position deletes → `overlap={}` для всех non-MoR committed.
- k2_control assertion: `rewrite_data` глобальна → control trials дают K2 → pytest FAIL
  (данные overlap верны: K2=20/20).

## 4. Эмпирические результаты (итерация 1, v2, serializable если не указано иное)

| Пара (pending × committed) | Гипотеза | Факт (overlap) | Rate | Статус |
| --- | --- | --- | --- | --- |
| append × append | K1 | K1 (20/20) | 100% | ✓ совпадение |
| stream_upsert × append | K1 | K1 (20/20) | 100% | ✓ |
| overwrite × append/su/ow/cow/mor/rw (serial) | K2 | K1 (20/20) | 0% | ✗ опции не устан. (§3.1) |
| overwrite × append/su (snapshot) | K1 | K1 (20/20) | 100% | ✓ |
| overwrite × append (default) | K2 | K1 (20/20) | 0% | ✗ опции не устан. |
| cow_dml × append (serial) | K2 | K2 (20/20) | 100% | ✓ |
| cow_dml × stream_upsert (serial) | K2 | K2 (20/20) | 100% | ✓ |
| cow_dml × overwrite (serial) | K2 | K2 (20/20) | 100% | ✓ |
| cow_dml × append (snapshot) | K1 | K2 (20/20) | 0% | not-conclusive (isolation не тест.) |
| cow_dml × stream_upsert (snapshot) | K1 | K2 (20/20) | 0% | not-conclusive |
| cow_dml × cow_dml (serial) | K2 | K2 (20/20) | 100% | ✓ |
| cow_dml × rewrite_data (serial) | K2 | K2 (20/20) | 100% | ✓ |
| cow_dml × mor_dml (serial) | K2 | K2=10,K1=10 | 50% | not-conclusive (file-overlap) |
| mor_dml × append (serial) | K2 | K1 (20/20) | 0% | ✗ conflictFilter не покрыл (§3.2) |
| mor_dml × stream_upsert (serial) | K2 | K1 (20/20) | 0% | ✗ |
| mor_dml × overwrite (serial) | K2 | K1=19,K2=1 | 5% | ✗ |
| mor_dml × append (snapshot) | K1 | K1 (20/20) | 100% | ✓ |
| mor_dml × stream_upsert (snapshot) | K1 | K1 (20/20) | 100% | ✓ |
| mor_dml × cow_dml (serial) | K2 | K2 (20/20) | 100% | ✓ |
| mor_dml × rewrite_data (serial) | K2 | K2 (20/20) | 100% | ✓ |
| mor_dml × mor_dml (serial) | K2 | K1 (20/20) | 0% | ✗ conflictFilter не покрыл (§3.2) |
| rewrite_data × append | K1 | K1 (20/20) | 100% | ✓ |
| rewrite_data × stream_upsert | K1 | K1 (20/20) | 100% | ✓ |
| rewrite_data × overwrite | K2 | K2 (20/20) | 100% | ✓ |
| rewrite_data × cow_dml | K2 | K2 (20/20) | 100% | ✓ G1 Группа 1 |
| rewrite_data × rewrite_data | K2 | K2 (20/20) | 100% | ✓ |
| rewrite_data × mor_dml | K2 | K2=10,K1=10 | 50% | not-conclusive (file-overlap) G1 Группа 2 |
| rewrite_deletes × mor_dml | K2 | K2=5,K1=11 | 31% | not-conclusive (file-overlap) |
| rewrite_deletes × rw/cow | K2 | {} (0/20) | — | not-conclusive (no prior position deletes) |
| rewrite_deletes × append/su | K1 | {} (0/20) | — | not-conclusive (no prior position deletes) |
| expire × append | K1 | success (20/20) | — | новая находка (§5) |

> Полная таблица: `results/stage-1/matrix_results.json`; тепловая карта: `results/stage-1/heatmap.png`.

### 4.1 G1 критерий 1 (K2 в ≥3/4 групп) — против исходной гипотезы

| # | Группа | Пара | K2/20 | Итог |
| --- | --- | --- | --- | --- |
| 1 | Компакция × CoW DML | rewrite_data × cow_dml | 20/20 | ✓ ПОДТВЕРЖДЕНО |
| 2 | Компакция × MoR DML | rewrite_data × mor_dml | 10/20 | ✗ (file-overlap ненадёжен) |
| 3 | CoW DML × append (serial) | cow_dml × append | 20/20 | ✓ ПОДТВЕРЖДЕНО |
| 4 | CoW DML × CoW DML | cow_dml × cow_dml | 20/20 | ✓ ПОДТВЕРЖДЕНО |

> **3/4 групп → Критерий 1 PASS**

### 4.2 G1 критерий 2 (≥95% совпадений) — против исходной гипотезы

| Всего | Not-conclusive | Оценивается | Match | Mismatch | Match rate |
| --- | --- | --- | --- | --- | --- |
| 39 | 9 | 30 | 19 | 11 | 63.3% |

> **63.3% < 95% → Критерий 2 FAIL → G1 не принят**

11 расхождений: 7 `overwrite×*` (опции не установлены) + 3 `mor_dml×{append,su,overwrite}` + 1 `mor_dml×mor_dml` (conflictDetectionFilter не покрывал committed-данные).

### 4.3 K1 vs success: протокол определения

K1 детектируется через прокси: `status=409` в `_last_commits` с `ts ≥ trial_start_ts`. `AppendFiles`/`ReplacePartitions` при 409 молча повторяют коммит — Spark-код видит `success`. `OverwriteFiles` при K2 бросает `ValidationException` — видно в Spark.

## 5. Новые находки

### expire → success (подтверждённо, итерация 1)

`expire×append` дал `success` (20/20). `ExpireSnapshots` коммитит через `ManageSnapshots` —
без `validateFromSnapshot()`, не затрагивает data-файлы. Нет OCC-конфликта с append.
Матрица обновлена: `expire×* → success`. Не зависит от вопроса ADR-004/ADR-005.

### rewrite_deletes — требует prior position deletes

`rewrite_deletes` работает только при наличии position-delete файлов в таблице
(создаются `mor_dml`). Для пар без prior `mor_dml` — 0 valid trials, `overlap={}`.
В итерации 2: пересоздавать таблицу с MoR-данными перед каждым повтором.

## 6. Деления по типу пересечения

Итерация 1 тестировала partition-overlap (тот же bucket). Для `mor_dml` необходим
`predicate`-тип: committed-данные должны попадать под `WHERE`-предикат pending-запроса.
Матрица получает новый тип пересечения `predicate` для `mor_dml×*` правил.

## 7. Требования к итерации 2

### 7.1 overwrite
- Вариант A: без опций (ожидание K1)
- Вариант B: `.option("isolation-level","serializable").option("validate-from-snapshot-id", <snapshot_id>)` (ожидание K2)
- Вариант C: snapshot isolation (`write.delete.isolation-level=SNAPSHOT`)

### 7.2 MoR DML — разделить на mor_delete, mor_update, mor_merge
Каждый в двух вариантах данных:
- (а) committed-данные попадают под WHERE-условие pending → ожидание K2 при serializable
- (б) тот же bucket, другой диапазон id → ожидание K1

### 7.3 CoW DML — UPDATE и MERGE раздельно (аналогично)

### 7.4 Ужесточить валидность повтора
Повтор засчитывается только если:
- pending и committed реально изменили данные: проверять по snapshot summary
  (`deleted-records`, `added-position-deletes`, `added-delete-files`, `deleted-data-files`)
  в журнале прокси и истории таблицы
- для file-overlap пар: множества файлов действительно пересекаются (по манифестам)

### 7.5 Данные
- Генерировать заново для каждого повтора; не исчерпывать
- Пересоздавать таблицу или уникальные диапазоны id на повтор

### 7.6 Snapshot isolation
Устанавливать `write.delete.isolation-level=snapshot` как table property.

### 7.7 rewrite_deletes
Таблица в режиме MoR, создание position-delete файлов перед каждым повтором.

### 7.8 Контроль для компакции
`rewrite_data_files(..., where => 'bkt = <другой_bucket>')` → контрольные K2 = 0.

### 7.9 Format v3
Полный прогон, включая `mor×mor` и `rewrite×mor` с deletion vectors.

### 7.10 Сценарные пары
На схеме `dt × bucket(8, key)`: операции из `config/workloads` (merge_upsert, update_status,
delete_by_keys, компакция с `where` по dt и bucket) против append из потока.
Зафиксировать `conflictDetectionFilter` из commit message Spark — определяет реальную
частоту K2 в сценариях S2–S5.

## 8. Следующий этап

Этап 1, итерация 2 — повторный прогон с исправленной методологией теста.
Блокер: решение G1 (принять итерацию 2 / PIVOT).
