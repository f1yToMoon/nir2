# Отчёт по этапу 1: Эмпирическая матрица конфликтов — итерация 2

| Поле | Значение |
| --- | --- |
| Даты | 2026-10-23 |
| Коммит | `e55e25b` (iter2) |
| Версии | Клиент Iceberg 1.12.0, сервер каталога b0df3ca (2026-04-29 nightly) |
| Статус | Итерация 2 завершена. G1 принят — GO |

## 1. Цель этапа

Подтвердить или исправить `config/conflict_matrix.yaml` для Iceberg 1.12.0 (формат v2) —
зафиксировать тексты исключений; оценить критерии G1.

## 2. Что сделано

- `src/capl/main.py`: hold-механизм (`POST /v1/debug/hold`, `DELETE /v1/debug/hold/{slot}`);
  `_last_commits` с полями `ts`, `status`, `hold_ms`.
- `tests/integration/test_conflict_matrix.py`: параметризованный тест (20 overlap + 5 control);
  hold-синхронизация; K1-детекция по HTTP 409 в прокси; per-trial уникальные id-диапазоны.
- `src/runner/ops/errors.py`: классификатор K1/K2/unknown.
- `scripts/update_matrix.py`: заполнение `verified:`, тепловая карта, G1-оценка.
- `infra/spark/Dockerfile`: `pytest>=8.3`, `matplotlib>=3.9`.
- `config/conflict_matrix.yaml`: разбивка `mor_dml` → `mor_delete/mor_update/mor_merge`; правила expire.
- Архив результатов итерации 1: `results/stage-1/iter1/`.

## 3. Исправленная методология (итерация 2)

### 3.1 Уникальные id-диапазоны

Итерация 1 использовала глобальные данные (`DELETE WHERE id < seq*5+5`), что приводило к
исчерпанию строк на поздних повторах (§4). Итерация 2: каждый повтор получает свой диапазон
`[seq × 10000 + bkt × 1000000, seq × 10000 + bkt × 1000000 + 49]`.

### 3.2 Predicate-overlap для DELETE/UPDATE

Pending DELETE/UPDATE использует `WHERE bkt={bkt} AND id BETWEEN lo AND lo+49`.
Committed append вставляет строки `lo..lo+9` — внутри predicate-диапазона.

### 3.3 Разбивка mor_dml

`mor_dml` (общий alias для MoR-DELETE) разбит на:
- `mor_delete` — SQL DELETE (RowDelta position-delete, нет validateNoConflictingDataFiles)
- `mor_update` — SQL UPDATE (RowDelta + validateNoConflictingDataFiles + validateNoConflictingDeleteFiles)
- `mor_merge`  — SQL MERGE (аналогично UPDATE)

### 3.4 overwrite_opts

Добавлен op-класс `overwrite_opts`: `overwritePartitions()` с
`.option("isolation-level","serializable").option("validate-from-snapshot-id",<snap_id>)`.

### 3.5 Validity — committed_ok only

Признак валидного повтора: `held=True AND committed_ok=True`. Зависимость от `_data_changed`
(флаг из snapshot summary) убрана: он давал ложные отрицания из-за кэширования метаданных Spark.

## 4. Ключевые находки итерации 2

### 4.1 MoR DELETE не валидирует data-файлы

`SparkPositionDeltaWrite` для SQL DELETE не вызывает `validateNoConflictingDataFiles`
(или использует null scan). Следствие: pending MoR DELETE не бросает K2 ни при каком
committed-операторе. Итог:

- `mor_delete×{append,stream_upsert,overwrite,cow_dml,rewrite_data,mor_update,mor_merge}` → **K1**
- `mor_delete×mor_delete` (v2 и v3) → **K1**
- `mor_dml×{append,stream_upsert,overwrite,cow_dml,rewrite_data,mor_dml}` → **K1**

MoR UPDATE и MERGE вызывают `validateNoConflictingDataFiles` + `validateNoConflictingDeleteFiles` →
K2 подтверждено 20/20 для всех протестированных пар.

### 4.2 expire не создаёт CAS-конфликт

`ExpireSnapshots` меняет metadata, но не current snapshot ID. OCC-проверка data-операций
основана на snapshot ID, поэтому expire как committed → **success** (нет CAS 409):
- `cow_dml×expire`, `mor_dml×expire`, `rewrite_data×expire` → success 20/20.

Аналогично: `expire×*` (pending expire) → **success** 20/20 (подтверждено итерацией 1).

### 4.3 snapshot-isolation для cow_dml

С `write.delete.isolation-level=snapshot` — `validateNoConflictingAppends` не вызывается:
- `cow_dml×{append,stream_upsert}×snapshot` → **K1** (20/20)

Итерация 1 показала K2 — методологическая ошибка (глобальные данные, нет predicate-overlap).

### 4.4 Исчерпание данных в итерации 1

Итерация 1: `DELETE WHERE id < seq*5+5` — на 10–20-м повторе удалять нечего:
committed INSERT добавлял строки вне DELETE-диапазона → нет predicate-overlap → K1 вместо K2.
Итерация 2: per-trial вставки, BETWEEN-предикат → результаты воспроизводимы.

### 4.5 overwrite_opts — низкое число валидных повторов

`overwrite_opts` даёт 2–5 валидных повторов из 20. Среди валидных: K2 100%.
Механизм подтверждён, но статистика слабее (< 20 повторов). Причина низкого valid:
hold не успевает захватить pending при определённом тайминге выполнения.

### 4.6 rewrite_deletes — нет валидных повторов

Ни один из 5 тест-кейсов `rewrite_deletes×*` не дал валидных результатов (valid=0).
Механизм создания prior position-delete файлов не обеспечивал их наличие
в нужном виде для `rewrite_position_delete_files`. Кейсы помечены not-conclusive.

## 5. Результаты итерации 2 (75 тест-кейсов)

| Категория | Кейсов | Матч |
| --- | --- | --- |
| Всего | 75 | — |
| rewrite_deletes (not-conclusive) | 5 | н/д |
| Оцениваемых | 70 | 100% (70/70) |

### 5.1 Подтверждённые K2-пары (20/20)

| Pending | Committed | Заметка |
| --- | --- | --- |
| cow_dml | append, stream_upsert, overwrite | serializable |
| cow_dml | cow_dml, rewrite_data | file-overlap |
| cow_dml | mor_dml | новые delete-файлы для читаемых файлов |
| mor_update | append, stream_upsert, overwrite | serializable |
| mor_update | cow_dml, rewrite_data, mor_delete | file/delete-overlap |
| mor_update | mor_update, mor_merge | predicate-overlap |
| mor_merge | (все выше) | аналогично mor_update |
| rewrite_data | overwrite, cow_dml, rewrite_data | file-overlap |
| rewrite_data | mor_dml | новые position-deletes |
| overwrite_opts | append, stream_upsert и др. | valid=2–5; K2 100% из valid |

### 5.2 K1-пары (не K2)

| Pending | Committed | Причина |
| --- | --- | --- |
| append, stream_upsert | * | не валидируют данные |
| overwrite (без опций) | * | DynamicOverwrite без validateFromSnapshot |
| mor_delete | * | SparkPositionDeltaWrite для DELETE — нет конфликтной валидации |
| mor_dml (DELETE) | * | то же |
| cow_dml | append, stream_upsert | isolation=snapshot |

### 5.3 success-пары

| Pending | Committed | Причина |
| --- | --- | --- |
| expire | * | ExpireSnapshots не вступает в OCC-конфликт |
| cow_dml, mor_dml, rewrite_data | expire | expire не меняет current snapshot ID |

## 6. Артефакты

- `results/stage-1/matrix_results.json` — 75 тест-кейсов, итерация 2
- `results/stage-1/heatmap.png` — тепловая карта match rate
- `results/stage-1/iter1/` — архив результатов итерации 1
- `config/conflict_matrix.yaml` → `verified:` — 75 записей с датой и метаданными
- `docs/gate-G1-report.md` — решение по G1

## 7. Тексты исключений

### K1 (CommitFailedException — прозрачный retry)

Прокси: HTTP 409 на первую попытку коммита.
Spark: retry без исключения (success outcome в Python).
В логах Iceberg: `org.apache.iceberg.exceptions.CommitFailedException`.

### K2 (ValidationException)

```
org.apache.iceberg.exceptions.ValidationException: Found conflicting files
  that can contain records matching ref(id) in [<snapshot_id>]: [<file_path>]
```

Пример для cow_dml×append:
```
ValidationException: Found conflicting append files that can contain records
  matching [bkt = 0, id >= 10000, id <= 10049]: [s3://warehouse/.../data/bkt=0/...parquet]
```
