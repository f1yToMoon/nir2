# ADR-005: RowDelta не валидирует новые записи данных (ОТКЛОНЕНО)

**Статус:** Отклонено (2026-10-23)  
**Причина:** Противоречит SparkPositionDeltaWrite.java:266-284  
**Дата:** 2026-10-23  
**Связано с:** ADR-004

---

## Причина отклонения

`SparkPositionDeltaWrite.java`, строки 266–284: при `isolation=serializable` **вызывается** `validateNoConflictingDataFiles()` с `conflictDetectionFilter`, построенным из условия запроса (`WHERE`-предиката). K2 возникает только если committed-данные попадают под этот фильтр.

Итерация 1 теста не учитывала это условие: `committed`-операции (append/su/overwrite) добавляли данные в тот же bucket, но не обязательно с id, попадающими под `WHERE`-предикат pending `mor_dml`. Эмпирически K1 — артефакт данных теста, не поведение Iceberg.

`validateNoConflictingDeleteFiles` — отдельная проверка только для UPDATE/MERGE (не для DELETE).

## Контекст (итерация 1)

Матрица прогнозировала K2 для `mor_dml × append`, `mor_dml × stream_upsert`, `mor_dml × overwrite` при `isolation=serializable`.
Гипотеза остаётся в силе: K2 при правильном тесте (committed-данные под conflictDetectionFilter).

## Эмпирический результат

| Пара (pending × committed)           | Ожидалось | Факт (20 повторов) |
|--------------------------------------|-----------|---------------------|
| `mor_dml × append` (serializable)    | K2        | K1 (20/20)          |
| `mor_dml × stream_upsert` (serial.)  | K2        | K1 (20/20)          |
| `mor_dml × overwrite` (serial.)      | K2        | K1 (19/20, 1 K2)    |

## Анализ

`RowDelta.validateFromSnapshot()` в Iceberg 1.12.0 (nightly b0df3ca) вызывает:

- `validateNoConflictingDeleteFiles(...)` — проверяет, что никакая параллельная
  операция не удалила/переписала **файлы данных**, на которые ссылаются
  позиционные deletes (именно это даёт K2 при `mor_dml × rewrite_data`).
- **НЕ** вызывает `validateNoConflictingAppends(...)` — проверку новых файлов
  в сканированных партициях.

Таким образом, `mor_dml` конфликтует только с операциями, удаляющими/
переписывающими уже существующие data-файлы, но не с добавлением новых данных.
`append`, `stream_upsert`, `overwrite` лишь добавляют файлы — конфликта нет → K1.

Единственный случай K2 в `mor_dml × overwrite (1/20)` объясняется редким
пересечением на уровне файлов (overwrite удаляет файл, на который ссылается
позиционный delete) — случайный overlap файлов, не систематический.

## Решение

Скорректировать 3 правила матрицы:

```yaml
# было:
- {pending: mor_dml, committed: [append, stream_upsert, overwrite],
   outcome: K2, overlap: partition, when: {isolation: serializable}}

# стало:
- {pending: mor_dml, committed: [append, stream_upsert, overwrite],
   outcome: K1, overlap: none,
   note: "RowDelta не вызывает validateNoConflictingAppends (ADR-005); K2 только при удалении ref-файлов"}
```

## Влияние на G1

- Группы G1-критерия 1, подтверждённые K2 ≥ 95%: **компакция×CoW DML** (✓), **CoW DML×append** (✓), **CoW DML×DML** (✓) — **3/4 PASS**.
- Группа компакция×MoR DML: `rewrite_data×mor_dml` = K2=10/20 (50% — механизм подтверждён, file-overlap ненадёжен). Не считается полностью подтверждённой.
- Итог: **3/4 групп** — Критерий 1 выполнен (нужно ≥3).

## Дополнительная находка: ×mor_dml file-overlap issue

Кроме расхождений ADR-004/ADR-005, полный прогон выявил ненадёжность K2 для всех
пар с committed=`mor_dml`, когда K2 требует file-level overlap:

| Пара | K2/всего valid | % K2 |
|------|----------------|------|
| `cow_dml×mor_dml` | 10/20 | 50% |
| `rewrite_data×mor_dml` | 10/20 | 50% |
| `rewrite_deletes×mor_dml` | 5/16 | 31% |

Механизм K2 **существует** (эмпирически подтверждён в 31–50% случаев), но
bucket-partition overlap в тесте не гарантирует, что committed position-deletes
ссылаются на те же data-файлы, которые читает pending-операция. Это ограничение
тестового стенда, а не ошибка предсказания матрицы.
