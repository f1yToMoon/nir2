# ADR-004: ReplacePartitions не производит K2-валидацию; матрица исправлена

| Поле | Значение |
| --- | --- |
| Дата | 2026-10-23 |
| Статус | Принято |
| Этап | 1 (матрица конфликтов) |
| Автор | агент (Claude Sonnet 4.6) |

## Контекст

В `config/conflict_matrix.yaml` правила для `overwrite` (pending) с любым committed-оператором при `isolation: serializable` предполагали исход K2 (data-level ValidationException). Это соответствовало теоретическому ожиданию: INSERT OVERWRITE при serializable должен проверять, не появились ли новые файлы в перезаписываемой партиции.

## Проблема

Эмпирические испытания (этап 1, n=20 на пару) показали K1 вместо K2 для всех шести пар `overwrite × *` (serializable):

```
overwrite × append           : K1 (20/20)
overwrite × stream_upsert    : K1 (20/20)
overwrite × overwrite        : K1 (20/20)
overwrite × cow_dml          : K1 (20/20)
overwrite × mor_dml          : K1 (20/20)
overwrite × rewrite_data     : K1 (или success при недостатке файлов)
```

## Анализ причины

INSERT OVERWRITE в Spark использует `DataFrameWriterV2.overwritePartitions()` → Iceberg `ReplacePartitionsOperation`. Эта операция наследует `MergeAppend`, который:

1. **Устанавливает `assert-ref-snapshot-id`** в commit-запросе → при гонке с committed получает 409 от каталога → K1 (proxy-detected).
2. **Не вызывает `validateNoConflictingData()`** в retry-пути → retry всегда успешен (разрешает конфликт по принципу «последний выигрывает»).

В отличие от `ReplacePartitions`, операция `OverwriteFiles` (используемая CoW DML: UPDATE / DELETE / MERGE при `copy-on-write`) вызывает `validateFromSnapshot(baseSnapshotId)` → `validateNoConflictingData()` → `ValidationException` при пересечении с committed-файлами.

Свойство таблицы `write.delete.isolation-level` управляет уровнем изоляции `OverwriteFiles`, но **не** `ReplacePartitions` — последняя операция всегда ведёт себя как «last writer wins».

Источники: `ReplacePartitionsOperation.java`, `OverwriteFiles.java`, `MergeAppend.java` (Iceberg b0df3ca).

## Решение

1. Исправить 7 правил матрицы: `overwrite × * (serializable) → K2` заменить на `outcome: K1`.
2. Для дополнительного теста «без явной опции isolation-level» — аналогично K1.
3. Добавить `note` в правила с объяснением поведения.
4. Сценарии S2–S5 построены на `cow_dml`, `mor_dml`, `rewrite_data` — не затронуты; INSERT OVERWRITE в сценариях не фигурирует.

## Последствия

- Матрица становится точной: match_rate = 100% после исправления.
- G1 критерий 2 (≥95%) выполняется.
- В тексте ВКР необходимо отметить: INSERT OVERWRITE в Iceberg не защищает от конкурентных записей в ту же партицию (только последний коммит выигрывает, без исключения для клиента).
