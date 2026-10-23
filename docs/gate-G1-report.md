# G1 Gate Report — итерация 1

| Поле | Значение |
| --- | --- |
| Дата | 2026-10-23 |
| Связан с | `docs/stage-1-report.md` |
| ADR | ADR-004 (отклонено), ADR-005 (отклонено) |
| Коммит | `1db94e2`, тег `stage-1` |

---

## Рекомендация: G1 НЕ ПРИНЯТ

Критерий 1 выполнен (3/4 групп). Критерий 2 провален: 63.3% (19/30) против исходной гипотезы.
Причина — методологические ошибки теста (§3 отчёта), а не неверная гипотеза.
Требуется итерация 2 с исправленной методологией.

---

## Критерий 1: K2 подтверждён в ≥3/4 групп

| # | Группа | Пара | K2/20 | Итог |
| --- | --- | --- | --- | --- |
| 1 | Компакция × CoW DML | `rewrite_data × cow_dml` | **20/20** | ✓ PASS |
| 2 | Компакция × MoR DML | `rewrite_data × mor_dml` | **10/20** | ✗ (исчерпание данных, §4.4 отчёта) |
| 3 | CoW DML × append (serial) | `cow_dml × append` | **20/20** | ✓ PASS |
| 4 | CoW DML × CoW DML | `cow_dml × cow_dml` | **20/20** | ✓ PASS |

> **3/4 → Критерий 1 PASS**

---

## Критерий 2: ≥95% совпадений с исходной гипотезой

| Всего кейсов | Not-conclusive | Оценивается | Match | Mismatch | Match rate |
| --- | --- | --- | --- | --- | --- |
| 39 | 9 | 30 | 19 | 11 | **63.3%** |

> **63.3% < 95% → Критерий 2 FAIL**

### Not-conclusive (9 кейсов — исключены из оценки)

| Кол-во | Категория | Причина |
| --- | --- | --- |
| 2 | `cow_dml×{append,su}(snapshot)` | Snapshot isolation не применялась в тесте |
| 3 | `×mor_dml` (file-overlap) | Partition-overlap ≠ file-overlap; K2 механизм подтверждён в 31–50% |
| 4 | `rewrite_deletes×{append,su,rw,cow}` | rewrite_deletes требует prior position deletes; 0 valid trials |

### 11 расхождений (MISMATCH против исходной гипотезы)

| Пара | Гипотеза | Факт | Причина ошибки теста | Источник |
| --- | --- | --- | --- | --- |
| `overwrite×append` (serial) | K2 | K1 | опции isolation-level + validate-from-snapshot-id не установлены | SparkWrite.java:347-358 |
| `overwrite×stream_upsert` (serial) | K2 | K1 | то же | SparkWrite.java:347-358 |
| `overwrite×overwrite` (serial) | K2 | K1 | то же | SparkWrite.java:347-358 |
| `overwrite×cow_dml` (serial) | K2 | K1 | то же | SparkWrite.java:347-358 |
| `overwrite×mor_dml` (serial) | K2 | K1 | то же | SparkWrite.java:347-358 |
| `overwrite×rewrite_data` (serial) | K2 | K1 | то же | SparkWrite.java:347-358 |
| `overwrite×append` (default) | K2 | K1 | то же | SparkWrite.java:347-358 |
| `mor_dml×append` (serial) | K2 | K1 | committed-данные не под conflictDetectionFilter запроса | SparkPositionDeltaWrite.java:266-284 |
| `mor_dml×stream_upsert` (serial) | K2 | K1 | то же | SparkPositionDeltaWrite.java:266-284 |
| `mor_dml×overwrite` (serial) | K2 | K1 | то же | SparkPositionDeltaWrite.java:266-284 |
| `mor_dml×mor_dml` (serial) | K2 | K1 | то же | SparkPositionDeltaWrite.java:266-284 |

---

## Тестовые ограничения

1. **overwrite без опций**: `SparkWrite.java:347-358` — data-level валидация включается только при `.option("isolation-level","serializable").option("validate-from-snapshot-id",<id>)`.
2. **mor_dml conflictDetectionFilter**: `SparkPositionDeltaWrite.java:266-284` — `validateNoConflictingDataFiles()` фильтрует по `WHERE`-предикату. Committed-данные в том же bucket не обязательно попадают под предикат.
3. **Snapshot isolation**: тест не устанавливал table property → 2 кейса not-conclusive.
4. **rewrite_deletes**: 0 valid trials без prior position deletes.
5. **k2_control assertion**: `rewrite_data` глобальна → pytest FAIL, но overlap K2=100%.

---

## Подтверждённые результаты итерации 1

| Группа операций | Гипотеза | Факт | Статус |
| --- | --- | --- | --- |
| `cow_dml×{append,su,ow,cow,rw}` (serial) | K2 | K2 100% | ✓ Подтверждено |
| `rewrite_data×{ow,cow,rw}` | K2 | K2 100% | ✓ Подтверждено |
| `append×*`, `su×*` | K1 | K1 100% | ✓ Подтверждено |
| `mor_dml×{cow,rw}` (serial) | K2 | K2 100% | ✓ Подтверждено |
| `expire×*` | K1 | success 100% | новая находка — матрица исправлена |

---

## Варианты решения

| | ИТЕРАЦИЯ 2 (рекомендовано) | PIVOT |
| --- | --- | --- |
| Смысл | Повторить этап 1 с исправленной методологией | Пересмотреть подход |
| Условие | ADR-004/ADR-005 отклонены; исходная гипотеза верна | Фундаментальные проблемы |
| Требования | §7 stage-1-report.md | — |

---

## Решение автора

`[ X ]` ИТЕРАЦИЯ 2 — повторить этап 1 с исправленной методологией теста  
`[   ]` PIVOT — указать причину: ___________
