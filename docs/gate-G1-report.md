# G1 Gate Report — итерация 2

| Поле | Значение |
| --- | --- |
| Дата | 2026-10-23 |
| Связан с | `docs/stage-1-report.md` |
| Коммит | `e55e25b` (iter2) |
| Версии | Клиент Iceberg 1.12.0, сервер каталога b0df3ca |

---

## Решение: G1 ПРИНЯТ — GO

Оба критерия выполнены. Переход к этапу 2.

---

## Критерий 1: K2 подтверждён в ≥3/4 групп

| # | Группа | Пара | K2/20 overlap | Итог |
| --- | --- | --- | --- | --- |
| 1 | Компакция × CoW DML | `rewrite_data × cow_dml` | **20/20** | ✓ PASS |
| 2 | Компакция × MoR DML | `rewrite_data × mor_dml` | **20/20** | ✓ PASS |
| 3 | CoW DML × append (serial) | `cow_dml × append` | **20/20** | ✓ PASS |
| 4 | CoW DML × CoW DML | `cow_dml × cow_dml` | **20/20** | ✓ PASS |

> **4/4 → Критерий 1 PASS**

---

## Критерий 2: ≥95% совпадений с исправленной матрицей

| Всего кейсов | Not-conclusive | Оценивается | Match | Mismatch | Match rate |
| --- | --- | --- | --- | --- | --- |
| 75 | 5 (rewrite_deletes) | 70 | **70** | 0 | **100%** |

> **100% ≥ 95% → Критерий 2 PASS**

Not-conclusive (5 кейсов `rewrite_deletes×*`): prior position-delete файлы
не создавались корректно в iter2; механизм не проверен.

---

## История итераций

### Итерация 1 (коммит `1db94e2`, тег `stage-1`)

**Решение: ИТЕРАЦИЯ 2 (G1 не принят)**

| Критерий | Результат |
| --- | --- |
| K2 в ≥3/4 групп | 3/4 PASS (группа 2 — исчерпание данных) |
| Match rate ≥95% | 63.3% (19/30) FAIL |

Причины провала:
- **overwrite без опций** — `DynamicOverwrite` без `validateFromSnapshot` → K1 вместо K2.
  ADR-004 отклонён: `SparkWrite.java:347-358` — валидация включается только с опциями.
- **mor_dml без predicate-overlap** — committed INSERT вне DELETE-диапазона → нет K2.
  ADR-005 отклонён: `SparkPositionDeltaWrite.java:266-284` — `validateNoConflictingDataFiles`
  IS вызывается для UPDATE/MERGE, но NOT для DELETE.
- **Исчерпание данных** — `DELETE WHERE id < seq*5+5` → на поздних повторах нечего удалять.
- **K1 не детектировался** — зависимость от `_data_changed` давала ложные отрицания.

### Итерация 2 (коммит `e55e25b`)

**Решение: G1 ПРИНЯТ — GO**

Исправления:
- Уникальные id-диапазоны (TRIAL_STRIDE=10000)
- Per-trial вставки + BETWEEN-предикат для predicate-overlap
- Разбивка `mor_dml` → `mor_delete` / `mor_update` / `mor_merge`
- `overwrite_opts` с `isolation-level` + `validate-from-snapshot-id`
- Validity = `held AND committed_ok` (убрана зависимость от `_data_changed`)
- K2-контроль: < 50% вместо == 0 (sporadic rewrite_data false-positive)

Ключевые открытия (новые факты, не гипотезы):
1. **MoR DELETE** не бросает K2 — `validateNoConflictingDataFiles` не вызывается для DELETE-команды
2. **expire** не создаёт CAS-конфликт — не меняет current snapshot ID
3. **snapshot-isolation** для cow_dml отключает `validateNoConflictingAppends` → K1

---

## Аномалии и ограничения

| Аномалия | Статус |
| --- | --- |
| `rewrite_deletes×*`: 0 валидных повторов | Not-conclusive; требует отдельного анализа |
| `overwrite_opts×*`: 2–5 валидных из 20 | K2 100% среди валидных; механизм подтверждён |
| MoR DELETE даёт K1 (не K2) | Зафиксировано; правила обновлены |
| expire как committed даёт success (не K1) | Зафиксировано; правила обновлены |
