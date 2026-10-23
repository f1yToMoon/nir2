# ADR-006: Исправления матрицы конфликтов по итерации 3 этапа 1

**Статус:** Предложено (ждёт решения автора на G1)
**Дата:** 2026-10-23
**Связано с:** ADR-004, ADR-005 (отклонены), `docs/reviews/stage-1-r1.md`, `docs/gate-G1-report.md`

## Контекст

Итерация 3 этапа 1 (`make matrix-verify NAME=iter3`, коммит `d28f773`, результаты `results/stage-1/iter3/`) проверила 174 пары (форматы v2 и v3, 20 повторов с пересечением + 5 контрольных), все 4350 повторов валидны по данным (журнал прокси, манифесты снапшотов). Исходная гипотеза (`config/conflict_matrix.yaml` на `c6e9a4e`) совпала с наблюдениями в 84,0 % валидных повторов (2720/3240) — ниже порога G1 (95 %). Ниже — все отличия, каждое объяснено кодом Iceberg 1.12.0 / Spark 4.0 (`third_party/iceberg-src`, тег `apache-iceberg-1.12.0`).

Сокращения путей: `SW` = `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java`, `SPDW` = `.../SparkPositionDeltaWrite.java`, `BRD` = `core/src/main/java/org/apache/iceberg/BaseRowDelta.java`, `BRF` = `core/.../BaseRewriteFiles.java`, `MSP` = `core/.../MergingSnapshotProducer.java`, `UR` = `core/.../UpdateRequirements.java`.

## Решение: исправления матрицы

| Пара (pending × committed) | Гипотеза `c6e9a4e` | Наблюдение (валидные повторы) | Механизм (код) |
| --- | --- | --- | --- |
| `overwrite` (INSERT OVERWRITE / `overwritePartitions()` без опций) × {append, stream_upsert, overwrite, cow_dml, mor_dml, rewrite_data}, serializable | K2 | K1 20/20 (v2 и v3) | Валидация `ReplacePartitions` включается только опциями записи `isolation-level` и `validate-from-snapshot-id` (`SW:350-358`); свойство таблицы `write.*.isolation-level` на неё не влияет. С опциями (`overwrite_opts`) — K2 20/20 |
| `cow_dml` × `stream_upsert`, snapshot | K1 | K2 20/20 (v2, v3) | При `snapshot` CoW всё равно вызывает `validateNoConflictingDeletes` (`SW:527`); equality deletes upsert попадают под `conflictDetectionFilter` |
| `mor_dml` (DELETE) × `mor_dml` (DELETE), v2 | K2 | K1 20/20 | DELETE не вызывает `validateNoConflictingDeleteFiles` (только UPDATE/MERGE, `SPDW:277-280`); в v2 два файла position deletes на один data-файл допустимы. В v3 гипотеза верна: K2 20/20 (`validateAddedDVs` вызывается всегда, `BRD:172`, `MSP:909-915`) |
| `rewrite_deletes` × {rewrite_data, cow_dml}, v2 | K2 | K1 20/20 при доказанном file-перекрытии | `BRF:135-142`: валидируются только заменённые data-файлы; переписывание одних delete-файлов не проверяется → коммитятся delete-файлы, ссылающиеся на удалённые data-файлы. С `mor_dml` — K2 20/20 («Missing required files to delete»: MoR DELETE сам заменяет delete-файл, `SPDW:255-258`) |
| `rewrite_deletes`, v3 | (не различалось) | коммита нет | При одних DV `rewrite_position_delete_files` ничего не делает (`RewritePositionDeleteFilesSparkAction.java:121-124`) → в v3 операция неприменима |
| `expire` × {append, cow_dml, mor_delete, rewrite_data} | K1 | success 20/20 (v2, v3) | Тело коммита expire — только `remove-snapshots`, требование одно — `assert-table-uuid` (журнал прокси; `UR:50-58`, `remove-snapshots` не добавляет требований `UR:93-115`) → 409 не возникает |

Пары, для которых исходной гипотезы не было (добавлены как исследовательские):

| Пара | Наблюдение | Механизм |
| --- | --- | --- |
| {rewrite_data, cow_dml, mor_dml} × `expire` | success 20/20 | базовый снапшот pending — текущий, expire его не удаляет; `main` не меняется |
| {rewrite_data, cow_dml} × `append_expire` (append в другую партицию + expire, удаляющий базовый снапшот pending — проверено по телу коммита в 25/25 повторах) | K1 20/20 | история валидации доходит до снапшота, чей parent = базовый (`MSP:990-994`) |
| `mor_update` × `append_expire` | `IllegalArgumentException` 20/20 (и 5/5 в контроле) | `BRD:134-139`: базовый снапшот должен быть предком текущего; удалённый снапшот не находится → отказ без повтора. Работа теряется, как при K2, но вне классов K1/K2 |

Итерация 2 утверждала обратное для MoR DELETE («K1 со всеми операциями») — это был артефакт теста (удалялись все строки файла → metadata delete, не RowDelta); гипотеза K2 подтверждена: `mor_dml`/`mor_delete` × {append, stream_upsert, overwrite} при serializable — K2 20/20 (`SPDW:282-284`), × {cow_dml, rewrite_data} — K2 20/20 (`SPDW:269`).

## Последствия

- `config/conflict_matrix.yaml` (правила + `verified`) — исправленная матрица; метрика G1 по-прежнему считается против `c6e9a4e`/`fd593a5` (`scripts/update_matrix.py`).
- Требует правки текстов ТЗ/AGENTS.md после решения автора на G1: «DML/overwrite × append при serializable» → overwrite конфликтует только с опциями записи; сценарий S4 `reprocess_day` (`params: {isolation_level_option: serializable}`) должен задавать также `validate-from-snapshot-id`, иначе K2 не возникнет; сценарий S3 `delete_rewrite` (`rewrite_deletes`) не применим к варианту `v3_deletion_vectors`.
- Для политик (этап 5+): `expire` не конфликтует на уровне каталога, но удаление базового снапшота MoR-операции приводит к её отказу без повтора — класс исхода, которого нет в ТЗ.
