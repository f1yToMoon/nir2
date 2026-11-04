# ADR-009: «Золотая» таблица с тегом и учёт стоимости по job tag

**Статус:** Принято (этап 3)
**Дата:** 2026-11-04
**Связано с:** `tz/05_workloads_and_experiments.md` §1.1, `tz/04_interfaces.md` §5, FR-RUN-5, ADR-008; `src/runner/main.py`, `src/runner/slots.py`, `src/runner/cost.py`

## 1. Тег `golden` на начальном снапшоте

**Контекст.** По ТЗ (tz/05 §1.1) перед каждым прогоном таблица сбрасывается к «золотой» версии: `DROP TABLE` без `PURGE` и `register_table` с сохранённым `metadata.json`. Данные золотой версии при этом не копируются: прогоны переиспользуют её data-файлы. По ADR-008 в сценариях работает периодический `expire_snapshots`. Процедура выполняет `ExpireSnapshotsSparkAction`: после коммита она удаляет файлы удалённых снапшотов, на которые не ссылается ни один оставшийся снапшот (`ExpireSnapshotsSparkAction.java:173-184`, `fileDS` по оставшимся снапшотам — `:236-246`). Опции «не удалять файлы» у процедуры нет: её параметры — `older_than`, `retain_last`, `max_concurrent_deletes`, `stream_results`, `snapshot_ids`, `clean_expired_metadata` (`ExpireSnapshotsProcedure.java:53-70`). Значит, первая компакция прогона и следующий expire удалили бы data-файлы золотой версии, а следующий прогон получил бы таблицу с отсутствующими файлами.

**Решение.**
1. При засеве (`make seed`) таблица пишется `files_per_partition` пакетами, затем `expire_snapshots(retain_last => 1)` оставляет один снапшот, и на нём создаётся тег: `ALTER TABLE … CREATE TAG golden`.
2. Тег удерживает снапшот: expire не удаляет снапшоты, на которые есть ссылки, а файлы, достижимые из оставшихся снапшотов, не являются кандидатами на удаление (`fileDS(updatedMetadata)`). Поэтому файлы золотой версии переживают любое число прогонов.
3. Свойства сценария (режимы MoR, format v3, уровни изоляции) задаются `ALTER TABLE … SET TBLPROPERTIES` поверх того же снапшота; для каждого сценария и варианта сохраняется свой `metadata.json` (`data/golden/<сценарий>[__вариант].json`). Данные у всех сценариев одни (`data/golden/_data.json`).
4. Перед прогоном удаляются все объекты под расположением таблицы, на которые не ссылаются золотые версии (`data/golden/_data.keep.txt`: data-файлы, манифесты, manifest list снапшота `golden`, все золотые `metadata.json`). Удаление выполняется до старта прогона и на метрики не влияет (tz/05 §1.1, п. 3).

**Последствия.**
- В метаданных таблицы всё время прогона есть один дополнительный снапшот и ссылка `golden`. Проверки конфликтов идут по истории ветки `main` (`MergingSnapshotProducer.validationHistory`) и тег не затрагивают.
- Expire читает манифесты и снапшота `golden` (40 манифестов, 9600 файлов). Это часть его стоимости, которая на этапе 3 измеряется (ревью S2-R4-03).
- Снапшот `golden` — корень линии `main` каждого прогона. Проверка `committed_ops_in_lineage` восстанавливает линию по журналу прокси до этого снапшота.

## 2. Учёт стоимости по job tag

**Контекст.** ТЗ (tz/04 §5, FR-RUN-5): стоимость операции — сумма `executorRunTime` и `executorCpuTime` стадий заданий с job group = `op_id`. Действия Iceberg (`rewrite_data_files`, `rewrite_position_delete_files`, `expire_snapshots`) выполняют задания внутри `withJobGroupInfo`, который заменяет job group на свой («REWRITE-DATA-FILES» и т. п.) на время действия (`BaseSparkAction.java:129-130`, `JobGroupUtils.java:42-62`). Задания компакции и expire не попали бы в job group операции, и их стоимость была бы занижена до нескольких заданий планирования.

**Решение.** Поток операции, как и по ТЗ, ставит job group = `op_id`, а дополнительно — job tag = `op_id` (`SparkContext.addJobTag`, Spark 3.5+). Iceberg теги не трогает. Задания операции находятся через `SparkStatusTracker.getJobIdsForTag(op_id)`. Затем по REST API драйвера (`/jobs/{id}`, `/stages/{id}`) суммируются все попытки стадий (`src/runner/cost.py`). В `ops.cost_detail.job_groups` записываются фактические job group заданий: по ним видно, где Iceberg заменил группу.

**Проверка.** Проверка этапа 3 сравнивает Σ `cpu_ms` операций прогона с суммой `executorCpuTime` всех стадий приложения и с CPU контейнеров Spark-воркеров по cgroup (`scripts/run_measured.sh`).
