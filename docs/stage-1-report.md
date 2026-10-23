# Отчёт по этапу 1: Эмпирическая матрица конфликтов — итерация 3

| Поле | Значение |
| --- | --- |
| Даты | 2026-10-22 — 2026-10-23 |
| Коммит / тег | прогон на `d28f773`, результаты `9bdc43b`, пилоты `93bd283`; тега нет (G1 — решение автора) |
| Версии | клиент Iceberg 1.12.0 (`iceberg-spark-runtime-4.0_2.13-1.12.0`), Spark 4.0.1 (кластер `spark://spark-master:7077`, 2 воркера), сервер каталога — REST fixture `b0df3ca`, SeaweedFS 4.48; Flink в этапе не используется (см. §5) |
| Статус | Итерация 3 завершена; критерий G1-2 против исходной гипотезы не выполнен (84,0 %), см. `docs/gate-G1-report.md` |

## 1. Цель этапа

Подтвердить или исправить `config/conflict_matrix.yaml` для Iceberg 1.12 (v2 и v3), зафиксировать тексты исключений, оценить критерии G1.

## 2. Что сделано (итерация 3)

- `src/capl/digest.py`, `src/capl/main.py`: журнал прокси (`/_capl/debug/last-commits`) хранит копию `requirements`/`updates` тела каждого коммита (для `add-snapshot` — снапшот целиком, включая summary и manifest-list), метки `ts` (приход), `ts_fwd` (форвард после удержания), `ts_resp` (ответ каталога), `held`, `hold_timeout`. Тело запроса пересылается исходными байтами (ограничение 1 AGENTS.md; `tests/unit/test_proxy_digest.py::test_digest_does_not_mutate_body`).
- `src/runner/ops/effects.py`: проверка эффекта операции по summary снапшота из тела коммита (MoR DELETE обязан добавить position deletes/DV и не удалять data-файлы; expire обязан удалить снапшоты; и т. д.), исход повтора, окно удержания.
- `src/runner/ops/errors.py`: класс исхода — по самому глубокому `org.apache.iceberg.exceptions.*` в цепочке `Caused by` (обёртка Spark-действий компакции называет оба класса в тексте, см. S1-R3 ниже).
- `tests/integration/test_conflict_matrix.py` — переписан (см. §3), `tests/integration/log4j2-matrix.properties` — захват commit message Spark с `conflictDetectionFilter`.
- `scripts/update_matrix.py`: метрика G1 против исходной гипотезы (`c6e9a4e`, `fd593a5`), тепловые карты v2/v3, `g1.json`; `scripts/matrix_verify.sh` + `make matrix-verify`: v2 и v3 параллельно (разные Spark-приложения и слоты удержания).
- `config/conflict_matrix.yaml`: `format_versions: [2, 3]`, правила исправлены по пилоту, каждое отличие от гипотезы — со ссылкой `файл:строка`; `verified` — 174 записи.
- `docs/decisions/ADR-006-matrix-iter3-corrections.md` — все исправления матрицы.
- Unit-тесты: `tests/unit/test_effects.py`, `tests/unit/test_proxy_digest.py` (19 тестов; до итерации `make test-unit` падал с кодом 5 — 0 тестов).

## 3. Методика повтора и проверки валидности

Повтор: свежие строки (уникальный диапазон id: `seq × 10000 + bkt × 10⁶`) → pending запускается, его первый коммит удерживается прокси → committed выполняется и коммитится → удержание снимается → исход pending: `success` (без 409), `K1` (был 409, повтор Iceberg-клиента успешен), `K2` (`ValidationException` в цепочке причин), иначе `error:<класс>`.

Повтор засчитывается, только если по артефактам (P5):

| Условие | Чем проверено |
| --- | --- |
| pending удержан, без таймаута | `held=true`, `hold_timeout` нет у первой попытки (журнал прокси) |
| committed закоммичен **во время** удержания | все коммиты committed: `status=200`, `ts ≥ ts(pending)` и `ts_resp ≤ ts_fwd(pending)` (`effects.in_hold_window`) |
| обе операции изменили данные и тип операции тот | summary из тела коммита (`effects.summary_effect`); для pending — тело первой попытки, т. е. и для K2-повторов |
| expire реально удалил снапшоты | `remove-snapshots` с ≥ 1 id в теле; для `append_expire` — базовый снапшот pending среди удалённых |
| пересечение есть (повтор с пересечением) / нет (контроль) | следы из манифестов обоих снапшотов (py4j `SnapshotParser.fromJson` + `addedDataFiles/removedDataFiles/addedDeleteFiles/removedDeleteFiles`); манифесты pending читаются, пока он удержан. `file` — пересечение множеств затронутых data-файлов (удалённые + цели delete-файлов; для v2 position deletes цели читаются из колонки `file_path`), `partition` — пересечение партиций, `predicate` — границы id новых файлов committed пересекают предикат pending |

Операции: MoR DELETE удаляет 10 из 48 (12 в компакционных кейсах) строк файла; MERGE содержит целевой предикат в `ON`; `overwrite_opts` — `validate-from-snapshot-id` = текущий снапшот, прочитанный через Java API; `expire` — `older_than` = момент запуска, `retain_last => 1`; `stream_upsert` — `RowDelta(addRows + equality deletes)` через Java API без валидаций, как коммитит Flink (`flink/v1.20/.../IcebergFilesCommitter.java:361-365`); `cow_dml` использует UPDATE или MERGE так, чтобы оператор не совпадал с MoR-оператором пары (режим записи задаётся на оператор).

Проверка способна упасть (P3): в пилотах она отклоняла повторы — «no file overlap» (до чтения `file_path`), «no remove-snapshots update» (expire в первом повторе), «pending not held» (`rewrite_deletes` в v3), см. `results/stage-1/iter3/pilot1`, `pilot2`.

## 4. Результаты проверок

| Проверка (из плана) | Тип | Ожидаемо | Фактически | Итог |
| --- | --- | --- | --- | --- |
| `make matrix-verify` — тест проходит, YAML обновлён | auto | тест проходит | v2: `91 passed` за 2542 с; v3: `83 passed` за 2155 с; `verified` — 174 записи | PASS |
| ≥ 20 повторов на пару + контроль | auto | 20 + контроль | 20 + 5 на каждый из 174 кейсов, 4350 повторов, **все валидны** (0 отклонённых) | PASS |
| Контроль без пересечения: K2 = 0 | auto | 0 | K2 = 0 во всех кейсах, кроме `merge_upsert_unscoped` (5/5, предсказано кодом до прогона, §4.3) | PASS |
| Каждое расхождение объяснено кодом | manual | ссылка класс/метод | ADR-006, комментарии правил в YAML | PASS |
| G1-1: K2 в ≥ 3 из 4 групп | auto | ≥ 3 | 4/4 (§4.1) | PASS |
| G1-2: совпадение с исходной гипотезой | auto | ≥ 95 % повторов | 84,0 % (2720/3240) против `c6e9a4e`; 88,9 % (2880/3240) против `fd593a5` | **FAIL** |
| `make lint typecheck test-unit` | auto | зелёные | `All checks passed!`; `Success: no issues found in 3 source files`; `19 passed` | PASS |

Команды: `make matrix-verify NAME=iter3` (лог `results/stage-1/iter3/pytest-v{2,3}.log.gz`, вывод `update_matrix.out`); `python3 scripts/update_matrix.py --out results/stage-1/iter3 --results results/stage-1/iter3/v2/matrix_results.json.gz --results results/stage-1/iter3/v3/matrix_results.json.gz`.

### 4.1 G1, критерий 1

| Группа | Пары (каждая K2 20/20 валидных, v2 и v3) |
| --- | --- |
| компакция × CoW DML | `rewrite_data × cow_dml`, `cow_dml × rewrite_data` |
| компакция × MoR DML | `rewrite_data × mor_dml`, `mor_{dml,delete,update,merge} × rewrite_data` |
| DML × append (serializable) | `cow_dml`, `mor_dml`, `mor_delete`, `mor_update`, `mor_merge` × append; сценарные `merge_upsert`, `update_status`, `delete_by_keys` × append |
| DML × DML | `cow_dml × {cow_dml, mor_dml}`, `mor_* × cow_dml`, `mor_update/merge × mor_*`; исключение: `mor_dml × mor_dml` v2 — K1 20/20 (ADR-006) |

### 4.2 G1, критерий 2 (против исходной гипотезы)

Сравнение — по каждому валидному повтору с пересечением (`scripts/update_matrix.py`, `g1_metric`; при нескольких подходящих правилах берётся наиболее специфичное). Кейсы без правила в гипотезе (12: `* × expire`, `* × append_expire`) исключены. Новые операции сопоставлены классам гипотезы: `mor_delete/update/merge`, `update_status` → `mor_dml`; `overwrite_opts` → `overwrite`; `merge_upsert*`, `delete_by_keys` → `cow_dml`; `compaction_where` → `rewrite_data`.

| Гипотеза | Совпало повторов | Пар (≥ 95 %) | Расходятся |
| --- | --- | --- | --- |
| `c6e9a4e` (до опытов) | 2720 / 3240 = **84,0 %** | 136 / 162 | overwrite без опций (12 кейсов), expire (8), `cow_dml × stream_upsert` snapshot (2), MoR DELETE × DELETE v2 (2), `rewrite_deletes × {rewrite_data, cow_dml}` (2) |
| `fd593a5` (после итерации 1) | 2880 / 3240 = **88,9 %** | 144 / 162 | то же без expire |

Расходящиеся пары и их механизмы — ADR-006. Исправленная матрица отдельно: 174/174 кейса совпадают с ней (это регрессионная проверка, не метрика G1).

### 4.3 Сценарные пары (схема `dt × bucket(8, key)` из `_base.yaml`, pending × append поздних событий)

| Сценарий / операция | Формат | С пересечением | Контроль (другой день, другие ключи) | `conflictDetectionFilter` (commit message Spark) |
| --- | --- | --- | --- | --- |
| S2 `merge_upsert` (`ON key AND dt AND t.dt = <день>`) | v2 | K2 20/20 | K1 5/5 | `ref(name="dt") == 20787` |
| S2 `merge_upsert_unscoped` (`ON key AND dt`) | v2 | K2 20/20 | **K2 5/5** | `true` |
| S3 `update_status` (MoR) | v2, v3 | K2 20/20 | K1 5/5 | `dt == 20787 and key >= k₀ and key <= k₀+49` |
| S5 `delete_by_keys` | v2 | K2 20/20 | K1 5/5 | `key in (k₀, k₀+5, …)` |
| S2/S3 компакция `where dt = <день>` | v2, v3 | K1 20/20 | K1 5/5 | — (RewriteFiles) |

Наблюдение для сценариев: фильтр конфликтов MERGE строится только из целевых предикатов скана (`SW:445-455`); без литерального предиката на целевую таблицу он равен `true`, и MERGE конфликтует с любым append в таблицу. Предикат `t.dt = …` сужает его до партиций дня, но не до ключей.

### 4.4 Тексты исключений (для `runner/ops/errors.py`)

K2 — `org.apache.iceberg.exceptions.ValidationException` (в 2005 повторах), сообщения (число валидных K2-повторов):

| Сообщение | Повторов | Где возникает |
| --- | --- | --- |
| `Found conflicting files that can contain records matching <filter>` | 985 | новые data-файлы под фильтром (CoW/MoR DML × append/upsert/overwrite) |
| `Cannot commit, missing data files` | 480 | файлы, на которые ссылаются deletes, удалены (MoR × CoW/компакция) |
| `Missing required files to delete` | 180 | удаляемые файлы уже удалены (компакция × компакция, `rewrite_deletes × mor_dml`) |
| `Found new conflicting delete files that can apply to records matching …` | 120 | MoR UPDATE/MERGE × новые deletes |
| `Cannot commit, found new delete for replaced data file` / `… new position delete …` | 160 | компакция/CoW × MoR deletes |
| `Found conflicting deleted files that can apply to records matching …` | 40 | |
| `Found concurrently added DV for …` | 40 | v3, DV × DV |

Компакция (`rewrite_data_files`, `rewrite_position_delete_files`) оборачивает и `ValidationException`, и `CommitFailedException` в `RuntimeException: Cannot commit rewrite because of a ValidationException or CommitFailedException…` (`RewriteDataFilesSparkAction.java:306-314`, `RewritePositionDeleteFilesSparkAction.java:238-248`); класс определяется по `Caused by`. K1 — HTTP 409 на первую попытку в журнале прокси (1875 повторов `[409, 200]`); исчерпания повторов (`CommitFailedException`) не наблюдалось. Новый класс: `IllegalArgumentException: Snapshot … is not an ancestor of …` (MoR после удаления базового снапшота, ADR-006).

## 5. Отклонения от ТЗ

| Пункт ТЗ | Что изменено | Причина | ADR |
| --- | --- | --- | --- |
| Матрица `conflict_matrix.yaml` | 6 групп исправлений + 3 исследовательские пары | наблюдения с объяснением кодом | ADR-006 |
| `stream_upsert` — поток Flink | коммит через Java API Iceberg в драйвере Spark | детерминированное удержание; Flink-коммиттер не включает валидаций (`IcebergFilesCommitter.java:361-365`), т. е. коммит идентичен | — |
| `rewrite_deletes` в v3 | не применим | `RewritePositionDeleteFilesSparkAction.java:121-124` | ADR-006 |
| Ресурсы Spark в тесте | драйвер 1536m, `cores.max=4` на приложение (в экспериментах 3g/8) | два приложения (v2, v3) параллельно в контейнере runner (5g); на валидацию коммита (в драйвере) не влияет | — |

## 6. Проблемы и открытые вопросы

- G1-2 не выполнен: решение (GO/CORRECT/PIVOT) — за автором, рекомендация в `docs/gate-G1-report.md`.
- Тексты ТЗ (AGENTS.md «DML/overwrite × append», S4 `reprocess_day`, S3 `delete_rewrite` для v3) требуют правки после решения автора (ADR-006, «Последствия»).
- В поле `filter` commit-лога для CoW-коммитов остаётся хвост ` to table …` (регулярное выражение; сырое сообщение сохранено в `commit_log[].msg`, значения в §4.3 очищены).
- Сценарные повторы не сохраняют список попыток pending (`pending_attempts`); окно удержания и 409 для них проверены по тем же записям журнала прокси, но в JSON не выгружены.
- Данные малы (48 строк на файл); зависимость вероятности конфликта от объёма и распределения — предмет этапов 3–4.

## 7. Что переносится в текст ВКР

В данной работе эмпирическая матрица конфликтов оптимистичного контроля параллелизма (Optimistic Concurrency Control, OCC) построена для Apache Iceberg 1.12.0 (Spark 4.0, форматы таблиц v2 и v3) по 174 парам операций с 20 повторами с пересечением и 5 контрольными повторами на пару; валидность каждого из 4350 повторов подтверждена по телам коммитов и манифестам снапшотов. Конфликт данных (К2) воспроизводится детерминированно (20 из 20 повторов) во всех четырёх группах пар, лежащих в основе сценариев: компакция × CoW DML, компакция × MoR DML, DML × append при уровне изоляции `serializable` и DML × DML. Таким образом, проблема, решаемая в работе, существует.

Исходная гипотеза о матрице подтвердилась в 84,0 % повторов. Расхождения объясняются кодом валидации конкретной версии: во-первых, перезапись партиций (`ReplacePartitions`) проверяет конфликты только при явных опциях записи; во-вторых, MoR DELETE в формате v2 не проверяет параллельные delete-файлы, тогда как в v3 ограничение «один вектор удалений на файл» делает ту же пару конфликтной; в-третьих, переписывание delete-файлов не проверяет удаление data-файлов, на которые они ссылаются; в-четвёртых, удаление снапшотов (`expire_snapshots`) не порождает конфликта каталога, однако удаление базового снапшота MoR-операции приводит к её отказу без повтора.

Фильтр обнаружения конфликтов (`conflictDetectionFilter`) строится из предикатов скана целевой таблицы; для MERGE без предиката на целевую таблицу он вырождается в `true`, и операция конфликтует с любой параллельной записью в таблицу.

## 8. Следующий этап

Этап 2 (прокси каталога) — только после решения автора на G1.

---

## Ответы на ревью

### Ревью `docs/reviews/stage-1-r1.md`

| ID | Что сделано | Где (файл, коммит) | Подтверждено числом |
| --- | --- | --- | --- |
| S1-R1-01 | MoR DELETE удаляет 10 из 48 строк файла (`DML_ROWS`); валидность требует `operation=delete`, `added-position-deletes`/`added-dvs` > 0 и `deleted-data-files = 0` в теле коммита pending (иначе — metadata delete, повтор отклоняется). Все пары `mor_delete × *`, `mor_dml × *` перепрогнаны в v2 и v3. Вывод §4.1 итерации 2 отозван: `mor_dml/mor_delete × {append, stream_upsert, overwrite}` — K2 20/20 (`SPDW:282-284`), `× {cow_dml, rewrite_data}` — K2 20/20 (`SPDW:269`); единственная K1-пара — DELETE × DELETE в v2 (`SPDW:277-280`). Противоречие с итерацией 1 (`mor_dml × rewrite_data` K2) снято — итерация 3 тоже K2 | `test_conflict_matrix.py` (`_exec_op`, `_run_one_trial`), `src/runner/ops/effects.py`; `46496e6`, результаты `9bdc43b` | 4350/4350 повторов валидны; `mor_delete_x_append_serializable_v2` K2 20/20, контроль K1 5/5 |
| S1-R1-02 | Проверка «данные изменены» возвращена и усилена, без Spark-кэша: summary берётся из тела `add-snapshot` в журнале прокси для обеих операций (для pending — первая попытка, т. е. и при K2), файлы — из манифестов через Java API | `src/capl/digest.py`, `effects.commit_effect`; `46496e6` | в пилотах проверка отклоняла повторы (`results/stage-1/iter3/pilot1`: «no remove-snapshots update» — по 1 повтору в каждом из 5 кейсов expire, v2 и v3; «no file overlap» — 17 кейсов v2 до чтения `file_path`); в итоговом прогоне 0 отклонённых |
| S1-R1-03 | `expire_snapshots(older_than => <момент запуска>, retain_last => 1)`; валидность требует `remove-snapshots` с ≥ 1 id; в setup — стартовый снапшот, чтобы удалять было что уже в первом повторе. Удаление базового снапшота pending — отдельный committed `append_expire`, проверяется по id в теле коммита. Обе стороны перепрогнаны | `test_conflict_matrix.py` (`expire`, `append_expire`, `_setup_table`); `d28f773` | `expire × *`: success 20/20, в 25/25 повторах удалено ≥ 1 снапшота (пример: до 3, после 2); `append_expire`: базовый снапшот удалён в 25/25 повторах каждой пары |
| S1-R1-04 | Метрика G1 считается против исходной гипотезы — `c6e9a4e` (до опытов) и `fd593a5` (названа в ревью); исправления — отдельной таблицей с `файл:строка` | `scripts/update_matrix.py`, ADR-006; `9bdc43b` | 84,0 % (2720/3240) и 88,9 % (2880/3240) — порог 95 % не выполнен; это отражено в отчёте G1 |
| S1-R1-05 | Возвращено `K2 == 0` в контроле (единственное исключение — `merge_upsert_unscoped`, для которого контроль `K2 == 5/5` задан **до** прогона с обоснованием `SW:445-455`; утверждение равенством, не порогом). Механизм прежних K2 в контроле найден: `overwrite_opts` — `validate-from-snapshot-id = max(snapshot_id)` (id случайны, это не текущий снапшот → валидация захватывала вставки самого повтора); `mor_merge` — MERGE без целевого предиката, фильтр `true` | `_assert_case`, `_exec_op` (`overwrite_opts`, MERGE); `46496e6` | `overwrite_opts × *` контроль K1 5/5 (было K2 4–5/5), `mor_merge × *` контроль K1 5/5 (было K2 5/5); commit message: фильтр `true` у unscoped MERGE |
| S1-R1-06 | `docs/gate-G1-report.md` переписан по шаблону: рекомендация агента, поле «Решение автора» пустое | `docs/gate-G1-report.md` | — |
| S1-R1-07 | `_latest_snap_id` удалён; базовый снапшот для `overwrite_opts` читается через Java API (`table.refresh().currentSnapshot()`), при отсутствии — `assert`, без запасного `0` | `_exec_op`, `_run_one_trial`; `46496e6` | `overwrite_opts × *`: 20/20 валидных (было 2–5), K2 20/20 в v2 и v3 |
| S1-R1-08 | Вывод pytest приведён целиком в репозитории, в отчёте — итоговые строки; ни один вывод не сделан по упавшему тесту | `results/stage-1/iter3/pytest-v2.log.gz`, `pytest-v3.log.gz` | v2: `91 passed, 1 warning in 2542.68s`, v3: `83 passed, 1 warning in 2155.11s` |
| S1-R1-09 | По журналу прокси: требования коммита expire — только `[{"type": "assert-table-uuid"}]`, у pending CoW — `assert-table-uuid` + `assert-ref-snapshot-id main`. Код: `UR:50-58` (всегда `AssertTableUUID`), `UR:93-115` (`remove-snapshots` требований не добавляет), `UR:117-129` (`AssertRefSnapshotID` — только для `set-snapshot-ref`) | `results/stage-1/iter3/v2/matrix_results.json.gz`, `expire_x_cow_dml_serializable_v2.trials[*].pending_attempts[*].requirements`, `cow_dml_x_expire_serializable_v2.trials[*].committed_commits` | 25/25 повторов каждой пары |
| S1-R1-10 | Результаты итераций коммитятся: `results/stage-1/iter2/` (архив итерации 2), `results/stage-1/iter3/` (JSON по повторам, логи pytest и commit-лог Spark в `.gz`, тепловые карты, `g1.json`, пилоты); `.gitignore` открывает `results/stage-1/iter*/` | `.gitignore`; `46496e6`, `9bdc43b`, `93bd283` | 3,6 МБ итерации 3 + 0,5 МБ пилотов |

Замечания предыдущих ручных ревью, перенесённые в r1:

| Замечание | Что сделано | Подтверждено |
| --- | --- | --- |
| Сценарные пары | 7 кейсов на схеме `dt × bucket(8, key)` (S2 `merge_upsert` в двух вариантах, S3 `update_status` v2/v3, S5 `delete_by_keys`, компакция с `where` v2/v3) против append; `conflictDetectionFilter` записан из commit message Spark | §4.3; K2/K1 20/20 |
| Полный прогон v3 | все правила прогнаны в v3 (83 кейса), включая MoR × MoR и компакцию × MoR | `pytest-v3.log.gz`: `83 passed`; тепловая карта `heatmap-v3.png` |
| `rewrite_deletes` | таблица в MoR, перед повтором создаются два позиционных delete-файла для data-файла с DML-диапазоном; валидность требует `removed-position-delete-files > 0` | v2: 5 пар × 20 валидных повторов; v3 неприменим (`RewritePositionDeleteFilesSparkAction.java:121-124`) |
| Метрика G1 против исходной гипотезы | см. S1-R1-04 | 84,0 % / 88,9 % |

Дополнительно найдено при самопроверке (не было в ревью):

| Проблема | Что сделано |
| --- | --- |
| Классификатор считал K2 по слову `ValidationException` в тексте; у компакции обёртка содержит оба слова, поэтому K1 (исчерпание повторов) неотличим от K2 | класс по самому глубокому `org.apache.iceberg.exceptions.*` в `Caused by` (`d28f773`, unit-тесты на обёртку). В итоговом прогоне у всех K2 компакции причина — `ValidationException` |
| Тест выполнялся в `local[4]` (P4) | `spark://spark-master:7077`, 2 воркера |
| `make lint` падал (F821 на `_latest_snap_id`), `make test-unit` — код 5 (0 тестов) | исправлено, 19 unit-тестов |
| `stream_upsert` выполнялся как обычный INSERT (то же, что append) | RowDelta с equality deletes через Java API, валидность требует `added-equality-deletes > 0` |
| `cow_dml × mor_update` выполнялся бы с одним режимом UPDATE для обеих операций | `cow_dml` использует оператор, не занятый MoR-операцией пары |
