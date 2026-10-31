# Отчёт по этапу 2: Прокси каталога

| Поле | Значение |
| --- | --- |
| Даты | 2026-10-31 |
| Коммит / тег | реализация `c2ecd71`…`9dbafd2`; итоговый прогон `make proxy-verify NAME=r1` на `7bb8f1d` (код `src/` прогона = `7bb8f1d`), результаты `9dbafd2`; тег `stage-2` — после ревью |
| Версии | клиент Iceberg 1.12.0 (Spark 4.0.1, Flink — образ `nir/flink:dev`), PyIceberg 0.12.0, сервер каталога — REST fixture `b0df3ca`, SeaweedFS 4.48; сервис CAPL: Python 3.11, FastAPI/Starlette, uvicorn, httpx 0.28, asyncpg, fastavro, prometheus-client |
| Статус | Завершён; отклонения — ADR-007 |

## 0. Перед этапом: решение G1 и поручения автора

| Поручение | Что сделано | Где |
| --- | --- | --- |
| Тег `stage-1` | **Не поставлен — блокирует хук** (`scripts/hooks/guard_pretool.py`). Причины: (1) в `docs/gate-G1-report.md` решение записано в две строки («Решение автора:» / «Решение по этапу 1: CORRECT»), а регулярное выражение хука (`review_lib._LINE`) ищет `Решение автора: CORRECT` в одной строке → `author_decision() = None`; (2) после ревью r4 (`1fec39a`) изменён `docs/REVIEWER.md` (`a2167ec`) → ревью не покрывает HEAD. Обходить хук агент не стал; оба файла правит только автор (см. §6) | — |
| Правки CORRECT | AGENTS.md: overwrite проверяет конфликты только с опцией `isolation-level` и только вместе с `validate-from-snapshot-id` (иначе ложный К2), `expire`, `rewrite_deletes` только v2, ограничение 6 + `base_expired`; S4 `reprocess_day`: `validate_from_snapshot_id: planning_snapshot`; S3 `delete_rewrite`: `format_versions: [2]` (новое поле, описано в `_base.yaml`); `tz/03` §2 — итоговая матрица по ADR-006, §4.2 — `base_expired`; `tz/04` §1.2 — таблица классификации `op_class`; согласованность: `tz/01` FR-RUN-4, `tz/05` (S3, S4, SQL overwrite, `wasted_cpu_base_expired_s`), `schemas/op_event.schema.json`, `api/advisor.openapi.yaml`, DDL; ADR-006 — статус «Принято» | `0998333` |
| `max_ms` в `POST /v1/debug/hold` | Срок удержания = `min(max_ms, hold.max_ms)`, `max_ms ≤ 0` → 422. Тест на старом коде: 3 из 4 падают; на новом проходят | `98017d7`; затем перенесено в `capl.proxy.hold` (`tests/unit/test_hold.py`, 8 тестов) |
| Название темы в README | «Прогнозирование конфликтов и планирование операций в Data Lakehouse» (английский подзаголовок не менялся — по поручению только русское название) | `addd46b` |

## 1. Цель этапа

Прозрачный прокси REST-каталога с журналированием попыток коммита и восстановлением следов коммитов (tz/06, этап 2; FR-PROXY-1…9, NFR-1).

## 2. Что сделано

- `src/capl/proxy/app.py` (шаги 2.1, 2.6, 2.7): catch-all прокси на Starlette. Цель запроса берётся из `raw_path`/`query_string` без перекодирования. Тело коммита читается целиком (нужно для разбора); остальные тела передаются потоком в обе стороны. Ответ отдаётся сырыми байтами (`aiter_raw`), без декодирования `Content-Encoding`. Hop-by-hop заголовки удаляются, включая перечисленные в `Connection`; заголовки `httpx` по умолчанию и `server`/`date` от uvicorn не добавляются. Собственные ответы прокси — только 502 (каталог недоступен) и 504 (таймаут). Сверка целостности `IntegrityProbe`/`IntegrityTransport`: SHA-256 тела и мультимножество заголовков на входе прокси против того, что передано транспорту, на выборке `proxy.integrity_sample_rate`. Режимы `full | passthrough`. Fail-open: вызов advisor в `asyncio.wait_for(…, 20 мс)`, любое исключение → пересылка без удержания; ошибки собственного учёта (разбор, журнал, обогащение) не влияют на ответ.
- `src/capl/proxy/commit_parser.py` (2.2): pydantic-модели `CommitTableRequest`/`CommitTransactionRequest`, разбор пути по регулярным выражениям (prefix необязателен, `%1F` в namespace), `classify()` по классу коммита Iceberg 1.12.0 (ссылки `файл:строка` в docstring и в `tz/04` §1.2), `has_deletes`, `flink.job-id`, `capl.op-id`, тип ошибки из тела ответа.
- `src/capl/proxy/hold.py` (2.6): ворота удержания с жёстким дедлайном `min(max_ms, hold.max_ms)`; освобождение — явное, по успешному коммиту указанного слота конкурента, по дедлайну (ворота снимаются), `release_all` при остановке прогона.
- `src/capl/telemetry.py` (2.3): asyncpg, батчи, `commit_attempts`, `commit_events`, `commit_partitions`, `client_reports`. Неблокирующая очередь; потери считаются метрикой.
- `src/capl/metrics.py` (2.3): метрики Prometheus с именами из дашборда Grafana, плюс `capl_proxy_phase_seconds{phase}`, `capl_fail_open_total`, `capl_body_integrity_total`, `capl_enrich_*`, `capl_telemetry_*`.
- `src/capl/enricher.py` (2.4): пул воркеров. Читает manifest list нового снапшота и манифесты с `added_snapshot_id` = снапшот (PyIceberg FileIO + fastavro, в потоке). Берёт записи ADDED/DELETED этого снапшота (наследование `snapshot_id` в v2), агрегирует их по партициям (`dt=…/key_bucket=…`), для DML/компакции сохраняет пути удалённых файлов. Пишет `commit_events`, `commit_partitions` и событие в `state`.
- `src/capl/state.py` (2.5): кольцевые буферы по таблицам, интенсивности по окнам 60/300/900 с (смесь с весами, события в момент `t` не учитываются), детектор периодических писателей (коэффициент вариации интервалов, период, фаза, распределение партиций), привязка слот → `op_id`.
- `src/capl/advisor/app.py`: заглушка advisor этапа 2 — реестр удержаний `POST/DELETE /v1/debug/hold`, флаг отказа `PUT /v1/debug/advisor {fault: none|down|error|slow}`, режим, слоты, `GET /v1/state`, `POST /v1/debug/flush`; совместима с тестом матрицы этапа 1.
- `src/capl/config.py`: загрузка `config/capl.yaml` с `${VAR}`; `src/capl/main.py` — три приложения на 8180/8190/9101 в одном процессе.
- `scripts/capture_commit_fixtures.py`, `tests/fixtures/commits/` (24 файла): реальные тела коммитов, снятые через прокси. Источники: Spark 4.0 — append v2/v3, `overwritePartitions` с опциями, статический `INSERT OVERWRITE`, CoW DELETE/UPDATE/MERGE, metadata delete, MoR DELETE/UPDATE/MERGE в v2 и v3, `rewrite_data_files`, `rewrite_position_delete_files`, `expire_snapshots`, CTAS, ALTER; Java API — RowDelta с equality deletes, мульти-табличная транзакция; Flink — append и upsert. Ожидаемый класс — выполненная операция, а не вывод парсера.
- Тесты: unit — 91 (`make test-unit`); integration — `tests/integration/test_proxy_stage2.py` (`make proxy-verify NAME=…`).
- `Makefile`: `venv` (uv, Python 3.11 — системный Python 3.10 не подходит для `src/capl`), `proxy-verify`, `fixtures-capture`; lint/typecheck/test-unit берут инструменты из `.venv`.

## 3. Результаты проверок

| Проверка (план / REVIEWER §4) | Тип | Ожидаемо | Фактически | Итог |
| --- | --- | --- | --- | --- |
| Unit: разбор 20+ реальных тел коммитов, `pytest tests/unit/test_commit_parser.py` | auto | 20+ тел, классы верны | 24 реальных тела, 29 тестов в файле: 24 из 24 фикстур дали ожидаемые `op_class`, `iceberg_op`, `has_deletes`, `is_flink`, id снапшотов и manifest-list. Классы: append, overwrite, cow_dml, mor_dml (v2 и v3), rewrite_data, rewrite_deletes, stream_upsert (Java и Flink), expire, metadata, транзакция | PASS |
| Integration: 1000 append через прокси — 100 % в `commit_attempts` | auto | 100 % | Spark: 1000 append + 20 CoW DELETE + 3 metadata delete + 5 компакций = 1028 снапшотов. В `commit_attempts` — 1028 из 1028 (все 200), в `commit_events` — 1028 из 1028; пропусков 0 (`r1/journal_footprints.json`) | PASS |
| Следы `commit_partitions` = PyIceberg `inspect.entries` на выборке ≥ 100 снапшотов | auto | 0 расхождений | 178 снапшотов (все 28 не-append + 150 append по зерну), 457 пар (снапшот, партиция): 0 расхождений по числу добавленных/удалённых data-файлов и добавленных delete-файлов. PyIceberg читает каталог напрямую, не через прокси | PASS |
| Прокси не меняет тело и заголовки (SHA-256 на выборке) | auto | 0 несовпадений | доля выборки 1.0 в тесте журнала: 5176 сверок, 0 несовпадений (`capl_body_integrity_total`). Unit: 6 типов запросов (коммит, `/v1/config` с query, `%1F`, OAuth, DELETE, HEAD) — тело и заголовки побайтно равны, hop-by-hop удалены, повторы заголовков сохранены. Проверка способна упасть: `test_integrity_probe_detects_change` | PASS |
| Ответы каталога без изменений, включая коды ошибок | auto | тождественны | стенд: 8 запросов (200, 404, 204 HEAD, 409, 500) через прокси и напрямую — одинаковы статус, заголовки (кроме `Date`) и SHA-256 тела (`r1/responses_identical.json`). Unit: 8 кодов × {буферизованный, потоковый} путь, gzip-тело не декодируется, `Set-Cookie` ×2 | PASS |
| Overhead bypass vs proxy, ≥ 1000 коммитов, p50/p95 (NFR-1: p50 ≤ 5 мс, p95 ≤ 15 мс) | auto | ≤ 5 / ≤ 15 мс | PyIceberg fast append, время запроса коммита (`commit_table`), по 1000 на путь, порядок путей перемешан по зерну. Прокси: p50 28,1 / p95 61,0 мс; bypass: 25,2 / 57,2 мс. Накладные: **p50 +2,9 мс** (95 % ДИ бутстрепа 1,4…4,5), **p95 +3,8 мс** (ДИ −16,1…+15,8). Собственное время прокси (`capl_proxy_overhead_seconds`): среднее 0,20 мс. На 328 коммитах Spark (`r1-phases`) все фазы < 0,5 мс | PASS (p95 — точечная оценка, см. §6) |
| Fail-open: остановка advisor во время нагрузки | auto | коммиты проходят без удержания | 4 писателя, удержания на 5 с на всех слотах обновляются каждые 0,5 с. advisor `down`/`error`/`slow` — 197/132/105 коммитов, ошибок 0. Максимальная длительность append 283/349/650 мс (удержание дало бы ≥ 5000 мс). `capl_fail_open_total`: +195/+132/+106. После восстановления удержания снова работают (4) | PASS |
| Классификация `op_class` на фикстурах всех типов, включая metadata delete и Flink | auto | — | см. первую строку: `spark_metadata_delete` → `cow_dml` (`delete`), `flink_append` → `append` с `flink.job-id`, `flink_upsert_v2` → `stream_upsert` | PASS |
| [manual] Дашборд Grafana показывает коммиты и задержки | manual | данные на панелях | «CAPL — конфликты и решения»: 5 из 8 панелей с данными (коммиты по статусу, p95 каталога, накладные прокси, удержания, очередь обогащения). Пустые 3 — решения advisor, риск, латентность advisor (этапы 5+). `r1/grafana_check.json` — запросы через API Grafana | PASS |
| Регрессия: тест матрицы этапа 1 на новом прокси | auto | исходы iter5 | `cow_dml × rewrite_data` и `mor_delete × append` (v2): K2 20/20, контроль K1 5/5, 25/25 повторов удержаны и в окне. Ужесточённые проверки (S1-R4-01/02) пройдены (`r1/matrix-regression`) | PASS |
| `make lint typecheck test-unit` | auto | зелёные | `All checks passed!`; `Success: no issues found in 14 source files`; `91 passed` | PASS |

Команды: `make venv`, `make lint typecheck test-unit`; `make up`; `make fixtures-capture`; `make proxy-verify NAME=r1` (`4 passed in 852.38s`, `results/stage-2/r1/pytest.log`). Регрессия матрицы: `docker compose … exec -T -e MATRIX_APPEND=1 -e MATRIX_FORMAT_VERSIONS=2 -e MATRIX_OUT=/workspace/results/stage-2/r1/matrix-regression -e MATRIX_ONLY=<кейс> runner pytest -q tests/integration/test_conflict_matrix.py`.

## 4. Полученные результаты

Проверки, способные упасть (P3), и то, что они реально поймали при разработке:

| Дефект | Как найден | Исправление |
| --- | --- | --- |
| Прототип этапов 0–1 возвращал декодированное тело ответа без `Content-Encoding` | unit `test_response_returned_unchanged` (gzip) | сырые байты, ADR-007 п. 3 |
| К пути добавлялся `?` при пустом query; `%1F` в пути вызывал `InvalidURL` | unit `test_request_forwarded_unchanged` | `raw_target()` |
| Каталог отвечает на транзакционный коммит кодом 204 — прокси не ставил его в обогащение | снятие фикстуры `java_multi_table_transaction` | успех = 2xx |
| Ответы 500 каталога различались телом | `test_responses_identical_to_catalog` | Jetty вписывает полученный `Host`; прокси передаёт `Host` без изменений. Тест сравнивает при одинаковом `Host` |

Хвост внутренней метрики накладных расходов (до 50 мс) на первом прогоне объясняется не прокси. Гистограмма копилась с запуска контейнера и включала отладочный прогон fail-open в режиме `slow`: там к каждому коммиту по построению добавляется таймаут advisor 20 мс. Кроме того, таймер стартовал до чтения тела. После разделения на фазы (`capl_proxy_phase_seconds`; накладные — от полностью принятого тела до пересылки) на 328 коммитах Spark все наблюдения < 0,5 мс, сумма — 0,061 с (`r1-phases/metrics.txt`).

## 5. Отклонения от ТЗ

| Пункт ТЗ | Что изменено | Причина | ADR |
| --- | --- | --- | --- |
| FR-PROXY-1 «без изменений» | на уровне HTTP-сообщения; reason-phrase и регистр имён заголовков не сохраняются | ограничение ASGI/h11; RFC 9112 §4, RFC 9110 §5.1 | ADR-007 |
| tz/04 §1.2 классификация | статический `INSERT OVERWRITE` → `cow_dml`; metadata delete → `cow_dml`; добавлен класс `metadata` | один класс коммита `OverwriteFiles` — по телу не различимы; истинный тип даёт привязка слот → `op_id` | ADR-007 |
| tz/08 §7: 5000 коммитов на путь | 1000 на путь | порог чек-листа этапа 2 — ≥ 1000; серия на 5000 — нагрузочная проверка AC-5 (этап 8) | — |
| Шаг 2.1 «потоковая передача тела» | тело **коммита** читается целиком | нужно для разбора и удержания; тела коммитов — единицы КБ. Остальные запросы и все ответы — потоком | ADR-007 |
| Шаг 2.5 «периодический сброс состояния в Postgres» | не реализован | следы уже пишет enricher в `commit_events`/`commit_partitions`; снимок состояния нужен с появлением intents (этап 5) | — |

## 6. Проблемы и открытые вопросы

- **Тег `stage-1` (нужно действие автора).** Чтобы хук пропустил тег, автору нужно (а) записать решение одной строкой `Решение автора: CORRECT` в `docs/gate-G1-report.md`, (б) получить ревью этапа 1, покрывающее HEAD (после r4 изменён `docs/REVIEWER.md`). Либо поставить тег вручную вне Claude: `git tag stage-1 a2167ec`. Агент оба файла менять не может (хук).
- p95 накладных расходов: точечная оценка +3,8 мс, но 95 % ДИ разности p95 широк (−16…+16 мс) при n = 1000. Задержка коммита в каталоге (запись metadata.json в S3) сама имеет разброс десятки мс. Более точная оценка — серия AC-5 (5000 коммитов, этап 8). Независимое свидетельство — собственное время прокси: среднее 0,2 мс, на коммитах Spark все < 0,5 мс.
- Бенчмарк NFR-1 выполнен клиентом PyIceberg, а не Spark (P4): время отдельного HTTP-запроса коммита в Spark из клиента не измерить без изменения Iceberg. Для коммитов Spark приведена внутренняя метрика прокси.
- Пропускная способность 200 коммитов/с к заглушке (tz/08 §7) — к этапу 8.
- Английский подзаголовок README не приведён к новому названию — решение автора.

## 7. Что переносится в текст ВКР

В данной работе прокси REST-каталога реализован как прозрачный посредник на уровне HTTP-сообщения. Тело и цель запроса, а также поля заголовков, кроме hop-by-hop, передаются побайтно; это подтверждено сравнением SHA-256 на 5176 запросах без единого расхождения. Ответы каталога, включая коды ошибок 404, 409 и 500, неотличимы от полученных напрямую. Восстановленные по манифестам следы коммитов совпали с независимым чтением метаданных PyIceberg на 178 снапшотах (457 пар «снапшот × партиция»). Прокси зафиксировал 1028 из 1028 снапшотов. Накладные расходы на запрос коммита составили +2,9 мс по медиане (95 % ДИ 1,4–4,5 мс) и +3,8 мс по 95-му процентилю при n = 1000, что в пределах NFR-1. При отказе компонента advisor прокси переходит в режим fail-open: 434 коммита из 434 прошли без удержаний.

## 8. Следующий этап

Этап 3 (runner и телеметрия) — по команде автора. В него переносятся:

- проверка SQL `INSERT OVERWRITE … WITH (…)` в dynamic-режиме одним кейсом (tz/05);
- реализация поля `format_versions` операции в загрузчике сценариев;
- `validate_from_snapshot_id: planning_snapshot` в исполнителе overwrite.

## Ответы на ревью

Замечания MINOR ревью этапа 1 (r4), перенесённые в этот этап:

| ID | Что сделано | Где | Подтверждение |
| --- | --- | --- | --- |
| S1-R4-01 | `control_counts` считается только по валидным контрольным повторам; все повторы — в отдельном поле `control_counts_all`. K2 в контроле без пересечения проверяется по всем повторам (строже) | `tests/integration/test_conflict_matrix.py` (`_run_case`, `_assert_case`), `0998333` | регрессия на новом прокси: `control_counts = control_counts_all = {K1: 5}` в обоих кейсах (`r1/matrix-regression`) |
| S1-R4-02 | Комментарий засева контроля исправлен: «хотя бы в одной партиции ≥ 2 файлов; в iter5 переписано 7 из 8». Допуск ужесточён до `control_valid == TRIALS_CONTROL`. Формулировка в отчёте этапа 1 исправлена | `_sc_trial`, `_assert_case`, `docs/stage-1-report.md`; `0998333` | ужесточённая проверка пройдена в регрессии (5/5 валидных) |
| S1-R4-03 | Отчёт этапа 1: «K2 = 0 во всех **парных** кейсах»; у одиночного кейса `control_counts` не заполняется; `update_matrix.build_verified` пишет исходы одиночного кейса в `observed`, а YAML-записи `overwrite_iso_only` приведены к этому виду. ADR-006: дата 2026-10-24, статус «Принято» | `test_overwrite_iso_only`, `scripts/update_matrix.py`, `config/conflict_matrix.yaml:291-292`, ADR-006; `0998333` | `grep iso_only config/conflict_matrix.yaml` → `"observed": {"K2": 5}, "control": {}` |
