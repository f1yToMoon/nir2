# 05. Нагрузка и эксперименты

## 1. Данные

Синтетическая таблица `bench.events` (схема — [../config/workloads/_base.yaml](../config/workloads/_base.yaml)):

| Параметр | full | lite |
| --- | --- | --- |
| Строк при инициализации | 30 млн | 9 млн |
| Партиционирование | `dt` (30 логических дней) × `bucket(8, key)` = 240 партиций | то же |
| Файлов на партицию при инициализации | 40 (мелкие, 2–6 МБ) | 20 |
| Размер на диске (Parquet, zstd) | ≈ 4–6 ГБ | ≈ 1.5 ГБ |
| Кардинальность `key` | 5 млн | 1.5 млн |

Почему синтетика, а не открытый датасет: управляемое распределение ключей по партициям и времени необходимо для контроля степени пересечения следов; семантика конфликтов не зависит от содержимого строк. Для ВКР дополнительно описывается, как те же сценарии строятся над TPC-DS `store_sales` (опционально, если останется время).

Генерация детерминирована зерном (`numpy.random.Generator(PCG64(seed))`); `event_id` уникален и монотонен, что используется проверкой инвариантов.

### 1.1. «Золотая» таблица и сброс

1. `make seed SCENARIO=<id>` один раз создаёт таблицу по сценарию (свойства, режимы, format version), заливает данные с нужной фрагментацией и сохраняет путь текущего `metadata.json` в `data/golden/<scenario>.json`.
2. Перед каждым прогоном runner: `DROP TABLE bench.events` (без `PURGE`) → `CALL lake.system.register_table('bench.events', '<golden metadata.json>')`. Это занимает секунды и даёт идентичное начальное состояние.
3. После прогона файлы, записанные прогоном (всё, что не принадлежит золотой версии), удаляются фоновой задачей `make gc` по префиксу `s3://warehouse/bench/events/data/` с фильтром по времени создания — до следующего прогона это не влияет на метрики.

## 2. Сценарии

| ID | Файл | Неуправляемые писатели | Управляемые операции | Ожидаемый класс конфликта |
| --- | --- | --- | --- | --- |
| S0 | `S0_baseline.yaml` | Flink append, 1000 соб/с, период 10 с | компакция старых партиций, редкий MERGE по старым дням | почти нет (контроль накладных расходов) |
| S1 | `S1_compaction_vs_stream_append.yaml` | Flink append (5 с) + 6 «агентов»-аппендеров (3 с) | компакция горячих партиций | К1, исчерпание повторов |
| S2 | `S2_compaction_vs_cdc_merge.yaml` | Flink append | компакция + CDC MERGE (CoW), Zipf по свежести | К2 симметричный |
| S3 | `S3_compaction_vs_mor_dml.yaml` | Flink append | компакция + MoR UPDATE + rewrite delete files | К2 симметричный; вариант v3 |
| S4 | `S4_merge_vs_stream_serializable.yaml` | Flink append, 15% поздних событий | пакетный MERGE, overwrite дня | К2, проигрывает MERGE; вариант snapshot |
| S5 | `S5_compliance_delete.yaml` | Flink append | DELETE по ключам во всех днях, компакция, CDC MERGE | К2, длинные операции с широким следом |
| S6 | `S6_mixed_drift.yaml` | Flink append, Zipf ключей, дрейф | все типы, синусоидальная интенсивность | смешанный; отложен для обобщения |

Уровни конкурентности `low/mid/high` масштабируют интенсивности и лимиты параллелизма (см. `concurrency_levels`).

### 2.1. Селекторы следа

| Селектор | Логика |
| --- | --- |
| `small_files_topn` | партиции с числом файлов ≥ `min_files` и возрастом ≥ `min_age_days`, top-N по числу мелких файлов (упрощённый AutoComp) |
| `delete_files_topn` | top-N партиций по числу delete-файлов |
| `recency_zipf` | число партиций — равномерно из диапазона; день выбирается по Zipf(`zipf_a`) от `logical_today` в пределах `days`, бакет — равномерно |
| `keys_all_days` | `n_keys` ключей из `buckets` бакетов → все дни этих бакетов |
| `single_day` | один день из диапазона возраста, все бакеты |
| `table` | вся таблица (expire) |

### 2.2. Операции

| `statement` | SQL (эскиз) |
| --- | --- |
| `merge_upsert` | `MERGE INTO s{k}.bench.events t USING batch s ON t.event_id = s.event_id AND t.dt = s.dt AND t.key = s.key WHEN MATCHED THEN UPDATE SET amount = s.amount, status = s.status, version = t.version + 1 WHEN NOT MATCHED THEN INSERT *` — пакет строится из существующих `event_id` следа (доля `update_fraction`) и новых строк |
| `update_status` | `UPDATE s{k}.bench.events SET status = 'X', version = version + 1 WHERE dt IN (...) AND key % 97 = r` |
| `delete_by_keys` | `DELETE FROM s{k}.bench.events WHERE key IN (...)` |
| overwrite | `INSERT OVERWRITE s{k}.bench.events SELECT ... WHERE dt = d` (dynamic), опция `isolation-level` |
| rewrite_data | `CALL s{k}.system.rewrite_data_files(table => 'bench.events', where => '<предикат следа>', options => map('min-input-files','5','partial-progress.enabled','false', ...))` |
| rewrite_deletes | `CALL s{k}.system.rewrite_position_delete_files(table => 'bench.events', where => '...')` |
| expire | `CALL s{k}.system.expire_snapshots(table => 'bench.events', older_than => TIMESTAMP '...', retain_last => 20)` |

Предикат следа всегда включает `dt` и `bucket`-условие (`bucket(8, key)` выражается через `system.bucket(8, key) = b`), чтобы след операции совпадал с декларацией. Проверка: `ops.footprint_executed` (из `commit_partitions`) ⊆ `footprint_requested` — несоответствие считается ошибкой runner.

## 3. Метрики

| Метрика | Определение | Источник |
| --- | --- | --- |
| `k2_rate` | доля управляемых операций с исходом `data_conflict` | `ops` |
| `k1_fail_rate` | доля с `retry_exhausted` | `ops` |
| `wasted_cpu_s` | Σ `cpu_ms/1000` по операциям с К2/отказом К1 (основная метрика H2) | `ops` |
| `wasted_share` | wasted_cpu_s / Σ cpu всех операций | `ops` |
| `wasted_bytes` | байты, записанные проигравшими операциями | `ops` |
| `goodput` | успешные управляемые коммиты в минуту | `ops` |
| `commit_lat_p50/p95/p99` | `responded_at − received_at` успешных попыток (прокси) и `commit_requested_at → finished_at` (runner) | `commit_attempts`, `ops` |
| `proxy_overhead` | `forwarded_at − received_at − hold_ms` + обратный путь | `commit_attempts` |
| `decision_delay` | `started_at − intent_at` | `ops` |
| `sla_violations` | операции, запущенные позже `sla_deadline` | `ops` |
| `debt_mean/max` | средняя/максимальная по времени доля мелких файлов (< 32 МБ) по таблице | `table_health` |
| `delete_ratio` | delete-файлы / data-файлы (S3, S6) | `table_health` |
| `stream_freshness_p95` | p95 задержки от времени события до коммита, содержащего его (по `event_ts` и `committed_at`) | Kafka timestamps + `commit_events` |
| `orphan_bytes` | объём orphan-файлов после прогона | `orphan_scan` |
| `pred_*` | PR-AUC, ROC-AUC, Brier, ECE по прогнозам на момент запуска | `ops.p_*_pred` |

## 4. Протокол прогона

1. `POST /v1/runs` (advisor) с политикой; сброс таблицы (§1.1); установка задержки Toxiproxy.
2. Старт неуправляемых писателей; прогрев `warmup_s`.
3. Генерация управляемых операций в течение `duration_s`.
4. Остановка генерации; ожидание завершения операций (`cooldown_s`); остановка потоков.
5. Финальные снимки `table_health`, проверка инвариантов, подсчёт orphan-файлов.
6. `POST /v1/runs/{id}/finish`; расчёт `run_summary`; экспорт трасс в Parquet.

Операции, начатые в прогреве, в метрики не включаются (`intent_at < start + warmup_s`).

## 5. Серии экспериментов

Полная матрица — [../config/experiments/matrix.yaml](../config/experiments/matrix.yaml).

| Серия | Цель | Прогонов | Часов |
| --- | --- | --- | --- |
| E1 | Накладные расходы (H3, AC-5) | 15 | 8 |
| E2 | Сбор данных (explore + P0) | 80 | 43 |
| E3 | Основное сравнение политик (H2) | 180 | 96 |
| E3b | Обобщение на S6 | 12 | 6 |
| E4 | Абляция P6 | 24 | 13 |
| E5 | Iceberg v3 (deletion vectors) | 9 | 5 |
| E6 | Эмуляция задержки S3 | 6 | 3 |
| E7 | Lakekeeper (AC-7) | 12 | 6 |
| E8 | Snapshot isolation | 6 | 3 |
| **Итого стенд** | | **344** | **≈ 183** |
| SIM1 | Проверка симулятора (AC-6) | тысячи | минуты–часы |
| SIM2 | Промышленный масштаб, оракул | ≈ 8×6×36×20 | часы |

Запуск серии: `make batch SERIES=E3` (возобновляемый: прогоны с `runs.status='finished'` пропускаются).

## 6. Статистический анализ

- Единица наблюдения для сравнения политик — прогон. Для каждой пары (политика, P0) в одном сценарии и уровне конкурентности вычисляется относительное изменение `wasted_cpu_s` и его 95% CI бутстрепом по прогонам (10 000 повторов, стратификация по зерну — прогоны с одинаковым зерном сравниваются парно).
- Сравнение P6 с P2–P4 — односторонний критерий Манна–Уитни (α = 0.05) с поправкой Холма на число сравнений.
- Проверка «равного долга»: сравнение `debt_mean` с допуском 10% (TOST-эквивалентность, если хватит прогонов; иначе — сообщение CI).
- Для модели — см. 03_conflict_model.md §4.4.
- Off-policy оценка: по прогонам `explore` с записанными propensity оценивается ожидаемая стоимость P5/P6 методом IPS/DR — дополнительный аргумент, не заменяющий прямые прогоны.

## 7. Графики для ВКР (`make figures`)

1. Доля К2 и wasted CPU по политикам (столбцы с CI) для каждого сценария.
2. Парето-фронт: wasted CPU vs долг обслуживания (точки — прогоны, цвет — политика).
3. Временной ряд прогона: интенсивность коммитов, прогноз риска, моменты решений и конфликтов (S2, P0 vs P6).
4. Reliability diagram и PR-кривые моделей (аналитика, эвристика, ML).
5. Важность признаков (SHAP или gain).
6. Накладные расходы прокси: ECDF латентности коммита bypass/proxy/P6 (S0).
7. Симулятор vs стенд: доли конфликтов по сценариям с CI.
8. Промышленный масштаб: выигрыш P6 и оракула как функция длительности компакции и периода стрима.
9. Матрица конфликтов (тепловая карта наблюдаемых исходов этапа 1).
