# 03. Модель конфликтов, признаки, ML и политики

## 1. Классы операций

| Код | Операция | API Iceberg | Как запускается на стенде |
| --- | --- | --- | --- |
| `append` | INSERT INTO, потоковый append | `AppendFiles` (fast append) | Flink SQL sink; Spark `INSERT INTO`; Spark Structured Streaming |
| `stream_upsert` | Потоковый upsert с equality deletes | `RowDelta` (equality deletes) | Flink SQL sink `'write.upsert.enabled'='true'` |
| `overwrite` | INSERT OVERWRITE (dynamic) | `ReplacePartitions` | Spark SQL |
| `cow_dml` | DELETE/UPDATE/MERGE copy-on-write | `OverwriteFiles` | Spark SQL, `write.{delete,update,merge}.mode=copy-on-write` |
| `mor_dml` | DELETE/UPDATE/MERGE merge-on-read | `RowDelta` (position deletes / DV в v3) | Spark SQL, `...mode=merge-on-read` |
| `rewrite_data` | Компакция | `RewriteFiles` | `CALL system.rewrite_data_files(...)` |
| `rewrite_deletes` | Переписывание delete-файлов | `RewriteFiles` (delete files) | `CALL system.rewrite_position_delete_files(...)` |
| `expire` | Удаление старых снапшотов | `ExpireSnapshots` | `CALL system.expire_snapshots(...)` |
| `orphan_cleanup` | Удаление orphan-файлов | — (без коммита) | `CALL system.remove_orphan_files(...)` |

Класс писателя `writer_class` ∈ {`stream`, `batch_dml`, `maintenance`, `user`} определяется заголовком `X-CAPL-Writer` или, для Flink, наличием `flink.job-id` в summary снапшота.

## 2. Матрица конфликтов

Машиночитаемая версия — [../config/conflict_matrix.yaml](../config/conflict_matrix.yaml). Ниже — смысловая таблица для уровня изоляции `serializable` (по умолчанию в Spark). Строка — операция, которая коммитит позже (ожидающая); столбец — операция, закоммиченная после базового снапшота ожидающей. Ячейка применима, только если следы пересекаются (условие пересечения указано в YAML: по партициям или по файлам).

| ожидающая ↓ / закоммиченная → | append | stream_upsert | overwrite | cow_dml | mor_dml | rewrite_data |
| --- | --- | --- | --- | --- | --- | --- |
| append | K1 | K1 | K1 | K1 | K1 | K1 |
| stream_upsert | K1 | K1 | K1 | K1 | K1 | K1 |
| overwrite | K2 (partition) | K2 (partition) | K2 (partition) | K2 (partition) | K2 (partition) | K2 (partition) |
| cow_dml | K2 (partition, serializable) / K1 (snapshot) | K2 (partition, serializable) | K2 (partition) | K2 (file) | K2 (file: new deletes for read files) | K2 (file: read files removed) |
| mor_dml | K2 (partition, serializable) / K1 (snapshot) | K2 (partition, serializable) | K2 (partition) | K2 (file) | K2 (file) | K2 (file: referenced files removed) |
| rewrite_data | K1 | K1 (при `use-starting-sequence-number=true`) | K2 (file) | K2 (file) | K2 (file: new pos deletes / DV) | K2 (file) |

Пояснения:

1. «partition» — достаточно пересечения партиций с фильтром детекции конфликтов операции; «file» — требуется пересечение множеств файлов (для DML — файлов, содержащих затрагиваемые строки).
2. Для пар `cow_dml/mor_dml × rewrite_data` исход симметричен: проигрывает та операция, которая коммитит второй. Это основа правила удержания (§6).
3. Строка `overwrite`: для `INSERT OVERWRITE` в Spark проверки конфликтов включаются опцией записи `isolation-level` (отдельного табличного свойства, как для DML, нет); поведение по умолчанию фиксируется на этапе 1, а в сценариях опция задаётся явно.
4. Все значения матрицы являются гипотезами, проверяемыми на этапе 1 микро-экспериментами (08_validation_and_testing.md §3). Фактическая версия матрицы фиксируется в YAML вместе с версией Iceberg.

## 3. Аналитическая модель

### 3.1. Интенсивности

Для таблицы $T$, партиции $p$ и класса операции $k$ поддерживается оценка интенсивности коммитов $\lambda_{k,p}(t)$:

- **Непериодические классы** — экспоненциально взвешенная оценка по окнам 1/5/15 мин:
  $\hat\lambda^{(w)}_{k,p}(t) = \frac{1}{w}\sum_{e \in E_{k,p},\ t-w \le t_e < t} 1$, итоговая — смесь окон с весами из конфигурации.
- **Периодические писатели** (потоковые чекпойнты): для каждого `writer_id` оцениваются период $T_w$ (медиана интервалов) и фаза; число коммитов в интервале $[t_1, t_2]$ прогнозируется как $\lfloor (t_2 - \phi)/T_w \rfloor - \lfloor (t_1 - \phi)/T_w \rfloor$, а партиции — по эмпирическому распределению партиций последних $n$ коммитов писателя (горячая партиция + доля поздних событий).
- **Зарегистрированные операции** (intents в состоянии `running`): учитываются не через интенсивность, а явно (§3.3).

### 3.2. Вероятность пересечения на уровне файлов

Для пар с условием «file» вводится $q_{k,p}$ — вероятность того, что коммит класса $k$ в партиции $p$ затронет хотя бы один файл из следа операции $o$:

$$q_{k,p}(o) \approx 1 - \Bigl(1 - \frac{|F_{\text{files}}(o) \cap p|}{N_p}\Bigr)^{m_{k,p}},$$

где $N_p$ — число data-файлов партиции, $m_{k,p}$ — среднее число файлов, изменяемых одним коммитом класса $k$ в $p$ (оценивается из `commit_event`). Для компакции, переписывающей все мелкие файлы партиции, $q \to 1$ при любом DML в той же партиции. Для условия «partition» $q = 1$.

### 3.3. Вероятность конфликта данных

Пусть $K(\tau)$ — классы, конфликтующие с типом $\tau$ по матрице (с учётом уровня изоляции), $F$ — партиции следа, $\hat D$ — прогноз длительности до коммита. Вклад фонового потока:

$$\Lambda_{\text{bg}}(o) = \sum_{p \in F}\sum_{k \in K(\tau)} q_{k,p}(o)\int_{t_s}^{t_s+\hat D}\lambda_{k,p}(t)\,dt.$$

Вклад зарегистрированных конкурентов $b \in B(o)$ (активные операции с пересекающимся следом и конфликтующим типом): если исход симметричен, $o$ проигрывает при $T_b < T_o$, где $T$ — момент коммита; $\pi_b = P(T_b < T_o)$ оценивается по ETA (прогноз длительности и прогресс из heartbeat) в предположении логнормальных ошибок. Если $o$ проигрывает всегда (например, `cow_dml` против `append`), $\pi_b = P(T_b \in [t_s, T_o])$.

$$P_{K2}(o) = 1 - e^{-\Lambda_{\text{bg}}(o)}\prod_{b \in B(o)}\bigl(1 - \pi_b\, q_{ob}\bigr).$$

### 3.4. Вероятность исчерпания повторов (отказ К1)

Пусть $\Lambda(t)$ — интенсивность всех коммитов таблицы (кроме самой операции), $a$ — длительность одной попытки (обновление метаданных + запись манифестов + CAS), $r$ — `commit.retry.num-retries`, $w_i$ — паузы backoff. Первая попытка опирается на базовый снапшот момента запуска, поэтому

$$P_{K1}(o) \approx \bigl(1 - e^{-\Lambda \hat D}\bigr)\prod_{i=1}^{r}\bigl(1 - e^{-\Lambda(t_i)\,a}\bigr),\quad t_i = t_s + \hat D + \sum_{j<i}(a + w_j).$$

Также учитывается ограничение `commit.retry.total-timeout-ms`.

### 3.5. Длительность и стоимость

- $\hat D(o)$ — модель длительности (§4.3); до её обучения — линейная по байтам с коэффициентами, оцененными на этапе 4.
- $c(o)$ — ожидаемые CPU-секунды: $\hat c = \alpha_\tau \cdot \text{bytes}$ (калибровка по `ops.cpu_ms`).
- $b(o)$ — польза: для `rewrite_data` — ожидаемое сокращение числа файлов $\sum_{p}\max\bigl(0,\ n^{small}_p - \lceil B^{small}_p / S_{target}\rceil\bigr)$, переведённое в CPU-эквивалент коэффициентом `benefit.file_value_cpu_s`; для DML и удалений $b$ не используется (они обязательны, допускается только задержка в пределах SLA).

Ожидаемые потери: $\mathbb{E}[W] = (P_{K2} + P_{K1}) \cdot \hat c$.

## 4. Признаки и ML

### 4.1. Признаки

Полный список с формулами — [../config/features.yaml](../config/features.yaml). Группы:

| Группа | Примеры |
| --- | --- |
| Операция | `op_type`, `mode`, `isolation`, `n_partitions`, `n_files_est`, `bytes_est_log`, `dur_pred_log`, `format_version` |
| Интенсивности в следе | `lam_{class}_{1m,5m,15m}_sum`, `lam_{class}_max_partition`, `periodic_commits_expected_in_window` |
| Время | `secs_since_last_commit_in_footprint_min`, `secs_since_last_conflicting_commit` |
| Конкуренты | `n_active_overlapping`, `n_active_symmetric`, `min_eta_competitor_s`, `sum_cost_competitors` |
| История | `k2_rate_footprint_1h`, `k2_rate_pair_types_1h`, `avg_attempts_table_15m` |
| Таблица | `table_commit_rate_1m`, `avg_files_per_partition`, `delete_files_ratio` |
| Аналитика | `p_k2_analytic`, `p_k1_analytic`, `lambda_bg` |

Требование point-in-time: функция `features.compute(intent, state_at(t))` использует только события с `committed_at < t`. Для офлайн-сборки состояние восстанавливается проигрыванием журнала `commit_events` и `ops` до момента `intent_at` (модуль `ml.dataset`).

### 4.2. Метки

| Метка | Определение |
| --- | --- |
| `y_k2` | `ops.outcome = 'data_conflict'` |
| `y_k1` | `ops.outcome = 'retry_exhausted'` |
| `y_any` | `y_k2 OR y_k1` |
| `y_dur` | `commit_requested_at - started_at` (с) |

Наблюдения: одна строка на выполненную управляемую операцию; признаки — на фактический момент запуска. Операции с исходом `error` исключаются.

### 4.3. Модели

| Модель | Алгоритм | Выход |
| --- | --- | --- |
| Исход К2 | LightGBM binary, `is_unbalance` или веса классов; изотоническая калибровка на отдельном фолде | $\hat p_{K2}$ |
| Исход К1 | LightGBM binary (обучается, если положительных примеров ≥ 200; иначе используется аналитика) | $\hat p_{K1}$ |
| Длительность | LightGBM regression на $\log(1 + D)$; квантильная версия (α = 0.1, 0.9) для ETA | $\hat D$, интервал |
| Альтернатива (опционально) | Модель выживаемости (Cox, `lifelines`) для времени до первого конфликтующего коммита | сравнение в ВКР |

Гиперпараметры: поиск Optuna (50 испытаний) по GroupKFold. Стартовые значения: `num_leaves=31`, `learning_rate=0.05`, `n_estimators≤2000` с early stopping, `min_child_samples=20`, `feature_fraction=0.8`.

### 4.4. Протокол валидации

1. GroupKFold (k = 5) по `run_id` — исключает утечку между операциями одного прогона.
2. Leave-one-scenario-out: обучение на 5 сценариях, тест на 6-м (обобщение на новый тип нагрузки).
3. Sim → real: обучение на симуляторе, тест на стенде (и наоборот) — проверка переносимости.
4. Метрики: PR-AUC (основная, классы несбалансированы), ROC-AUC, Brier, ECE (10 бинов), reliability diagram; для длительности — MAPE и $R^2$ по логарифму.
5. Базовые линии: аналитическая модель; эвристика P4 как классификатор (1, если в следе был конфликтующий коммит за последние $\Delta$); константа (доля положительных).
6. Значимость различий — bootstrap по прогонам (10 000 выборок), 95% CI разности PR-AUC.

## 5. Политики решения о запуске

Общий интерфейс:

```python
class Policy(Protocol):
    name: str
    def decide(self, intent: Intent, state: StateView, models: Models, now: float) -> Decision: ...
    def hold(self, commit: CommitAttempt, state: StateView, models: Models, now: float) -> HoldDecision: ...
```

Кандидаты действий для intent: $\mathcal{A} = \{\text{now}, +\Delta_1, \dots, +\Delta_m\} \times \{F, F'_1, \dots\}$, где $\Delta_j$ из сетки `policy.delay_grid_s` с $\Delta_j \le \text{SLA}$, а $F'_i$ — сужения следа (только для `rewrite_data` и `rewrite_deletes`): жадное удаление партиций по убыванию вклада в $\Lambda_{\text{bg}}$.

| Политика | `decide` | `hold` |
| --- | --- | --- |
| P0 `occ_default` | всегда `run_now(F)` | всегда `pass` |
| P1 `retry_tuned` | как P0; runner выставляет `commit.retry.num-retries=10`, `min-wait-ms=100`, `max-wait-ms=60000` | `pass` |
| P2 `static_partition` | для обслуживания: $F' = \{p \in F : \text{age}(p) \ge N\}$; DML — `run_now` | `pass` |
| P3 `maintenance_window` | обслуживание — только внутри окон `[k·W, k·W+L)`, иначе `delay` до начала окна; DML — `run_now` | `pass` |
| P4 `reactive_recent` | исключить из $F$ партиции с коммитом конфликтующего класса за последние $\Delta_r$; если после исключения $F' = \varnothing$ — `delay(Δ_r)`; DML — `delay`, если в следе был конфликтующий коммит за $\Delta_r$ и SLA позволяет | `pass` |
| P5 `analytic` | $\arg\max_{a \in \mathcal{A}}\ b(a) - (P_{K2}+P_{K1})(a)\,\hat c(a) - \mu\cdot\text{delay}(a)$ с аналитическими оценками; ограничения §5.1 | `pass` |
| P6 `ml` | то же с $\hat p$ от ML (fallback — аналитика) | правило §6 |
| `explore` | с вероятностью ε случайный кандидат, иначе P5; логирует propensity для off-policy оценки | `pass` |
| P★ `oracle` | только в симуляторе: известна будущая трасса; выбирается кандидат без конфликта с минимальной задержкой | идеальный порядок |

### 5.1. Ограничения

- **SLA:** если `now + Δ > intent.sla_deadline`, кандидат исключается; если исключены все отложенные — `run_now` с минимальным риском по сужениям.
- **Долг обслуживания:** если $\text{Debt}_T(t) > \text{Debt}_{\max}$ (из конфигурации таблицы), задержка обслуживания запрещена (разрешено только сужение).
- **Защита от голодания:** число откладываний одного intent ≤ `policy.max_deferrals`; после — `run_now`.
- **Детерминизм:** при равных оценках выбирается кандидат с меньшей задержкой, затем с большим следом.

## 6. Правило удержания коммита (P6)

При получении коммита операции $A$ (известна по слоту):

1. Найти активные операции $B$ с: (а) симметричным К2 относительно $A$ по матрице; (б) пересечением следов; (в) $\text{ETA}_B - \text{now} \le \texttt{hold.max\_ms}$ с вероятностью ≥ `hold.eta_confidence` (квантиль модели длительности); (г) $\hat c(B) \ge \kappa\,\hat c(A)$, $\kappa$ = `hold.cost_ratio` (по умолчанию 2).
2. Если $B$ найдено — удерживать $A$ до коммита $B$ (событие от прокси), отмены $B$ или дедлайна; затем пропустить.
3. Никогда не удерживать: `append`, `stream_upsert` (неуправляемые писатели), коммиты без привязки к операции, коммиты при включённом fail-open.
4. Дедлайн всегда меньше клиентского таймаута: `hold.max_ms ≤ 0.5 · rest.client.socket-timeout-ms`.

Ожидаемый выигрыш правила на одну пару: $\hat c(B) - \hat c(A)$ при условии, что без удержания $A$ закоммитил бы первым. Эффект измеряется отдельно (ablation P6 с `hold.enabled=false`).
