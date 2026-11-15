# Новая квалификация R540: приватные сессии

## Итог измерений 15.11.2026 — актуальное состояние

Все семь полных прогонов завершены. Активных pipeline/watcher нет; session71783
и cell148 завершились. Нижние записи — история исполнения, не команды продолжения.
Код S4/S5:764d95fc0b33a42fab35305a1b6fe19371c9ef49; код остальных пяти:
1f506a610ac9bafa27311f8ae75c86557e94de83. plan-amendment.json фиксирует происхождение.

checks.json:94/94, failed=[]; verification.json:517 файлов,22 262 600 событий,
все хеши/времена/счётчики верны, p95 точно совпал в7/7. Матрица partition-only
v2/v3:80/80 overlap K2,20/20 control K1, invalid0. График S2 построен и осмотрен.
Старые отрицательные/диагностические серии сохранены,602 хеша проверены.

Оговорки: одно восстановление Flink S1; health gap S4 max158,434401с;
инъекции5 на S5+1 на отдельной таблице. Полные объяснения и числа —
docs/stage-3-report.md §§3–8,18–19; исходные данные — evidence/manifest.json.

Следующий шаг: заключительная самопроверка, чистый коммит, свежий независимый
stage-reviewer. Формальных раундов на этот срез0/3. Дальнейший вердикт и SHA
определяются новыми docs/reviews/stage-3-r*.md и штатным stage-3 после PASS*.
Измерения не повторять и не перезаписывать. Этап4 не начинать.

## Актуально после S4/S5 и исправления тестового покрытия

Активен exec session **71783**: STAGE3_FROM=S0, тот же суффикс. HEAD
**1f506a610ac9bafa27311f8ae75c86557e94de83**. Не менять HEAD/исходники до завершения
S0×2/S1–S3. Оригинальный план сохранён; **plan-amendment.json** задаёт фактический
SHA последних пяти прогонов. git diff764d95f..1f506a6 по src/scripts/config/infra/
api/schemas/Makefile/pyproject.toml пуст: measurement-code-equivalence.diff.

Живой watcher — functions cell **148**, опрашивает71783 раз в55с и публикует
последние строки активного сценария. Не опрашивать71783 конкурентно; ждать148.
Код1f506a6 опубликован, origin/main SHA совпал. Источники/HEAD заморожены.

Прежний pipeline84222 завершился exit1 после двух валидных полных S4/S5:
24 integration PASS, но probe потребовал6 инъекций, а в S5 было5. Все4 широкие
DELETE дали K2; deleted_ids пуст. Сохранился отрицательный вывод series.log,
пятый вид не выдаётся за шестой. Исходный S5 после тестов восстановлен.

Добавлен безусловный tests/integration/test_runner_invariants_stage3.py:
две золотые строки, реальный CoW DELETE, повторная вставка удалённой как дефект.
1 passed/24.33s; clean и after_restore все5 true, injected ledger ровно1 лишняя строка,
физический журнал корректен2=2. Отдельные run_id/таблица, S5 неизменён. Новый
verify_fault_coverage.py строго проверил6 типов:5 на S5+1 на контроле, с равенством
исходных счётчиков217attempts/187events/53ops/current3143129065520675882.
Доказательства: fault-coverage-with-control.json и ../retained-deleted-control-20261115.
Preflight325 unit, lint/mypy14/config21. Дополнение — тест/документы/доказательства;
основные результаты S4/S5 не отбираются повторно по исходам DELETE.

S4: validation true,5 invariants,12commit/23K2 (measurement),3517600/3517600 fresh,
p95=14.533032011985778s. Late UI61/61 (34 controlled,6 successful merges×10jobs).
S5: validation true,5 invariants,19commit/6K2,3340600/3340600 fresh,
p95=9.721097767353058s. Подробности в ../runs и полных logs. Formal review0/3.

Дополнительное наблюдение (не скрывать): health-intervals-S4-S5.json содержит все
фактические интервалы. S4 median30.0001s, max158.434401s (36observations);
S5 median30.0001075s, max30.001791s (59observations).30с — заданный период,
не гарантированная фактическая частота под нагрузкой. Байты метаданных на графике
только наблюдённые; плотные пики числа снимков проверяются независимо по RPC.
Не заявлять строгие30с и отсутствие пропусков наблюдений. Включить ограничение
в первичный отчёт/ADR и дать независимому ревьюеру оценить соответствие FR-RUN-6.
После последних пяти — все проверки из нижнего списка, экспорты, отчёт/коммит,
нативный свежий stage-reviewer. Stage4 не начинать.

## Дополнение14:26 UTC

Оба S0 уже finished/validation=true, по11 committed,5/5 invariants.
checks-S0-pair.json/log: failed=[],11 identical intents,11/11 resolved footprints
identical,Jaccard1.0. Готовы4 из7 (S4,S5,S0,S0rep). АктивенS1 (Flink+6agents),
затемS2/S3. Источники/HEAD1f506a6 не менялись. Watcher148 продолжает polling71783.

Промежуточный S1 в14:26:13: lower/upper peak1325 (одинаковые), observed1269,
observed metadata1267654Б. Это НЕ итог квалификации; файл history-S1-interim.json.
Первая и вторая компакции committed после3 и2attempts — ожидаемый K1.
spark-runtime-properties.json подтверждает живой кластер, executor2g, FAIR,
driver3g/max8cores (отфильтрован только безопасный список свойств).

После всей серии не забыть:
- verify_evidence.py уже учитывает plan-amendment.json и выполняет git diff
  неизменности src/scripts/config/infra/api/schemas/Makefile/pyproject.toml.
  Не заменять исходный plan.json; SHA первых2=764d95f, последних5=1f506a6.
- Полноту инъекций описывать5 на S5+1 на отдельной фикстуре. Не выдавать
  первоначальный probe6 как PASS; его отказ сохранён в series.log.
- В первичном отчёте явно дать интервалы health, особенноS4 max158.434401s;
  metadata bytes только observed, плотные snapshot пики отдельно.
- Включить таблицу покрытия5.claude+4external из docs/codex-workflow.md,
  свежие hook trust/discovery и отдельный PostCompact вопрос с неустановленной
  внутренней причиной. Механизм не редактировать.
- Независимых формальных review0/3. Только нативная роль stage-reviewer,
  fork_turns=none, после чистого зафиксированного полного отчёта/результатов.

## Дополнение14:47 UTC

S1 finished/validation=true,5/5 invariants,13committed controlled. Готовы5/7.
АктивенS2, затемS3; watcher148/session71783 ещё работают. Источники/HEAD1f506a6
не менять. S1 all5355700 events fresh, p95=5.072331440448761s, agents3346ok/0failed.

НЕ СКРЫВАТЬ: Flink S1 восстановился1 раз. Полный exceptions/checkpoints JSON:
flink-recovery-S1.json, truncated=false. CommitFailedException, 14:41:09.570 UTC;
flink-conflict-attempts-S1.json показывает5 HTTP409 одного candidate4542388454158207224
между14:41:07.345 и09.327; после restore checkpoint320 другой candidate получил409/200.
Это K1/исчерпание попыток фонового Flink, не ошибка runner; controlled13commit,
потерь/дубликатов итоговые инварианты не обнаружили. Scope чисел ошибок обозначать
явно. CSV verifier дополнить max/p99 latency, чтобы хвост не скрывался за p95.

PostCompact: причина на уровне контракта ТЕПЕРЬ УСТАНОВЛЕНА. Сохранённый stdout
Caveman содержит hookSpecificOutput, официальные схемы0.167/0.169 запрещают это поле
для PostCompact (additionalProperties=false); Rust parser serde deny_unknown_fields
возвращает invalid PostCompact hook JSON output даже при exit0 процесса. Полные
схемы и wire-validation.json — results/codex-workflow/postcompact-20261115.
diagnosis.md и отчёт§17 обновлены. Исходный stderr того события не найден, отличать
вывод по исходникам от найденных логов. Конфигурация/доверие/защита не менялись.

---

Ниже первоначальное состояние запуска (до остановки на покрытии инъекций):

Код зафиксирован: 764d95fc0b33a42fab35305a1b6fe19371c9ef49. Все семь новых run_id и неизменные параметры — plan.json.
Не менять HEAD/src/tests/scripts/config во время серии. S4/S5 → инъекции → S0×2/S1–S3.
Исходные отрицательные серии сохранены; хеши 517+85 файлов проверены (preservation-20261115.json).
Независимых ревью 0/3. Этап4 не начинать.

## Исполнение и оставшиеся проверки

Пайплайн `bash run_series.sh` запущен в exec session **84222**. main/origin/main =
764d95fc0b33a42fab35305a1b6fe19371c9ef49. Активная серия пишет полный `series.log`,
каждый сценарий — `../<run_id>.log`, сводку и cgroup CPU — `../runs/`.
Не запускать вторую серию и не менять исходники/HEAD параллельно.

S4 начат 12:04 UTC. Ранний UI-аудит6/6 операций (2 controlled), средний33/33
(16 controlled), все job sets и CPU совпали. JSON полные; перед публикацией сжать
lossless с хешами. Нужна поздняя выборка до завершения S4 (UI исчезает после stop).
batch_merge-0003 в новом S4 = data_conflict/ValidationException.

Исходный невалидный R540 S4 экспортирован ДО reset в diagnostic-r540-20261115/evidence
(85 файлов/68МБ). Хеши517 старых R600+85 диагностических файлов совпали:
../preservation-20261115.json. Исходные журналы/data/runs не удалялись.

После семи успешных run-result:

- Экспорт scripts/stage3_artifacts.py внутри runner, все7 IDs из plan.json,
  --out этот каталог/evidence; затем make export RUN=каждый ID. Старые каталоги не заменять.
- scripts/stage3_checks.py --runs все7 --determinism S0 S0rep --out здесь/checks.json,
  полный stdout/stderr в checks.log. Пороги1452/1510000, R_low<=540<=R_high.
- verify_evidence.py здесь проверяет все хеши, JSON/Parquet rows, полные CSV,
  p95 без допуска (round_trip parser), исходный pickle, замыкание истории.
- maintenance_summary.py путь-серии; plot_timeline.py; прочитать результаты,
  осмотреть PNG. Добавлен R и вынесена легенда. Сохранить CSV/PDF/источник графика.
- Partition-only матрица: прямой pytest test_conflict_matrix.py, MATRIX_ONLY=ovpartition,
  версии2/3,20overlap/5control/min10 valid; wrapper НЕ вызывать (он заменяет verified).
  results/stage-1/iter-stage3-partition-20261115/v2 и v3, уникальные slot suffix;
  update_matrix.py --no-write. Других Spark-проверок во время серии не запускать.
- Нет общего интервала R → предусмотренное решение автора. Не ослаблять проверки.
- Обновить первичные §3/4/7/8 отчёта, ADR008 измерения, CHANGELOG/handoff,
  ответы S2-R4-01..06, покрытие переноса docs/codex-workflow.md5.claude+4external, ограничения.
- Сжать полные .log и большие UI JSON lossless с SHA256/длинами; файлы<100MB.
  Сохранить отрицательные серии. Коммит отчёта/артефактов; чистое дерево до ревью.
- Нативный stage-reviewer: collaboration.spawn_agent, agent_type=stage-reviewer,
  fork_turns=none, без model override. Навык review уже прочитан; перед запуском объявить.
  Передать SHA,N3,K1,base stage-2, инструкции/критерии/пути. Только новый review,
  no fixes/no commit. Во время ревью HEAD и артефакты не менять. Использовано0/3.
- Прочитать весь результат, коммитить только новый review. Доказательство dispatch/result
  можно сохранить НОВЫМ JSON под docs/reviews/ и отдельным коммитом после review:
  этот путь покрывается review_covers_head. Другие paths после PASS требуют нового раунда.
  CHANGES_REQUESTED: все ID исправить, свежий раунд, максимум3 за весь запуск.
- PASS*: штатный stage-3 тег, push main+tag, local==remote SHA, остановка перед4.

PostCompact — отдельный вопрос пользовательского Caveman. Повтор обработчика
exit0/0.118s, исходный stderr не найден, внутренняя причина не установлена.
CAPL SessionStart/PreToolUse enabled/trusted; реальная блокировка после compaction.
Очищенная диагностика и свежий discovery — results/codex-workflow/postcompact-20261115.
Конфигурация/доверие/защита не менялись. Локальная подпись удалена из публикуемой пробы;
исходный результат только /tmp/capl-postcompact-probe-20261115.private.json.
