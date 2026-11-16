# Этап3: исправление после полного S1, 16.11.2026

Актуальны отчёт§25, ADR-015 и results/stage-3/series-r540-health-avro-resumed-20261116/STATE.md.
Исходнаяпауза снята поручением автора. HEAD покаd76c8ec; новый код/результаты готовятсяккоммиту.
ПолныеS4/S5/S0×2 валидны;S1 невалиден:1gap31,094039с>31с,5invariants=true,0runner_errors.
Все5экспортированы:4881файл/всеSHAиledgerпроверены. S2/S3 незапускались;pipeline61566/poller34 завершеныexit1.
Прототипprojection неустановлен. CPU-конкуренцияразличена140Avroобъектами;
исправлениеProcessPoolExecutor(2,spawn),cleanupвоfinally.347unit/lint/mypy14/config21,
nativev2/v3 2passed/31,90с;новые8контролей прошли после7failed/1passed дофикса.
Следующее:коммитисправления/короткийS4/новаяполнаясерия7IDиз-заизменениядиагностическойCPU-нагрузки.
Период30с/jitter1с/R540/пороги не менять. Старые результаты сохранять.
Формальныхreview1/3,следующий2;независимыйустановленныйstage-reviewerпоfrozenSHA,послеPASStag/push/remoteSHA/stopbefore4.
Ниже — сохранённая история; её команды не применять поверх текущего состояния.

# Возобновление этапа 3 — 16.11.2026

Работа возобновлена по поручению автора. HEAD main/origin: `d76c8ecf9e99caafecdd0d991934cee9d2997b15`.
Новая точка: `results/stage-3/series-r540-health-avro-resumed-20261116/STATE.md`; исходная пауза ниже сохранена.
S4 из предыдущего плана сохранён; новый S5: `stage3-S5-P0-mid-s11-r540-health-avro-20261116-resume1`.
План и дополнение лежат в новом каталоге серии, исходный plan.json не менялся.
Контроллер `continue_series.py`: полный S5 → checks S4/S5 → integration/6faults/restore → S0×2/S1–S3 → postprocessing.
Он конечный, при отказе останавливается; исходники и HEAD во время измерений не менять.
Проверки после подъёма: 339unit, lint, mypy14, 21config; активных приложений до старта0, снимок паузы совпал.
Ревью использовано1/3, следующий2; этап4 не начинать. Полные журналы — pipeline.log и ../<run_id>.log.

# Передача этапа 3: пауза 16.11.2026

**Работа остановлена по прямому запросу автора.** Актуально: отчёт §23,
results/stage-3/series-r540-health-avro-20261116/STATE.md и PAUSED.json.
HEAD/remote main: d76c8ecf9e99caafecdd0d991934cee9d2997b15; результаты и документы
паузы локально незакоммичены, сохранить. Источники после SHA не менялись.

Новый полный S4 валиден: 5 инвариантов,62 samples,0 missed,maxgap30,845521с,
151metadata/max62415Б;10committed/25K2. Экспорт239файлов/всеSHA проверены.
Новый S5 прерван SIGINT:failed/user_pause, финальных JSON/ledger нет. Partial:
31ops/134attempts/117events/2642partitions/8640health/36meta,127metadata.
Снимок3336585217761892171. Все сохраняемые файлы/SQL/intents/тома/топикKafka
оставлены. Flink отменён,Spark apps0,pipeline/postprocessor остановлены.

Продолжать только после нового поручения: сохранить валидный S4; выбрать новый
run_id S5, дополнить исходный план и адаптировать контроллер/STAGE3_RUN. Старые
run_series.sh/wait_postprocess.py без изменений не запускать. Затем S4+newS5
checks → integration/6faults/restore → S0×2/S1–S3 → полные exports/checks/plot →
отчёт/коммит → свежий stage-reviewer раунда2. Использован1/3, последний вердикт
CHANGES_REQUESTED. Stage3 не принят, stage4 не начинать. Ниже — история.

**Актуально16.11МСК — отчёт§22.** ПолныйS4на9775b53 не прошёл coverage:
59samples/3gaps>31/max31,182191с,5invariants/0runner_errors.230файлов сохранены
доreset/всеSHAпроверены. Код89e8dd0:fastavroдекодирует один полный read объекта;
холодные счётчики4/4равны,0,368…0,463с. Отрицательный прямойreader сохранён.
339unit/lint/mypy14/config21;nativev2/v3 2passed/29,17с. Короткий новыйS4 завершён:
validation=true/5invariants/7samples/0missed/maxgap30,207867с,
11metadata/max19168Б;31файл экспорта, времена/архивы перепроверены;
следующая полная серия — results/stage-3/series-r540-health-avro-20261116/.
Ни пороги,ниjitter,нинаучныепараметры не менялись. Раундов1/3; следующий2.
Ниже сохраняется история промежуточных вариантов, не итоговая квалификация.

**Актуально после полного контроля health:** отчёт§21. Полный S4 на e0c01c1
не прошёл 1с-jitter:6gaps,max32,548526с;5invariants=true,runner_errors0.
234файла/всеSHA сохранены доreset;148metadata дали62433Б против sampled60435Б.
Исправление3f135180216ce831c01818b4140502f8fb3c5b6e: начало расписания от старта
acquisition, кэш только агрегатов immutable manifests, свежие metadata/list.
336unit/lint/mypy14/config21; nativev2/v3 2passed/30,97с с append/removal/no-reread.
Короткий новыйS4 завершён:validation=true,5invariants,7samples/0missed,
maxgap30,431921с,11metadata/max19169Б;31файл экспорта, времена/архивы перепроверены.
Подготовка новой полной серии:
results/stage-3/series-r540-health-cache-20261115/PREPARATION.md.
Допуск/пороги не повышались. Формальных раундов1/3; следующий2. Ниже — история.

**Актуально после независимого раунда1:** CHANGES_REQUESTED,3 MAJOR/2 MINOR,
reviewed e0038188bcf61102d723a1326041563824b8972f; ревью сохранено d4efe911.
Три MAJOR относятся к health: будущие metadata с прошлым at, пропуски30с,
неполный предел bytes между samples. Исправления ADR-014 и новые регрессии:
334 unit, native2passed v2/v3 (+повтор с reread2passed), lint/mypy14/config21.
Нужна новая полная серия R540 со свежими run_id, всеми исходными файлами, экспортом
и графиком; затем формальный раунд2 (использован1/3). Старую серию не перезаписывать.
Актуальные ответы по ID и статус — отчёт§20. Ниже сохранён прежний измерительный
срез: его числа подтверждены, но телеметрия здоровья и предел bytes не приняты.

Ветка main. Семь новых полных прогонов `-r540-sessions-20261115` завершены:
validation=true/5 инвариантов каждый; общие проверки94/94, детерминизм S0:
11/11 intents и11/11 разрешённых следов. Экспорт517 файлов и22 262 600 событий:
хеши/времена/JSON-Parquet counts/замыкание истории проверены, p95 совпал точно.
Матрица partition-only v2/v3:4 пары,80/80 overlap K2,20/20 controls K1,
невалидных0; verified не перезаписывался. График S2 создан и осмотрен.

Код измерений S4/S5:764d95fc0b33a42fab35305a1b6fe19371c9ef49;
S0×2/S1–S3:1f506a610ac9bafa27311f8ae75c86557e94de83. Между ними только новый
безусловный тест retained_deleted_row/документы/доказательства: diff src/scripts/
config/infra/api/schemas/Makefile/pyproject.toml пуст. Исходный plan.json сохранён,
фактические SHA дополнены plan-amendment.json.

R540 прошёл самопроверку: общий интервал377,3…595,3с, dense peak S1=1327≤1452;
observed metadata1 270 464≤1 510 000Б. Отрицательная R600 серия (dense1459) и
невалидный R540 S4 (2 Runtime file filtering) сохранены отдельно. Хеши602
старых/диагностических файлов перепроверены. Никаких порогов/ожиданий не снижали.

Оговорки обязательны: S5 все4 DELETE дали K2, поэтому выполнены5 видов инъекций
на S5+1 на отдельной реальной двухстрочной фикстуре; первоначальный probe6 отказал,
отказ сохранён. Измеренный S5 восстановлен. S1 Flink восстановился1 раз после
5HTTP409/CommitFailedException; все события наблюдены, потерь/дубликатов не найдено.
S4 health gap max158,434401с при номинале30с. Пики снимков считаются плотным журналом,
bytes только observed. Batch Spark-агенты не объявляются Structured Streaming;
ограничения профиля и фактического размера данных — ADR009§3. Метрика долга остаётся
открытым вопросом перед этапом4; H1–H4 не объявлены доказанными.

Перенос5 файлов.claude+4 внешних покрыт таблицами отчёта§18 и codex-workflow.md.
6/6 native hooks runtime за08.11 сохранены;15.11 оба CAPL project hook enabled/trusted,
после compaction PreToolUse реально блокировал защищённую запись. PostCompact —
несовместимый stdout Caveman: запрещённый hookSpecificOutput в схемах0.167/0.169;
схемы и проверка сохранены, настройки/доверие/механизм не менялись.

Основной отчёт: docs/stage-3-report.md; измерения:
results/stage-3/series-r540-sessions-20261115/ (checks/verification/summary-table,
evidence/manifest, все CSV/JSON, график, native logs). Native reviewer smoke —
results/codex-workflow/. Независимый вердикт и Reviewed-Commit задаются только
новым docs/reviews/stage-3-r*.md. После PASS* — штатный stage-3, push и проверка
удалённого SHA; остановка перед этапом4 для проверки автором в ChatGPT Web.
Решения автора и существующие ревью не редактировать. Не более3 формальных раундов.

Исторический handoff до итогового среза сохранён отдельно в каталоге серии.
