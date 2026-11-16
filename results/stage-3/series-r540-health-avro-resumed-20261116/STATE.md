# Возобновление этапа 3 — 16.11.2026

HEAD d76c8ecf9e99caafecdd0d991934cee9d2997b15, main; origin/main совпадает.
S4 сохранён; прерванный S5 диагностический, новый ID указан в plan-amendment.json.
Хеши 239 файлов S4 и 9 DB-экспортов partial S5 перепроверены до reset.
Параметры и пороги неизменны. Формальных ревью 1/3; следующий раунд 2.
Подъём среды: make up, тома сохранены. Далее S5, S4+S5 checks, integration/6faults/restore, S0×2/S1–S3.

## Pipeline запущен16:25UTC

Экспериментальныйexec61566; единственный poller functions cell34.
Ожидать functions.wait34, не опрашивать61566 вторым watcher.
Контроллер конечный continue_series.py, все команды/выходы в pipeline.log/progress.jsonl.
Postprocessing встроен последним шагом, отдельных wait-процессоров нет.
Старые остановленные toolID не использовать. До ревью HEAD и sourcepaths не менять.
S5 resume1 идёт; в16:38UTC26health/maxgap30,549522с.

## S5 завершён16:54UTC

Новый S5: validation=true,18committed/7K2,runner_errors0,incomplete_cost0;209metadata/max86503Б.
Checks S4/S5 exit0; исходный S5 перед инъекциями сохранён в s5-before-injection.json.
Активны integration-регрессии; далее безусловное покрытие6типов/restore, затем S0×2/S1–S3.

## Интеграция завершена17:00UTC

27passed/209deselected/349,43с;2предупреждения pytest об asyncio (синхронные тесты).
Шесть видов дефектов подтверждены (5наS5+retained_deleted_row на отдельной фикстуре).
BaselineS5 неизменён:216attempts/188events/53ops/snapshot4112340258148149364.
Доказательство fault-coverage.json и injection-after.log. Первый полный S0 запущен.
Pipeline61566/poller34 остаются; впереди S0повтор/S1–S3 и встроенныйpost_series.

## ПервыйS0 завершён17:29UTC

validation=true,5invariants,11committed,runner_errors0,incomplete_cost0,
200metadata/max83090Б,1674600freshness events. Начался S0rep(seed1).
Завершены S4/S5/S0(3из7); послеS0rep идутS1–S3. Poller34/pipeline61566.

## S0rep завершён17:59UTC

validation=true,5invariants,11committed,runner_errors0,incomplete_cost0;
начался полный S1. Готовы4из7: S4/S5/S0×2. Источники/HEAD неизменны.
Далее S1/S2/S3, встроенная постобработка, отчёт и свежий review2(использован1/3).

## S1: отказ coverage обнаружен18:03UTC

S1 sample at18:03:00.913501:gap31,094039с >31с; acquisition1,188311с,
REST0,025228с. Начала acquisitions по-прежнему точно раз30с; задержка послеREST.
Порог/jitter не менять. Полный S1 продолжается как диагностический для сохранения
всех исходных данных и финальных инвариантов. Pipeline сам остановится на validation=false;
S2/S3 не начнутся. После завершения — сохранить/export все результаты до GC,
профилировать закреплённые metadata/manifest list и различить IO/декодирование/агрегацию
до исправления. Не повторять тот же ID и не выбирать PASS из повторов без устранения причины.

## Причина и различающие опыты до исправления18:23UTC

Резерв частоты недостаточен при переписывании манифестов: все53пути новые дляcache;
новыйбольшойAvro1,826МБ/19518live entries, CPU-декодирование конкурирует с другим
Python-наблюдателем. Профиль закреплённых состояний:0,766сwall/0,776сCPU.
ПровереныSHA всех заархивированныхAvro/metadata;140манифестов дают равныесчётчики.
Projection-прототип не устанавливается: выигрыш мал (0,852→0,757с по сумме лучших).
Различающий контроль с параллельным decode:8threads1,377…1,519с,2processes0,681…0,763с,
140/140равенства в каждом из12случаев. NativeS3IO в этом контроле не измерялось.
7новыхрегрессий ДОфикса отказали:4изоляция/enum/close,3cleanup(normal/error/SIGINT).
Всеисточники пока d76c8ec, production не менялся. После полногоS1:export+hashes,
исправитьhealth decode стандартным ProcessPoolExecutor(2,spawn), cache/DB/provenance
оставитьвродителе; чётко закрыватьpool вRun.finally/последнемsample.
Затем unitточно пересчитать(ожидается339+7=346), nativev2/v3 черезpool, короткийS4,
новаяполнаясерия7IDs/одинновыйSHA/прежние30с+jitter1с. Старые4valid сохранить,
но из-за изменения диагностическойCPU-нагрузки итоговыйнабор долженбытьсвежим.
Формальныеreviewпо-прежнему1/3. Далееreport/commit/review2/tag/push/stopbefore4.

## Исправление реализовано и проверено18:35UTC

S1 полностью завершён:validation=false только1gap31,094039с;5invariants/0runner_errors/
0incompletecost,13committed,3773metadata/max1329861Б,5363200freshness.
Pipeline61566/poller34 закрытыexit1, S2/S3не запускались.
Экспорт5полныхпрогонов4881файл:всеархивыиledgerпроверены (preservation-verified.json).
Код health/main исправлен: ProcessPoolExecutor(2,spawn) для чистогоdecode;кэш/IO/DB/
provenance у родителя;close послеfinalsample иfinally/stop с явнойошибкойтаймаута.
8новыхрегрессий:до7failed/1passed,после8passed. Полный347unit/lint/mypy14/config21;
nativev2/v3 черезpool2passed/31,90с. Всежурналы lossless.gz, ADR-015/отчёт§25/handoff обновлены.
Далее коммитисправления,короткийS4,егоexport/verify, затемноваяполнаясерия7ID/одинSHA.
Формальныйreview1/3,следующий2. Stage4не начинать. Старыеизмерения сохранять.
