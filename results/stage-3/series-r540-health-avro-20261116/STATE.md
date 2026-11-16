# Квалификация R540: чтение манифестов с буфером

Код d76c8ecf9e99caafecdd0d991934cee9d2997b15; план7 новыхrun_id в plan.json. Параметры неизменны:
P0/mid/R540/180сwarmup/1500сmeasurement/120сcooldown/30сhealth/jitter1с.
339unit/lint/mypy14/config21; nativev2/v3 2passed/29,17с.
Короткий протокол на89e8dd0:validation=true/5invariants/7samples/0missed,
maxgap30,207867с,11metadata/max19168Б;31файл, времена/архивы перепроверены.
Измерительный код между89e8dd0 и текущимSHA не менялся.

Старые S4 -health-r1 и -health-cache отвергнуты, полностью сохранены:
234+230файлов/всеSHA перепроверены ДОreset. Непринятая исходнаясемипрогонная
серия и более ранние отрицательные серии также не удаляются.

Далее: публикация/remoteSHA; S4/S5 → строгиеchecks → integration/6видов/
неизменностьbaselineS5 → S0×2/S1–S3. HEAD/src/config во время серии не менять.
После7: post_series.sh, полныйexport/Parquet/verify, график осмотреть,
обновить отчёт/CHANGELOG/handoff, чистыйкоммит, свежийstage-reviewerраунда2.
Использован1/3 формальныхраундов. Не ослаблятьпроверки; этап4не начинать.

## Полная серия запущена21:41UTC15.11 (16.11МСК)

HEAD/remote:d76c8ecf9e99caafecdd0d991934cee9d2997b15.
Pipelineexec10291, liveUIsamplerexec92131. Источники/HEADне менять до конца7.
АктивенS4; далееS5, checks-S4-S5, полнаяintegration/6видов/неизменныйbaselineS5,
затемS0,S0rep,S1,S2,S3. Контроллерrun_series.sh не обходитневалидные результаты.
Одинwatcherдолженpoll10291; егоcellID записан ниже. Полныеlogseries.log и
../<run_id>.log. Послеуспехаpipelineзапуститьpost_series.sh, затемотчёт/ревью2.
UI92131закрытьпослеокончания3сверок;остальныепроверкине запускатьнапараллельномSpark.

Watcherfunctions377 единоличноpoll10291. Ждатьfunctions.wait377;
не опрашивать10291параллельно. Лимитформальныхревьюиспользован1/3.

## Автоматическая обработка после всей серии

Фоновыйwait_postprocess.py:exec60367. Старыйтолькоожидающий45242остановлен
дляисправлениячтениянезавершённыхфайлов; эксперимент10291/UI92131не трогались.
Обработчикждёт7validation=true+7cpu.exit0 и6видовинъекций+checks-S4-S5,
затемединождызапускаетpost_series.sh. Не запускатьpost_series.shпараллельно!
Состояния:postprocessing-events.jsonl,postprocessing-started.json,
postprocessing-result.json; полныйлогpostprocessing.log. Приготовитchecks,
portableJSON/CSV/metadata,Parquet,verification,таблицы,график. При любойошибке
сохраняетотказ иостанавливается. Независимоеreview/коммит/tag/push остаютсяCLI.
HEADпо-прежнемуd76c8ec, формальныхраундов1/3; следующий2. ТекущийS4:
40наблюдений,maxgap30,600775с,пропусковпока0. Источники/HEADне менятьдо7.

## S4 завершён, активен S5

ПервыйполныйS4наd76c8ec локальноvalid:true/5invariants/62samples/
0missed/maxgap30,845521с,151metadata/верхняя62415Б,runner_errors0,
incomplete_cost0;10committed/25K2,3512400наблюденийсвежести. Этоещёне
общаяприёмка:послеS5контроллерchecks-S4-S5пересчитаетсырьевыеданные.
S5запущен22:12UTC15.11,исполняется. Pipeline10291/watch377/UI92131;
postwaiter60367ждётвсе7. HEAD/sourceпостоянны,формальныхreview1/3.

## ПАУЗА по прямому запросу автора16.11МСК

Актуальная точка:PAUSED.json. HEAD/remote d76c8ecf9e99caafecdd0d991934cee9d2997b15.
Pipeline10291/watch377 завершеныexit2 послеSIGINTвS5. Postwaiter60367 остановлен;
UI92131 завершён. РаботающихSpark-приложений иFlнk-заданий0. Контейнеры, тома,
данные иKafkaтопик сохранены; автоматического продолжения нет.

S4 завершён:validation=true/5invariants/62samples/0missed/max30,845521с,
151metadata/max62415Б. Portableexport239файлов/всеSHA проверены:
paused-20261116/qualified-S4-evidence/. S4сохранять и не перезапускать.

S5 прерван пользователем:status=failed, финальногоJSON/ledger.pkl нет.
ПоздниеPy4J/отменаSpark послеSIGINT относятся к остановке. Partialexport:
31ops/134attempts/117events/2642partitions/8640health/36meta,
127metadata-версий,current_snapshot3336585217761892171. СохраненыSQL/intents/
полныйrunnerlog иKafkaтопик. Каталогиpaused-20261116/partial-S5/ и
partial-S5-journals/. Полнаясвежесть ифинальныеинварианты S5не завершены;
этотS5 не включать в итоговые измерения.

Продолжение:новыйrun_id S5, исходныйplan не менять, создатьamendment и
адаптироватьконтроллер/STAGE3_RUN. Старыеrun_series.sh/wait_postprocess.py
безизменений НЕ запускать:они включаютпрерванныйID. Затемchecks S4+newS5,
integration/6видов/restore, S0×2/S1–S3, полныеэкспорты/проверки/график/отчёт,
чистыйкоммит, свежийreview2. Использован1/3, stage3не принят,stage-3нет,
этап4не начинать. Новые результатыи документы локальнонезакоммичены.
