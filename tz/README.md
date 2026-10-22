# Техническое задание: CAPL — Conflict-Aware Planning Layer

Каталог содержит полное техническое задание на реализацию магистерской работы «Прогнозирование конфликтов оптимистичного контроля параллелизма и конфликт-осведомлённое планирование операций записи и обслуживания таблиц в Data Lakehouse» (см. [../README.md](../README.md)).

Рабочее название реализуемой системы — **CAPL** (Conflict-Aware Planning Layer).

## Порядок чтения

| № | Файл | Содержание |
| --- | --- | --- |
| 1 | [01_TZ.md](01_TZ.md) | Бизнес-требования, функциональные и нефункциональные требования, границы проекта, критерии приёмки |
| 2 | [02_architecture.md](02_architecture.md) | Компоненты, схемы развёртывания и взаимодействия, структура репозитория |
| 3 | [03_conflict_model.md](03_conflict_model.md) | Матрица конфликтов, аналитическая модель, признаки, ML-конвейер, политики |
| 4 | [04_interfaces.md](04_interfaces.md) | Поведение прокси, Advisor API, схема телеметрии, события |
| 5 | [05_workloads_and_experiments.md](05_workloads_and_experiments.md) | Данные, сценарии нагрузки, план экспериментов, метрики, статистика |
| 6 | [06_implementation_plan.md](06_implementation_plan.md) | Этапы реализации: шаги, проверки, критерии готовности, сроки |
| 7 | [07_hardware_and_deployment.md](07_hardware_and_deployment.md) | Ресурсы, развёртывание на одном ПК, версии, профили |
| 8 | [08_validation_and_testing.md](08_validation_and_testing.md) | Тестирование, проверка корректности изоляции, валидация симулятора и модели |

## Файлы конфигурации и спецификации

| Путь | Назначение |
| --- | --- |
| [../infra/docker-compose.yml](../infra/docker-compose.yml) | Стенд: объектное хранилище, каталог, Spark, Kafka, Flink, PostgreSQL, наблюдаемость, CAPL |
| [../infra/.env.example](../infra/.env.example) | Версии компонентов и ресурсы |
| [../infra/](../infra/) | Dockerfile-ы и конфигурации компонентов |
| [../config/conflict_matrix.yaml](../config/conflict_matrix.yaml) | Машиночитаемая матрица конфликтов |
| [../config/capl.yaml](../config/capl.yaml) | Конфигурация прокси, advisor и политик |
| [../config/features.yaml](../config/features.yaml) | Определения признаков модели |
| [../config/workloads/](../config/workloads/) | Сценарии нагрузки S0–S6 |
| [../config/experiments/matrix.yaml](../config/experiments/matrix.yaml) | Матрица экспериментов |
| [../config/simulator.yaml](../config/simulator.yaml) | Параметры симулятора |
| [../api/advisor.openapi.yaml](../api/advisor.openapi.yaml) | OpenAPI Advisor API |
| [../schemas/](../schemas/) | JSON Schema событий |
| [../infra/postgres/init/01_telemetry.sql](../infra/postgres/init/01_telemetry.sql) | Схема БД телеметрии |
| [../Makefile](../Makefile) | Команды запуска, проверок и экспериментов |
| [../pyproject.toml](../pyproject.toml) | Python-зависимости |
| [../AGENTS.md](../AGENTS.md) | Инструкции для нейросетевого ассистента, реализующего ТЗ |

## Ключевые решения в одном абзаце

Объект — таблицы Apache Iceberg 1.12 под REST-каталогом. CAPL состоит из прокси перед REST-каталогом (наблюдает все коммиты, восстанавливает след каждого коммита по манифестам, при необходимости задерживает коммит более дешёвой из двух конфликтующих операций) и advisor-сервиса (принимает намерения управляемых операций и решает: запустить, отложить или сузить область). Риск оценивается аналитической пуассоновской моделью и градиентным бустингом над признаками потока коммитов. Метки исходов собирает исполнитель нагрузки (runner), так как конфликт данных обнаруживается на клиенте Iceberg. Всё запускается в Docker Compose на одном ПК; промышленный масштаб оценивается на дискретно-событийном симуляторе, откалиброванном по стенду и использующем тот же код политик.
