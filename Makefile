# Команды проекта CAPL. Запускать из корня репозитория.
SHELL := /bin/bash
COMPOSE := docker compose --env-file infra/.env -f infra/docker-compose.yml
RUNNER  := $(COMPOSE) exec -T runner
PY      := python3
RUNNER_PY := python3

SCENARIO ?= S2_compaction_vs_cdc_merge
POLICY   ?= P0
CONC     ?= mid
SEED     ?= 11
SERIES   ?= E3_main
RUN      ?=

.PHONY: help env build up up-lite up-lakekeeper down clean ps logs versions-check smoke \
        validate-config seed run batch export gc matrix-verify \
        test-unit test-int test-e2e lint typecheck \
        dataset train evaluate sim calibrate acceptance figures tables reproduce-smoke

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

env: ## Создать infra/.env из примера (если нет)
	@test -f infra/.env || cp infra/.env.example infra/.env

build: env ## Собрать образы capl, spark, flink
	$(COMPOSE) --profile streaming build

up: env ## Поднять full-профиль (ядро + streaming + observability)
	$(COMPOSE) --profile streaming --profile observability up -d

up-lite: env ## Поднять lite-профиль (без Kafka/Flink)
	$(COMPOSE) --profile observability up -d

up-lakekeeper: env ## Поднять с Lakekeeper в качестве каталога
	CAPL_UPSTREAM_URI=http://lakekeeper:8181/catalog CATALOG_IMPL=lakekeeper \
	$(COMPOSE) --profile streaming --profile observability --profile lakekeeper up -d

down: ## Остановить стенд (данные сохраняются)
	$(COMPOSE) --profile streaming --profile observability --profile lakekeeper down

clean: ## Остановить и удалить тома (ВСЕ данные)
	$(COMPOSE) --profile streaming --profile observability --profile lakekeeper down -v

ps: ## Состояние контейнеров
	$(COMPOSE) ps

logs: ## Логи CAPL
	$(COMPOSE) logs -f --tail=200 capl

versions-check: env ## Проверить доступность всех образов и jar (этап 0)
	bash scripts/versions_check.sh infra/.env

smoke: ## Smoke-тест стенда (этап 0)
	$(RUNNER) python3 -m scripts.smoke

validate-config: ## Валидация YAML сценариев и конфигураций
	PYTHONPATH=src $(PY) -m runner.scenario --validate config/workloads config/experiments/matrix.yaml

seed: ## Создать «золотую» таблицу для SCENARIO
	$(RUNNER) python -m runner.main seed --scenario $(SCENARIO)

run: ## Один прогон: SCENARIO POLICY CONC SEED
	$(RUNNER) python -m runner.main run --scenario $(SCENARIO) --policy $(POLICY) --concurrency $(CONC) --seed $(SEED)

batch: ## Серия из config/experiments/matrix.yaml: SERIES
	$(RUNNER) python -m runner.batch --matrix config/experiments/matrix.yaml --series $(SERIES)

export: ## Экспорт трасс прогона в Parquet: RUN=<run_id> (пусто = все завершённые)
	$(RUNNER) python -m runner.export --run "$(RUN)" --out data/traces

gc: ## Уборка файлов прошедших прогонов
	$(RUNNER) python -m runner.gc

matrix-verify: ## Эмпирическая проверка матрицы конфликтов (этап 1)
	$(RUNNER) pytest -q tests/integration/test_conflict_matrix.py && $(PY) scripts/update_matrix.py

test-unit: ## Unit-тесты (без Docker)
	PYTHONPATH=src pytest -q tests/unit

test-int: ## Integration-тесты (стенд должен быть поднят)
	$(RUNNER) pytest -q tests/integration

test-e2e: ## E2E: короткие прогоны всех сценариев
	$(RUNNER) pytest -q tests/e2e

lint: ## ruff
	ruff check src tests

typecheck: ## mypy
	mypy --strict src/capl

dataset: ## Сборка обучающей выборки (point-in-time)
	PYTHONPATH=src $(PY) -m ml.dataset --pg "$${TELEMETRY_PG_DSN:-postgresql://capl:capl@localhost:5432/telemetry}" --out data/datasets

train: ## Обучение моделей
	PYTHONPATH=src $(PY) -m ml.train --data data/datasets --out models

evaluate: ## Оценка моделей по протоколу 03 §4.4
	PYTHONPATH=src $(PY) -m ml.evaluate --models models/current --data data/datasets

sim: ## Симуляция: SERIES=SIM1_calibration_check | SIM2_production_scale
	PYTHONPATH=src $(PY) -m sim.run --matrix config/experiments/matrix.yaml --series $(SERIES)

calibrate: ## Калибровка симулятора по трассам стенда
	PYTHONPATH=src $(PY) -m sim.calibrate --traces data/traces --out config/simulator.yaml

acceptance: ## Проверка критериев приёмки AC-1..AC-8
	PYTHONPATH=src $(PY) -m analysis.acceptance

figures: ## Все графики ВКР в results/figures
	PYTHONPATH=src $(PY) -m analysis.figures --out results/figures

tables: ## Таблицы для ВКР
	PYTHONPATH=src $(PY) -m analysis.tables --out results/tables

reproduce-smoke: ## Воспроизведение с нуля: стенд, один прогон, один график
	$(MAKE) build up smoke seed run SCENARIO=S2_compaction_vs_cdc_merge POLICY=P6 SEED=11
	$(MAKE) export figures
