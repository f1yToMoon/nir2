# Команды проекта CAPL. Запускать из корня репозитория.
SHELL := /bin/bash
COMPOSE := docker compose --env-file infra/.env -f infra/docker-compose.yml
RUNNER  := $(COMPOSE) exec -T runner
PY      := python3
RUNNER_PY := python3
# Инструменты разработки: из .venv (make venv, Python 3.11 + extras service,dev), иначе из PATH
VENV_BIN := $(if $(wildcard .venv/bin/python),.venv/bin/,)

SCENARIO ?= S2_compaction_vs_cdc_merge
POLICY   ?= P0
CONC     ?= mid
SEED     ?= 11
SERIES   ?= E3_main
RUN      ?=

.PHONY: help venv env build up up-lite up-lakekeeper down clean ps logs versions-check smoke \
        validate-config seed run batch export gc matrix-verify proxy-verify fixtures-capture \
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
	PYTHONPATH=src $(VENV_BIN)python -m runner.scenario --validate config/workloads config/experiments/matrix.yaml

VARIANT  ?=
RUNFLAGS ?=
seed: ## Создать «золотую» таблицу для SCENARIO [VARIANT]
	$(COMPOSE) exec -T -e GIT_SHA=$(GIT_SHA) runner python3 -m runner.main seed --scenario $(SCENARIO) \
	  $(if $(VARIANT),--variant $(VARIANT)) $(SEEDFLAGS)

run: ## Один прогон: SCENARIO POLICY CONC SEED [VARIANT] [RUNFLAGS="--duration 300 ..."]
	$(COMPOSE) exec -T -e GIT_SHA=$(GIT_SHA) runner python3 -m runner.main run --scenario $(SCENARIO) \
	  --policy $(POLICY) --concurrency $(CONC) --seed $(SEED) $(if $(VARIANT),--variant $(VARIANT)) $(RUNFLAGS)

RUN_ID ?=
run-measured: ## Прогон с замером CPU воркеров (cgroup): RUN_ID SCENARIO CONC SEED [RUNFLAGS]
	bash scripts/run_measured.sh $(RUN_ID) $(MAKE) run RUNFLAGS="--run-id $(RUN_ID) $(RUNFLAGS)"

RUNS ?=
DET ?=
stage3-checks: ## [auto]-проверки этапа 3 по прогонам: RUNS="<run_id> ..." DET="<run_a> <run_b>"
	$(RUNNER) python3 scripts/stage3_checks.py --runs $(RUNS) $(if $(DET),--determinism $(DET))

batch: ## Серия из config/experiments/matrix.yaml: SERIES
	$(COMPOSE) exec -T -e GIT_SHA=$(GIT_SHA) runner python3 -m runner.batch --matrix config/experiments/matrix.yaml --series $(SERIES)

export: ## Экспорт трасс прогона в Parquet: RUN=<run_id> (пусто = все завершённые)
	$(RUNNER) python3 -m runner.export --run "$(RUN)" --out data/traces

gc: ## Уборка файлов прошедших прогонов (всё, на что не ссылаются «золотые» версии)
	$(RUNNER) python3 -m runner.gc

# коммит, на котором выполняется прогон; -dirty — есть незакоммиченные изменения
GIT_SHA = $(shell git rev-parse HEAD)$(shell test -z "$$(git status --porcelain)" || echo -dirty)

# NFR-1: по умолчанию бенчмарк проходит полный цикл «пилы» истории ADR-008 (рост до expire_at,
# два expire на путь); BENCH_EXPIRE=false — неограниченный рост истории (поиск границы);
# BENCH_SNAPSHOT_BYTES=0 — снапшоты PyIceberg без добавленных свойств; K — выбор тестов (pytest -k)
BENCH_COMMITS ?= 3000
BENCH_EXPIRE ?= true
BENCH_SNAPSHOT_BYTES ?= 960
proxy-verify: ## Интеграционные проверки прокси (этап 2): NAME=<каталог результатов> [K=<pytest -k>]
	$(COMPOSE) exec -T -e GIT_SHA=$(GIT_SHA) -e RESULTS_NAME=$(or $(NAME),run) \
	  -e STAGE2_BENCH_COMMITS=$(BENCH_COMMITS) -e STAGE2_BENCH_EXPIRE=$(BENCH_EXPIRE) \
	  -e STAGE2_BENCH_SNAPSHOT_BYTES=$(BENCH_SNAPSHOT_BYTES) runner \
	  pytest -q -p no:cacheprovider tests/integration/test_proxy_stage2.py $(if $(K),-k "$(K)")

fixtures-capture: ## Снять реальные тела коммитов в tests/fixtures/commits (этап 2)
	$(COMPOSE) exec -T -e GIT_SHA=$(GIT_SHA) runner python3 -m scripts.capture_commit_fixtures

matrix-verify: ## Эмпирическая проверка матрицы конфликтов (этап 1)
	bash scripts/matrix_verify.sh $(or $(NAME),iter3)

venv: ## Локальное окружение .venv (Python 3.11, extras service + dev) через uv
	uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -e ".[service,dev]"

test-unit: ## Unit-тесты (без Docker)
	PYTHONPATH=src $(VENV_BIN)pytest -q tests/unit

test-int: ## Integration-тесты (стенд должен быть поднят)
	$(RUNNER) pytest -q tests/integration

test-e2e: ## E2E: короткие прогоны всех сценариев
	$(RUNNER) pytest -q tests/e2e

lint: ## ruff
	$(VENV_BIN)ruff check src tests scripts/update_matrix.py scripts/capture_commit_fixtures.py \
	  scripts/overhead_enricher_ablation.py scripts/overhead_localization_blocks.py \
	  scripts/overhead_localization_timestamps.py scripts/grafana_check.py scripts/stage3_checks.py

typecheck: ## mypy
	$(VENV_BIN)mypy --strict src/capl

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
