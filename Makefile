## Lab 17 — Data Pipeline Engineering (Track 2).
## Windows / no make: run the python commands shown next to each target (see README).

VENV ?= .venv
PY   := $(VENV)/bin/python
DBT  := $(abspath $(VENV))/bin/dbt
DAY  ?= 2026-08-12
export DO_NOT_TRACK := 1

.DEFAULT_GOAL := help

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\n"} \
	      /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

setup: ## Create .venv + install the lite-path deps
	python3 -m venv $(VENV) && $(PY) -m pip -q install -r requirements.txt

setup-dbt: ## Add dbt-core + dbt-duckdb to .venv (dbt track)
	$(PY) -m pip -q install -r requirements-dbt.txt

run: ## Fresh build: reset Silver/Gold, backfill 08-10..08-16 from Bronze   (python main.py)
	@$(PY) main.py

day: ## One daily run on the existing warehouse, e.g. make day DAY=2026-08-14
	@$(PY) main.py --date $(DAY)

lateness: ## Measure event lateness (P50/P95/P99) from Bronze   (python main.py --lateness)
	@$(PY) main.py --lateness

rerun3: ## GRADING TEST: fresh build, re-run 2026-08-12 three times, compare checksums
	@$(PY) -m scripts.rerun_check

verify: ## All pipeline contracts (18 checks) — a fresh clone FAILS: that is the lab
	@$(PY) -m scripts.verify

test: ## pytest (unit tests + table contracts + extensions)
	@$(PY) -m pytest

dbt: ## dbt track: land Bronze, dbt build (merge + microbatch + unit test)
	@$(PY) main.py --land-only > /dev/null
	cd dbt_project && DBT_PROFILES_DIR=. "$(DBT)" build --event-time-start 2026-08-10 --event-time-end 2026-08-17

parity: ## dbt track: same Bronze in -> same checksum out (lite vs dbt)
	@$(PY) -m scripts.parity

bonus-llm: ## Bonus: LLM labelling step with hash cache + validation
	@$(PY) -m scripts.bonus_llm

flywheel: ## Extension (ungraded): agent traces -> eval set + DPO pairs
	@$(PY) -m extensions.flywheel

kg: ## Extension (ungraded): knowledge graph vs vector retrieval
	@$(PY) -m extensions.kg_demo

docker-up: ## Bonus: the same daily run on real Airflow 3
	docker compose -f docker/docker-compose.yml up

clean: ## Remove venv and everything the pipeline built (data/ is the source of truth)
	rm -rf $(VENV) lake warehouse.duckdb warehouse.duckdb.wal datasets .pytest_cache \
	       dbt_project/dbt.duckdb dbt_project/target dbt_project/logs
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

.PHONY: help setup setup-dbt run day lateness rerun3 verify test dbt parity bonus-llm flywheel kg docker-up clean
