SHELL := /bin/bash
PYTHON ?= python3
VENV := .venv/bin
export PRE_COMMIT_HOME := $(CURDIR)/.venv/.cache/pre-commit
TEST_IMAGE := weather-tests:2.9.3-python3.10
TEST_PROJECT ?= weather-tests
TEST_COMPOSE := docker compose --env-file /dev/null -p $(TEST_PROJECT) -f docker-compose.test.yml

.PHONY: help setup hooks lint format precommit config-check build test-build test test-integration check up down

help:
	@echo "setup            Create tooling venv (Python >=3.10)"
	@echo "hooks            Install pre-commit into this Git checkout"
	@echo "lint / format    Check / fix Python style"
	@echo "precommit        Run hooks on all tracked files"
	@echo "config-check     Validate development and test Compose configuration"
	@echo "build            Build runtime Airflow image"
	@echo "test             Build test image and run offline unit/runtime/DAG tests"
	@echo "test-integration Run disposable MinIO/PostgreSQL tests, then clean up"
	@echo "check            Run lint and both test suites"
	@echo "up / down        Start / stop development services (preserve volumes)"

setup:
	$(PYTHON) -c 'import sys; assert sys.version_info >= (3, 10), "Python >=3.10 required"'
	$(PYTHON) -m venv .venv
	$(VENV)/python -m pip install --require-hashes -r requirements-dev.txt

hooks:
	$(VENV)/pre-commit install

lint:
	$(VENV)/ruff check scripts dags tests
	$(VENV)/ruff format --check scripts dags tests

format:
	$(VENV)/ruff check --fix scripts dags tests
	$(VENV)/ruff format scripts dags tests

precommit:
	$(VENV)/pre-commit run --all-files

config-check:
	docker compose config --quiet
	$(TEST_COMPOSE) config --quiet

build:
	docker compose build airflow-webserver

test-build:
	docker build --target test -t $(TEST_IMAGE) .

test: test-build
	docker run --rm --network none $(TEST_IMAGE) -m 'not integration'

test-integration:
	@set -eu; trap 'status=$$?; if [ "$$status" -ne 0 ]; then $(TEST_COMPOSE) logs --no-color; fi; $(TEST_COMPOSE) down --volumes --remove-orphans; exit $$status' EXIT; \
	$(TEST_COMPOSE) up --build --abort-on-container-exit --exit-code-from tests tests

check: lint config-check test test-integration

up:
	docker compose up -d --build

down:
	docker compose down
