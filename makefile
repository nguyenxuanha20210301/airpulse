# common commands for development and for running the system, see readme.md.

PYTHON ?= python3.11
VENV ?= .venv
BIN := $(VENV)/bin
UV := $(shell command -v uv 2>/dev/null)
PROFILE ?= dev
SCRIPTS := deploy/scripts

# tool caches stay inside the repo so nothing is written outside it.
export UV_CACHE_DIR ?= $(CURDIR)/.cache/uv
export PIP_CACHE_DIR ?= $(CURDIR)/.cache/pip

.PHONY: help setup lint format test check clean cluster-up cluster-down deploy port-forward demo

help:
	@echo "setup         create the virtualenv and install the package with dev tools"
	@echo "format        format python code with ruff"
	@echo "lint          run ruff checks without changing files"
	@echo "test          run unit tests"
	@echo "check         format check, lint and unit tests, run before every commit"
	@echo "clean         remove caches and build output, keeps the virtualenv"
	@echo "cluster-up    create the local kubernetes cluster, PROFILE=dev by default"
	@echo "cluster-down  delete the local kubernetes cluster"
	@echo "deploy        install or upgrade every component on the cluster"
	@echo "port-forward  expose dashboards and apis on localhost"
	@echo "demo          run the end to end demo scenario"

setup:
ifdef UV
	test -d $(VENV) || uv venv $(VENV) --python $(PYTHON)
	uv pip install --python $(BIN)/python -e ".[dev]"
else
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"
endif

format:
	$(BIN)/ruff format src tests
	$(BIN)/ruff check --fix src tests

lint:
	$(BIN)/ruff format --check src tests
	$(BIN)/ruff check src tests

test:
	$(BIN)/pytest

check: lint test

clean:
	rm -rf build dist .pytest_cache .ruff_cache .coverage htmlcov
	find src tests -name __pycache__ -type d -prune -exec rm -rf {} +

cluster-up:
	sh $(SCRIPTS)/cluster_up.sh $(PROFILE)

cluster-down:
	sh $(SCRIPTS)/cluster_down.sh $(PROFILE)

deploy:
	sh $(SCRIPTS)/deploy.sh $(PROFILE)

port-forward:
	sh $(SCRIPTS)/port_forward.sh $(PROFILE)

demo:
	sh $(SCRIPTS)/demo.sh $(PROFILE)
