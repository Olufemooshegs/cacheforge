.PHONY: up down migrate seed api test test-unit test-integration lint format verify

VENV := .venv/bin

up:
	docker compose up -d

down:
	docker compose down

migrate:
	$(VENV)/alembic upgrade head

seed:
	$(VENV)/python -m demo.seed

api:
	$(VENV)/python -m uvicorn demo.api:app --reload --port 8000

test:
	$(VENV)/python -m pytest

test-unit:
	$(VENV)/python -m pytest tests/unit -q

test-integration:
	$(VENV)/python -m pytest tests/integration -q

verify:
	@echo "GET health:" && curl -s localhost:8000/health && echo
	@echo "GET readiness:" && curl -s localhost:8000/health/ready && echo
	@echo "GET product 1:" && curl -s localhost:8000/products/1 && echo
	@echo "GET product 1 (should be a hit):" && curl -s localhost:8000/products/1 && echo
	@echo "Cache counters:" && curl -sL localhost:8000/metrics/ | grep 'cache_requests_total{operation='

lint:
	$(VENV)/ruff check .
	$(VENV)/ruff format --check .

format:
	$(VENV)/ruff format .