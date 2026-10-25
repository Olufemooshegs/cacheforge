.PHONY: up down seed api test test-unit lint format

up:
	docker compose up -d

down:
	docker compose down

seed:
	python -m demo.seed

api:
	uvicorn demo.api:app --reload --port 8000

test:
	pytest

test-unit:
	pytest tests/unit

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .