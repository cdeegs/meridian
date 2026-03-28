.PHONY: up down dev test

up:
	docker compose up -d

down:
	docker compose down

dev:
	uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000

test:
	pytest backend/tests/ -v
