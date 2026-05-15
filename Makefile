.PHONY: up down logs build tidy

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f processor

build:
	go build ./...

tidy:
	go mod tidy

# Alvos implementados nos próximos sub-planos:
# publish, inspect, inspect-quarantine, test, test-integration
# demo-idempotency, demo-mixed, demo-stream
