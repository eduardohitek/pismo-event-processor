.PHONY: up down logs build tidy publish inspect inspect-quarantine test test-integration \
        demo-idempotency demo-mixed demo-stream

# LocalStack credentials and endpoint for local development
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
export SQS_QUEUE_URL=http://localhost:4566/000000000000/events

# ── Infrastructure ──────────────────────────────────────────────────────────────

up:
	docker compose up -d --build
	@echo ""
	@echo "Stack status:"
	@docker compose ps localstack processor

down:
	docker compose down -v

logs:
	docker compose logs -f processor

# ── Build ────────────────────────────────────────────────────────────────────────

build:
	go build ./...

tidy:
	go mod tidy

# ── Tests ────────────────────────────────────────────────────────────────────────

test:
	go test ./... -race -cover

test-integration:
	go test -tags=integration -timeout 60s -v ./test/integration/...

# ── Publish ──────────────────────────────────────────────────────────────────────

publish:
	go run ./cmd/producer --count 5 --invalid 2

demo-idempotency:
	go run ./cmd/producer --scenario idempotency
	@echo "Sleeping 5s for processor to consume..."
	@sleep 5
	@$(MAKE) inspect

demo-mixed:
	go run ./cmd/producer --scenario mixed-load
	@echo "Sleeping 10s for processor to consume..."
	@sleep 10
	@$(MAKE) inspect
	@$(MAKE) inspect-quarantine

demo-stream:
	go run ./cmd/producer --rate 5 --duration 30s

# ── Inspect ──────────────────────────────────────────────────────────────────────

inspect:
	aws --endpoint-url=http://localhost:4566 dynamodb scan \
	    --table-name events \
	    --output table \
	    --region us-east-1

inspect-quarantine:
	aws --endpoint-url=http://localhost:4566 dynamodb scan \
	    --table-name quarantined_events \
	    --output table \
	    --region us-east-1
