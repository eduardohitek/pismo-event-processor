# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Pismo Event Processor — a Go service that consumes CloudEvents from SQS, validates them against JSON Schema, and persists them to DynamoDB with idempotency guarantees. Built for the Pismo Staff Engineer challenge.

**Module:** `github.com/eduardohitek/pismo-event-processor`  
**Go version:** 1.26+

## Commands

```bash
# Build
make build              # go build ./...
make tidy               # go mod tidy

# Local environment (requires Docker)
make up                 # start LocalStack + provision via AWS CLI + Processor
make down               # stop and remove volumes
make logs               # tail processor logs

# Publish test events
make publish            # 5 valid + 2 invalid events
make demo-idempotency   # same event ID × 3 (expect 1 record)
make demo-mixed         # 50 valid + 10 invalid, interleaved
make demo-stream        # continuous 5 msg/s for 30s

# Inspect DynamoDB
make inspect            # scan events table
make inspect-quarantine # scan quarantined_events table

# Tests
make test               # go test ./... -race -cover  (unit only)
make test-integration   # requires make up; build tag: integration

# Single package test
go test ./internal/validation/... -v -run TestValidate
go test ./internal/processor/... -v -run TestHandleTransientError

# Code quality
go vet ./...
gofmt -l .
```

## Architecture

```
Producer ──► SQS (events queue) ──► Processor ──► DynamoDB (events)
                     │                        └──► DynamoDB (quarantined_events)
                     └──► DLQ (after 5 failures)
```

**Data flow inside the Processor:**

```
SQSConsumer.Receive()
    └──► SchemaValidator.Validate()
              ├── CloudEvents parse + .Validate()
              ├── subject check (tenant_id)
              ├── event type lookup in compiled schemas
              └── JSON Schema validation of data payload
         ├── ValidationError → QuarantineStore.Save() + Ack
         └── *domain.Event → EventStore.Save()
                  ├── ErrDuplicate (ConditionalCheckFailedException) → Ack only
                  ├── Transient error → NO Ack (SQS redelivers)
                  └── Success → Ack
```

## Package structure

| Package | Role |
|---------|------|
| `internal/domain` | Pure types: `Event`, `Quarantined`, `QuarantineReason` constants |
| `internal/config` | `Load() (*Config, error)` — env vars only, no files |
| `internal/validation` | `Validator` interface + `SchemaValidator` (CloudEvents + JSON Schema) |
| `internal/storage` | `EventStore` + `QuarantineStore` interfaces + DynamoDB impls; `ErrDuplicate` sentinel |
| `internal/messaging` | `Consumer` interface + `SQSConsumer`; `Message` type |
| `internal/processor` | Orchestrates receive→validate→persist; N workers + graceful shutdown |
| `cmd/processor` | Wiring only: config → AWS clients → processor.Run(ctx) |
| `cmd/producer` | Test tool: publishes valid/invalid events; supports --scenario and --rate |
| `schemas/payloads/` | JSON Schema files, one per event type, named by type string |
| `scripts/setup.sh` | AWS CLI provisioning: SQS + DLQ + 2 DynamoDB tables (~8s) |
| `test/integration/` | E2E tests (build tag `integration`), assume `make up` already running |

## Key design decisions

**Interfaces defined at the consumer side** (Go convention): `messaging.Consumer`, `storage.EventStore`, `storage.QuarantineStore`, and `validation.Validator` are all defined in the package that uses them, not in the package that implements them.

**Idempotency:** `EventStore.Save` uses DynamoDB `ConditionExpression: attribute_not_exists(id)`. A `ConditionalCheckFailedException` returns `ErrDuplicate` — the processor acks without re-saving. This is the idempotency core.

**Error routing:** transient storage errors must NOT ack the SQS message. This is the most critical invariant — SQS redelivers after VisibilityTimeout (30s), DLQ catches after 5 failures.

**Schema loading:** `SchemaValidator` compiles all `*.json` files from `schemas/payloads/` at startup. Filename without extension = event type key (e.g., `com.pismo.payment.authorized.v1.json` → `com.pismo.payment.authorized.v1`). Startup fails fast if schemas directory is empty or unreadable.

**CloudEvent fields mapping:**
- `id` → idempotency key (DynamoDB PK)
- `subject` → `tenant_id`
- `type` → event type + schema lookup key
- `data` → JSON payload validated by type-specific schema

## Environment variables

| Variable | Required | Default |
|----------|----------|---------|
| `SQS_QUEUE_URL` | yes | — |
| `DYNAMODB_EVENTS_TABLE` | yes | — |
| `DYNAMODB_QUARANTINE_TABLE` | yes | — |
| `AWS_REGION` | no | `us-east-1` |
| `AWS_ENDPOINT_URL` | no | `""` (real AWS when empty) |
| `PROCESSOR_WORKERS` | no | `5` |
| `SHUTDOWN_GRACE_PERIOD` | no | `30s` |

For local development against LocalStack: `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`, `AWS_ENDPOINT_URL=http://localhost:4566`.

## DynamoDB schema

**`events` table:** PK `id` (String, ULID), stream `NEW_IMAGE`, `PAY_PER_REQUEST`. Field `data` stored as String (raw JSON), not as DynamoDB map.

**`quarantined_events` table:** PK `event_id` (String). Fallback: if `event_id` is empty (unparseable envelope), generate a ULID via `ulid.Make()`.

## Test patterns

Unit tests use manual mock structs (no mock frameworks) with function fields:
```go
type mockDynamo struct {
    putItemFn func(ctx, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}
```

Integration tests use `eventuallyAssert(t, predicate func() bool, timeout, msg)` with 100ms polling — required because SQS consumption is async.

Call `clearTables(t)` at the start of each integration test for isolated state.

## Implementation order

See `Plans/` directory for the 7 sub-plans:
1. `Plans/01-foundation.md` — domain types + config
2. `Plans/02-infra.md` — Docker + Terraform + Makefile
3. `Plans/03-validation.md` — CloudEvents + JSON Schema validation
4. `Plans/04-storage-messaging.md` — DynamoDB + SQS clients
5. `Plans/05-processor-wiring.md` — orchestration + cmd/processor
6. `Plans/06-producer.md` — test producer + Makefile completion
7. `Plans/07-tests-docs.md` — integration tests + README + docs/

Each sub-plan must compile and pass tests before the next begins.
