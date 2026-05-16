# Pismo Event Processor

A Go service that consumes CloudEvents from Amazon SQS, validates them against per-type JSON Schema contracts, and persists valid events to DynamoDB with idempotency guarantees. Invalid events are quarantined with structured failure reasons. Built as a reference implementation for the Pismo Staff Engineer challenge.

## Architecture

```
Producer ──► SQS Queue ──► Processor ──► DynamoDB (events)
                │                   └──► DynamoDB (quarantined_events)
                └─► DLQ (after 5 failures)
```

The processor runs N parallel workers (default: 5). Each worker receives a message, validates the CloudEvent envelope and payload, and routes based on the result: valid events are persisted and acked; invalid events are quarantined and acked; transient storage errors are not acked (SQS redelivers after `VisibilityTimeout=30s`).

## Quick Start

```bash
git clone https://github.com/eduardohitek/pismo-event-processor
cd pismo-event-processor
make up && make publish && make inspect
```

`make up` builds the processor, provisions LocalStack infrastructure via Terraform, and starts all services. `make publish` sends 5 valid + 2 invalid test events. `make inspect` shows the persisted records in DynamoDB.

> **Prerequisites:** Docker and Docker Compose. No AWS account or credentials required — everything runs against LocalStack.

## Commands

| Command | Description |
|---------|-------------|
| `make up` | Build processor, apply Terraform (LocalStack), start all services |
| `make down` | Stop all services and remove volumes |
| `make logs` | Tail processor logs (JSON structured) |
| `make publish` | Publish 5 valid + 2 invalid events |
| `make demo-idempotency` | Publish same event 3× — expect exactly 1 record |
| `make demo-mixed` | 50 valid + 10 invalid events, randomly shuffled |
| `make demo-stream` | Continuous 5 msg/s for 30s (visual demo) |
| `make inspect` | Scan `events` table |
| `make inspect-quarantine` | Scan `quarantined_events` table |
| `make test` | Unit tests with race detector and coverage |
| `make test-integration` | E2E tests against running LocalStack (requires `make up`) |

## Why These Choices

### CloudEvents v1.0 vs custom envelope

CloudEvents is a CNCF-graduated specification with official SDKs in 15+ languages. Using it means envelope validation (required fields, spec version, content type) is handled by a maintained library — not hand-rolled code. The `id`, `type`, and `subject` fields map directly to our domain needs: idempotency key, schema routing, and tenant identifier. Custom envelopes introduce undocumented contracts that no external tooling understands.

*Rejected:* raw JSON with custom fields — no interoperability, reinvents a solved problem.

### JSON Schema Draft 2020-12 vs struct-level validation

JSON Schema is a contract, not a Go struct tag. It lives in a versioned file (`schemas/payloads/com.pismo.payment.authorized.v1.json`), can be shared with producers in any language, can evolve independently of Go types, and is the language schema registries speak. `go-playground/validator` validates Go struct fields — useful for internal request models, wrong for inter-service data contracts that cross language boundaries.

*Rejected:* `go-playground/validator` — couples contract definition to Go struct definitions; no cross-language sharing.

### Amazon SQS vs Apache Kafka

SQS is serverless, zero-ops, and provides at-least-once delivery with a DLQ built in. For event ingestion at moderate throughput, SQS is the correct default. Kafka requires cluster management, Zookeeper/KRaft coordination, consumer group rebalancing, and partition key design — complexity that doesn't improve the reliability model for this use case.

*Rejected:* Apache Kafka — disproportionate operational overhead for the problem at hand.

### DynamoDB vs PostgreSQL

DynamoDB is serverless, requires no schema migrations, and natively emits change data capture via Streams — exactly the mechanism the Sender service would consume (see [docs/sender-design.md](docs/sender-design.md)). The ULID primary key (`id`) distributes writes uniformly across partitions. Idempotency is a single conditional write (`attribute_not_exists(id)`) with no external locking.

PostgreSQL would require schema migration tooling, an explicit idempotency mechanism (`INSERT ... ON CONFLICT`), and a CDC layer (Debezium, pglogical) for the Sender. None of that complexity improves reliability for this case.

*Trade-off accepted:* no ordering by tenant. A `PK=tenant_id, SK=event_id` design enables tenant-scoped queries but creates hot partitions for high-volume tenants. Global ordering is not required here.

## Resilience Model

| Failure type | Action | Guarantee |
|---|---|---|
| Producer fails before `SendMessage` | Message never enters queue | Caller's responsibility to retry |
| SQS message lost after enqueue | `MessageRetentionPeriod=14d`, standard durability 99.9% | Effectively never lost once enqueued |
| Processor crash mid-processing | `VisibilityTimeout=30s` — SQS redelivers automatically | At-least-once delivery |
| DynamoDB transient error | Message not acked — SQS redelivers; DLQ after 5 failures | No permanent loss within retention window |
| Duplicate delivery | `attribute_not_exists(id)` — returns `ErrDuplicate`, acked without re-saving | Exactly-once persistence despite at-least-once transport |
| Invalid envelope (deterministic) | Quarantined + acked | No retry; failure is permanent |
| Ack fails after successful save | Message redelivered; duplicate conditional write rejected | Idempotency absorbs the redundant message |

## Schema Versioning

Event types follow the convention `com.<org>.<domain>.<event>.<vN>`:

- `com.pismo.payment.authorized.v1` — current version
- `com.pismo.payment.authorized.v2` — when the contract changes incompatibly

Each version maps to a separate JSON Schema file in `schemas/payloads/`. The processor loads all files at startup. New versions are deployed by adding a schema file without modifying existing ones. Old schema files remain active until all producers have migrated, allowing parallel versions in production.

## Project Layout

```
├── cmd/
│   ├── processor/          # Entrypoint: config → AWS clients → processor.Run
│   └── producer/           # Test tool: publishes valid and invalid events to SQS
├── internal/
│   ├── config/             # Env-var loading; all environment config in one place
│   ├── domain/             # Pure types: Event, Quarantined, QuarantineReason
│   ├── messaging/          # Consumer interface + SQSConsumer (long-poll, ack)
│   ├── validation/         # Validator interface + SchemaValidator (CloudEvents + JSON Schema)
│   ├── storage/            # EventStore + QuarantineStore interfaces + DynamoDB impls
│   └── processor/          # Orchestrates receive→validate→persist; N workers, graceful shutdown
├── test/
│   └── integration/        # E2E tests (build tag: integration); require make up
├── schemas/
│   └── payloads/           # JSON Schema files, one per event type, named by type string
├── terraform/              # SQS + DLQ + 2 DynamoDB tables provisioned against LocalStack
├── docs/
│   ├── architecture.md     # Design decisions in depth
│   ├── resilience.md       # Pipeline failure analysis
│   └── sender-design.md    # Proposed Sender service design (out of scope)
├── docker-compose.yml      # LocalStack + Terraform + Processor
├── Dockerfile              # Multi-stage: go:1.26-alpine builder → distroless runtime
└── Makefile                # All operational commands
```

## Future Evolution

**Sender service:** DynamoDB Streams feeds a KCL worker or Lambda trigger that delivers persisted events to downstream consumers. See [docs/sender-design.md](docs/sender-design.md) for the detailed design including idempotency, retry configuration, and catchup handling.

**Schema Registry:** Replace file-based schemas with a central registry (AWS Glue Schema Registry, Confluent) for cross-team schema governance and automated compatibility enforcement.

**Outbox pattern for producer:** In production, the publisher would use an outbox table in its own database to guarantee exactly-once publishing, decoupling event creation from SQS delivery.

**Observability:** Structured JSON logs are already in place. Next: Prometheus counters (`events_processed_total`, `events_quarantined_total`), latency histograms, and OpenTelemetry traces for end-to-end pipeline visibility.

## Evaluation Criteria

| Criterion | Where to find it |
|---|---|
| **Problem Understanding** | Correct error routing (transient vs deterministic), at-least-once + idempotent consumer, DLQ after 5 retries, CloudEvents envelope |
| **Maintainability** | Package-per-concern in `internal/`, interfaces defined at the consumer side (Go convention), no circular dependencies, explicit error types |
| **Simplicity** | stdlib-only flags and logging (`log/slog`), no framework, no ORM, ~1200 lines of production Go |
| **Testability** | Manual fakes (no mock framework), 8 unit test cases covering all error-routing branches, 5 integration E2E cases |
| **Documentation** | This README (10 sections), `docs/architecture.md`, `docs/resilience.md`, `docs/sender-design.md` |
| **Reproducibility** | `make up` — Docker + Terraform + LocalStack — full running system from a single command |
