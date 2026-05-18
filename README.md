# Pismo Event Processor

A Go service that consumes CloudEvents from Amazon SQS, validates them against per-type JSON Schema contracts, triages them by routing rules, and persists valid events to DynamoDB with idempotency guarantees. Invalid or unroutable events are quarantined with structured failure reasons. Built as a reference implementation for the Pismo Staff Engineer challenge.

## Architecture

```
Producer ──► SQS Queue ──► Processor ──► DynamoDB (events)
                │                   └──► DynamoDB (quarantined_events)
                └─► DLQ (after 5 failures)
```

**Internal pipeline (per message):**

```
                              Event Processor
                  ┌──────────────────────────────────────────┐
SQS ─► Receive ─► │ Validate ─► Triage ─► Persist ─► Ack     │ ─► DynamoDB (events)
                  │     │           │                         │
                  │     └───────────┴──► QuarantineStore.Save │ ─► DynamoDB (quarantined_events)
                  └──────────────────────────────────────────┘
```

The processor runs N parallel workers (default: 5). Each worker receives a message, validates the CloudEvent envelope and payload, triages it against routing rules, and persists to DynamoDB. Validation failures and triage failures are quarantined and acked; transient storage errors are not acked (SQS redelivers after `VisibilityTimeout=30s`).

## Pipeline Stages

Every message flows through four explicit stages. Each stage has a single responsibility, distinct error semantics, and observable boundaries in the structured logs (`stage` field).

**Receive.** The processor long-polls SQS (20s wait, batch of up to 10) and pulls messages onto an in-process channel. Messages remain invisible to other workers for `VisibilityTimeout=30s` — long enough to complete the rest of the pipeline. If the processor crashes before acknowledging, SQS automatically redelivers.

**Validate.** Two-layer validation: first the CloudEvents v1.0 envelope (required fields, format constraints) using the official SDK; then the typed payload against the JSON Schema registered for that event type. Failures here are *deterministic* — retrying with the same input produces the same result — so failures are quarantined and acknowledged, not redelivered.

**Triage.** This is the stage that transforms event metadata into explicit routing intent. The `RuleBasedTriager` checks the tenant against an allowlist, then matches the event type against ordered rules from `config/routing.yaml`. The result — `target_client`, `category`, `priority` — is attached to the event before persistence. The Sender service downstream reads these fields directly from DynamoDB Streams without re-evaluating rules. Triage failures (unregistered tenant, no matching rule) are quarantined like validation failures.

**Persist.** A single `PutItem` to DynamoDB with `ConditionExpression: attribute_not_exists(id)`. This is the core idempotency mechanism: duplicate messages from at-least-once delivery are rejected at the database level, returning `ConditionalCheckFailedException`, which the processor treats as success (the message is acknowledged without re-saving). Transient errors (throttling, network) are *not* acknowledged — SQS redelivers, and the DLQ catches messages that fail repeatedly.

## Quick Start

```bash
git clone https://github.com/eduardohitek/pismo-event-processor
cd pismo-event-processor

make up          # ~30s — LocalStack + infra (CLI) + processor up
make publish     # publishes 5 valid + 2 invalid events
make inspect     # expect 5 rows in `events`, 2 in `quarantined_events`
```

`make up` builds the processor, provisions LocalStack infrastructure via AWS CLI, and starts all services. `make publish` sends 5 valid + 2 invalid test events. `make inspect` shows the persisted records in DynamoDB.

> **Prerequisites:** Docker and Docker Compose. No AWS account or credentials required — everything runs against LocalStack.

## Commands

| Command | Description |
|---------|-------------|
| `make up` | Build processor, provision LocalStack via AWS CLI, start all services |
| `make down` | Stop all services and remove volumes |
| `make logs` | Tail processor logs (JSON structured) |
| `make publish` | Publish 5 valid + 2 invalid events |
| `make demo-idempotency` | Publish same event 3× — expect exactly 1 record |
| `make demo-mixed` | 50 valid + 10 invalid events, randomly shuffled |
| `make demo-stream` | Continuous 5 msg/s for 30s (visual demo) |
| `make inspect` | Scan `events` table (CLI) |
| `make inspect-quarantine` | Scan `quarantined_events` table (CLI) |
| `make test` | Unit tests with race detector and coverage |
| `make test-integration` | E2E tests against running LocalStack (requires `make up`) |

After `make up`, a DynamoDB Admin UI is available at **http://localhost:8001** — browse tables, inspect items, and run queries without the CLI.

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

### Provisioning: AWS CLI script, not Terraform

The challenge brief notes that "an Infrastructure as Code solution will be welcome." This implementation deliberately uses an AWS CLI script (`scripts/setup.sh`) instead, and documents the trade-off explicitly.

**The friction:** `terraform-provider-aws` has built-in propagation waiters for SQS — after `CreateQueue`, it polls `GetQueueAttributes` every 5 seconds for ~25 seconds per queue to confirm attribute consistency. This delay is correct for real AWS, where SQS attributes propagate asynchronously across the regional control plane. Against LocalStack, where resources are created synchronously in-process, the waiters add no safety — only latency. Two queues plus two tables: ~50s vs ~8s with the CLI. For a reviewer running `make up` once, this is the difference between a smooth first impression and an awkward wait.

**What's being traded:**

- *Lost:* declarative state management, `terraform destroy` for clean teardown, drift detection, and (importantly for the brief's intent) demonstration of IaC fluency in a familiar form.
- *Kept:* deterministic, repeatable provisioning. The script is idempotent (re-runnable without errors), version-controlled, and documents the resources as code — just in bash + AWS CLI rather than HCL.

**When this decision flips:** for any deployment beyond LocalStack — staging, production, or shared developer environments on real AWS — Terraform (or CDK, or CloudFormation) is the correct choice. The trade-off here is scoped to the LocalStack iteration loop. A production-bound version of this service would have `terraform/` as the source of truth.

**Verdict:** in a case evaluated on Reproducibility and Simplicity (two of the six official criteria), faster setup with equivalent declarativeness beats slower setup with theoretical state tracking against a mock backend.

## Resilience Model

| Failure type | Action | Guarantee |
|---|---|---|
| Producer fails before `SendMessage` | Message never enters queue | Caller's responsibility to retry |
| SQS message lost after enqueue | `MessageRetentionPeriod=14d`, standard durability 99.9% | Effectively never lost once enqueued |
| Processor crash mid-processing | `VisibilityTimeout=30s` — SQS redelivers automatically | At-least-once delivery |
| DynamoDB transient error | Message not acked — SQS redelivers; DLQ after 5 failures | No permanent loss within retention window |
| Duplicate delivery | `attribute_not_exists(id)` — returns `ErrDuplicate`, acked without re-saving | Exactly-once persistence despite at-least-once transport |
| Invalid envelope (deterministic) | Quarantined + acked | No retry; failure is permanent |
| Unregistered tenant or no routing rule | Quarantined + acked | No retry; failure is permanent |
| Ack fails after successful save | Message redelivered; duplicate conditional write rejected | Idempotency absorbs the redundant message |

## Schema Versioning

Event types follow the convention `com.<org>.<domain>.<event>.<vN>`:

- `com.pismo.payment.authorized.v1` — current version
- `com.pismo.payment.authorized.v2` — when the contract changes incompatibly

Each version maps to a separate JSON Schema file in `schemas/payloads/`. The processor loads all files at startup. New versions are deployed by adding a schema file without modifying existing ones. Old schema files remain active until all producers have migrated, allowing parallel versions in production.

## Routing Configuration

Triage rules live in `config/routing.yaml` and are loaded at startup. The file defines:

- **`rules`**: ordered list of matchers; first match wins. `match.type` is an exact event type or a glob suffix ending in `.*` (e.g. `com.pismo.monitoring.*` matches any event type starting with `com.pismo.monitoring.`).
- **`default`**: fallback route applied when no rule matches. If absent, events with no matching rule are quarantined with reason `no_routing_rule`.
- **`registered_tenants`**: allowlist of valid tenant IDs. Events from tenants not in this list are quarantined with reason `unregistered_tenant`.

Each matched event is enriched with three DynamoDB attributes: `routing_target` (the tenant ID), `routing_category`, and `routing_priority`. The Sender service reads these fields from DynamoDB Streams to route events to their destinations without re-evaluating rules.

To add support for a new event type or tenant, update `config/routing.yaml` and redeploy. No code change required.

The path to this file is configured via the `ROUTING_CONFIG` env var (default: `/config/routing.yaml`). In Docker Compose the file is mounted from `./config:/config:ro`.

## Project Layout

```
├── cmd/
│   ├── processor/          # Entrypoint: config → AWS clients → processor.Run
│   └── producer/           # Test tool: publishes valid and invalid events to SQS
├── internal/
│   ├── config/             # Env-var loading; all environment config in one place
│   ├── domain/             # Pure types: Event, Routing, Quarantined, QuarantineReason
│   ├── messaging/          # Consumer interface + SQSConsumer (long-poll, ack)
│   ├── validation/         # Validator interface + SchemaValidator (CloudEvents + JSON Schema)
│   ├── triage/             # RuleBasedTriager: loads routing.yaml, routes events by type + tenant
│   ├── storage/            # EventStore + QuarantineStore interfaces + DynamoDB impls
│   └── processor/          # Orchestrates receive→validate→triage→persist; N workers, graceful shutdown
├── test/
│   └── integration/        # E2E tests (build tag: integration); require make up
├── schemas/
│   └── payloads/           # JSON Schema files, one per event type, named by type string
├── config/
│   └── routing.yaml        # Triage rules: type matchers, category/priority, registered tenants
├── scripts/
│   └── setup.sh            # AWS CLI provisioning: SQS + DLQ + 2 DynamoDB tables
├── docs/
│   ├── architecture.md     # Design decisions in depth
│   ├── resilience.md       # Pipeline failure analysis
│   └── sender-design.md    # Proposed Sender service design (out of scope)
├── docker-compose.yml      # LocalStack + setup (AWS CLI) + Processor + DynamoDB Admin UI
├── Dockerfile              # Multi-stage: go:1.26-alpine builder → distroless runtime
└── Makefile                # All operational commands
```

## Future Evolution

**Sender service:** DynamoDB Streams feeds a KCL worker or Lambda trigger that delivers persisted events to downstream consumers. See [docs/sender-design.md](docs/sender-design.md) for the detailed design including idempotency, retry configuration, and catchup handling.

**Schema Registry:** Replace file-based schemas with a central registry (AWS Glue Schema Registry, Confluent) for cross-team schema governance and automated compatibility enforcement.

**Outbox pattern for producer:** In production, the publisher would use an outbox table in its own database to guarantee exactly-once publishing, decoupling event creation from SQS delivery.

**Dynamic routing rules:** The current rules are static YAML loaded at startup — a deliberate choice for the case scope (Simplicity is a stated evaluation criterion). Future evolutions could add hot reload (inotify watch or config server poll) or a rule expression engine for payload-conditional routing (e.g. "route if amount > 1000"). Both add operational complexity not justified by the current requirements.

**Observability:** Structured JSON logs are already in place with `stage` field per pipeline step. Next: Prometheus counters (`events_processed_total`, `events_quarantined_total`), latency histograms, and OpenTelemetry traces for end-to-end pipeline visibility.

**Terraform for production deployment:** The current provisioning uses AWS CLI for LocalStack speed (see *Provisioning* section above). A production deployment would replace `scripts/setup.sh` with a `terraform/` module — same resources, same configuration, but with state management and CI-driven plan/apply. The CLI script's idempotency design means the transition is mechanical, not structural.

## Evaluation Criteria

| Criterion | Where to find it |
|---|---|
| **Problem Understanding** | Correct error routing (transient vs deterministic), at-least-once + idempotent consumer, DLQ after 5 retries, CloudEvents envelope |
| **Maintainability** | Package-per-concern in `internal/`, interfaces defined at the consumer side (Go convention), no circular dependencies, explicit error types |
| **Simplicity** | stdlib-only flags and logging (`log/slog`), no framework, no ORM, compact production codebase (~1k lines, no framework, stdlib + minimal deps) |
| **Testability** | Manual fakes (no mock framework), unit tests covering all error-routing branches in validator, triager, and processor; end-to-end integration tests covering happy path, quarantine paths, and idempotency. Test coverage >70% on `internal/` packages. |
| **Documentation** | This README (10 sections), `docs/architecture.md`, `docs/resilience.md`, `docs/sender-design.md` |
| **Reproducibility** | `make up` — Docker + AWS CLI + LocalStack — full running system from a single command |
