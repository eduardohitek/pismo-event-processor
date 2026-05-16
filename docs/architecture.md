# Architecture Decisions

This document expands on the key design decisions in the Pismo Event Processor. Each decision includes the rationale, alternatives considered, and trade-offs accepted.

---

## CloudEvents v1.0

The [CloudEvents](https://cloudevents.io/) specification (CNCF graduated, v1.0) defines a standard envelope for event data. We use it for three reasons:

1. **Required fields** (`id`, `type`, `source`, `specversion`, `datacontenttype`) are validated by the SDK before any application code runs. Malformed envelopes are caught early with structured error messages.
2. **Interoperability**: any producer in any language can emit a CloudEvent and be immediately compatible with this processor.
3. **Semantic mapping**: `id` → idempotency key, `type` → schema routing key, `subject` → tenant identifier. These semantics are part of the spec, not our invention.

The CloudEvents Go SDK (`github.com/cloudevents/sdk-go/v2`) provides `Event.Validate()`, which checks all required fields and format constraints. This replaces hand-rolled envelope validation.

---

## JSON Schema Draft 2020-12

Payload validation uses [JSON Schema](https://json-schema.org/) rather than struct-level validation for the following reasons:

**Contract-first design:** The schema file is the canonical definition of the event payload. It can be shared with producers in Python, Java, or any language without coupling them to our Go types.

**Independent versioning:** `schemas/payloads/com.pismo.payment.authorized.v1.json` is a file that can be reviewed, diffed, and evolved independently of the Go codebase. A new event version (`v2`) is a new file — no code change required.

**Expressiveness:** JSON Schema 2020-12 supports `exclusiveMinimum`, `additionalProperties: false`, `format` keywords, and `$ref` composition. These express domain constraints more precisely than Go struct tags.

**Runtime extensibility:** Adding support for a new event type means dropping a schema file into `schemas/payloads/`. The processor loads all `*.json` files at startup and indexes them by filename (without extension). Zero code changes for new event types.

Schemas are compiled at startup (`jsonschema.NewCompiler().Compile(path)`). Startup fails fast if the schemas directory is empty or any file is invalid — no deferred failures at runtime.

---

## Primary Key Design: ULID

Events are keyed by `id` (the CloudEvent `id` field), which producers are expected to generate as a ULID.

**ULID properties relevant here:**
- **Monotonically increasing** (millisecond precision prefix): DynamoDB writes sort lexicographically within a small time window, reducing write amplification on the B-tree index.
- **Globally unique without coordination**: producers generate IDs independently.
- **Idempotency key**: the DynamoDB conditional write `attribute_not_exists(id)` uses this key to reject duplicate events. If a producer retries after a network timeout (not knowing if the first message was received), the second write is rejected without data corruption.

The quarantine table uses `event_id` as its key, with a fallback to `ulid.Make()` for events whose envelope was unparseable (so the `id` field couldn't be extracted).

---

## DynamoDB vs PostgreSQL

| Dimension | DynamoDB | PostgreSQL |
|---|---|---|
| Ops overhead | Serverless, zero cluster management | Requires DB host, backups, upgrades |
| Schema migration | Not applicable (schema-less) | Required for every structural change |
| Idempotency | `attribute_not_exists(id)` — atomic conditional write | `INSERT ... ON CONFLICT DO NOTHING` — requires unique index |
| Change data capture | Native Streams (NEW\_IMAGE) — no plugin needed | Requires Debezium, pglogical, or WAL tailing |
| Write throughput | Scales automatically (PAY\_PER\_REQUEST) | Manual capacity planning |

The native DynamoDB Streams support is particularly important: it provides the exact trigger mechanism the Sender service would use to read persisted events and deliver them downstream.

**Trade-off accepted:** no secondary indexes. For the current schema (PK = `id`), queries by tenant or time range are not efficient. In production with ordering or filtering requirements, the table would add a GSI with `tenant_id` as partition key and `received_at` as sort key. For this case, that complexity is unnecessary.

---

## At-Least-Once + Idempotent Consumer

"Exactly-once" delivery is impossible across a distributed system with independent failure domains (SQS, DynamoDB, the processor itself). We instead implement:

- **At-least-once delivery**: SQS guarantees the message will be delivered at least once. Duplicates are possible on redelivery.
- **Idempotent consumer**: the conditional write ensures only the first successful write for a given `id` persists. Subsequent writes for the same `id` are rejected with `ConditionalCheckFailedException` and the message is acked without error.

This combination provides exactly-once persistence semantics for the happy path, while handling the full retry/redelivery lifecycle correctly.

---

## Error Routing

The processor handles four distinct outcomes for every message:

| Outcome | DynamoDB write | SQS ack | Log level |
|---|---|---|---|
| Valid event, new | ✅ Events table | ✅ Ack | info |
| Valid event, duplicate | ❌ (rejected by condition) | ✅ Ack | info |
| Invalid event (deterministic) | ✅ Quarantine table | ✅ Ack | warn |
| Storage transient error | ❌ | ❌ No ack | error |

The "no ack on transient error" path is the most critical invariant. SQS redelivers after `VisibilityTimeout=30s`. After 5 failures (`maxReceiveCount=5`), the message moves to the DLQ for manual inspection.

---

## Schema Evolution Strategy

The `type` field in a CloudEvent encodes the event type and version: `com.pismo.payment.authorized.v1`. Adding `v2` is:

1. Create `schemas/payloads/com.pismo.payment.authorized.v2.json`
2. Deploy updated processor (now supports both `v1` and `v2`)
3. Migrate producers to emit `v2`
4. Remove `v1` schema file when no producers remain

This strategy supports zero-downtime schema migrations with no coordination window between producers and the processor.
