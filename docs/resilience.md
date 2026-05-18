# Resilience Model

This document maps every stage of the pipeline to its failure modes, mitigations, and residual risk.

---

## Pipeline Overview

```
[Producer]
    │
    │ SQS SendMessage
    ▼
[SQS Queue]  ─────────────────────────────────► [DLQ] (after maxReceiveCount=5)
    │
    │ ReceiveMessage (long-poll 20s, up to 10 msgs)
    ▼
[Processor Worker]
    │
    ├─► SchemaValidator.Validate()
    │       ├── CloudEvents parse + .Validate()
    │       ├── subject check (tenant_id)
    │       ├── event type lookup in compiled schemas
    │       └── JSON Schema validation of data payload
    │
    ├─ ValidationError ──► QuarantineStore.Save() + Ack
    │
    ├─► RuleBasedTriager.Route()
    │       ├── tenant allowlist check (registered_tenants in routing.yaml)
    │       └── rule match by type (exact or glob *.*)
    │
    ├─ TriageError ──► QuarantineStore.Save() + Ack
    │
    └─ *domain.Routing ──► EventStore.Save()
            ├── ErrDuplicate (ConditionalCheckFailedException) ──► Ack only
            ├── Transient error ──► NO Ack (SQS redelivers)
            └── Success ──► Ack
```

---

## Stage-by-Stage Failure Analysis

### 1. Producer → SQS (SendMessage)

| Failure | Mitigation | Residual risk |
|---|---|---|
| Producer crashes before calling `SendMessage` | Message never enters queue | **Not mitigated** — producer must implement retry or outbox pattern |
| Network error on `SendMessage` | AWS SDK retries with exponential backoff | Transient: recovered automatically |
| SQS endpoint unavailable | SDK retries; producer logs error | Short outage: messages buffered at producer if retry implemented |
| Duplicate `SendMessage` (producer retried after success) | `id` field in CloudEvent is idempotency key; processor deduplicates | Duplicate enters queue; processor acks second occurrence without saving |

> **Retry policy note:** the producer relies on AWS SDK v2's default retry mode (`standard`: up to 3 attempts with exponential backoff for retryable errors). For production deployment, this would be tuned per-environment via `RetryMaxAttempts` and `RetryMode` config options, with environment-specific values for staging vs production.

### 2. SQS Queue

| Failure | Mitigation | Residual risk |
|---|---|---|
| Message lost in SQS | Standard queue durability: 99.9%+ across multiple AZs | Practically never; not mitigatable without FIFO queue |
| Message retention exceeded | `MessageRetentionPeriod=14d` | Messages older than 14 days are silently dropped — alert on queue depth |
| VisibilityTimeout expires during processing | SQS redelivers automatically after 30s | Processor sees message again; idempotency handles re-save |

### 3. Processor: Receive

| Failure | Mitigation | Residual risk |
|---|---|---|
| Processor crashes during `ReceiveMessage` | Message becomes visible again after `VisibilityTimeout=30s` | At-least-once redelivery — handled by idempotency |
| Long-poll timeout (20s) | Normal operation; returns empty list, loop continues | None |
| Context cancelled mid-receive | `subCtx` (25s timeout derived from parent) expires; loop checks `ctx.Done()` | None; graceful shutdown proceeds |

### 4. Processor: Validation

| Failure | Mitigation | Residual risk |
|---|---|---|
| Malformed JSON envelope | `QuarantineStore.Save()` + Ack | Permanent; visible in `quarantined_events` |
| Missing required CloudEvent fields | Same | Same |
| Unknown event type (no schema) | Same; `reason=unknown_event_type` | Same |
| Missing tenant (`subject` empty) | Same; `reason=missing_tenant` | Same |
| Payload fails JSON Schema | Same; `reason=invalid_payload` | Same |

All validation failures are **deterministic** — retrying will produce the same result. Acking after quarantine is correct; SQS redelivery would loop indefinitely without it.

### 5. Processor: Triage

| Failure | Mitigation | Residual risk |
|---|---|---|
| Tenant not in `registered_tenants` | `QuarantineStore.Save()` + Ack; `reason=unregistered_tenant` | Permanent; fix: add tenant to `config/routing.yaml` and redeploy |
| No rule matches event type (and no default) | Same; `reason=no_routing_rule` | Permanent; fix: add rule or default route to `config/routing.yaml` |
| Routing config file missing at startup | Process exits (fail-fast) | Service will not start — mount `./config:/config:ro` volume |

All triage failures are **deterministic** — retrying will produce the same result. Acking after quarantine is correct.

### 6. Processor: Storage (EventStore)

| Failure | Mitigation | Residual risk |
|---|---|---|
| `ConditionalCheckFailedException` (duplicate) | Log + Ack; no write | None — idempotency working as designed |
| Transient DynamoDB error (throttling, network) | **No Ack** — SQS redelivers after 30s | Up to 5 redeliveries; message goes to DLQ if all fail |
| DynamoDB table deleted | Transient error path; messages pile up in DLQ | Operator intervention required |

### 7. Processor: Ack (DeleteMessage)

| Failure | Mitigation | Residual risk |
|---|---|---|
| `DeleteMessage` fails after successful save | Message redelivered; duplicate `PutItem` rejected by condition | Ack logged as error; no data loss |
| `DeleteMessage` fails after quarantine save | Same message redelivered; processor validates again, quarantines again (idempotent since `quarantined_events` overwrites on same PK) | No data loss |

### 8. Graceful Shutdown (SIGTERM)

| Failure | Mitigation | Residual risk |
|---|---|---|
| SIGTERM received while worker processes a message | `ctx.Done()` triggers `close(jobs)`; `wg.Wait()` blocks until worker finishes | Worker completes current message before exiting |
| SIGTERM received while `ReceiveMessage` is blocked | `subCtx` (derived from parent `ctx`) is cancelled; receive returns immediately | Loop exits cleanly on next `select` iteration |
| Force kill (SIGKILL) | Message becomes visible again after `VisibilityTimeout=30s` | At-least-once redelivery; idempotency absorbs duplicate |

---

## DLQ and Dead Letters

Messages that fail processing 5 times (`maxReceiveCount=5`) are moved to the DLQ (`events-dlq`). This covers:

- Transient DynamoDB errors that outlast 5 retry windows (5 × 30s = 2.5 minutes minimum)
- Programming bugs that cause panics or non-validation errors for specific message shapes
- Infrastructure failures that outlast the retry window

DLQ messages should be alarmed on (CloudWatch metric: `ApproximateNumberOfMessagesVisible > 0`) and inspected manually. They can be replayed once the root cause is fixed.

---

## Operational Metrics for "No Event Lost"

To verify pipeline health operationally:

| Metric | Source | Meaning |
|---|---|---|
| `events` table item count | DynamoDB / CloudWatch | Total valid events persisted |
| `quarantined_events` count | DynamoDB | Events rejected deterministically |
| DLQ depth | SQS CloudWatch | Events that exhausted retries |
| SQS queue depth | SQS CloudWatch | Backlog — processor falling behind |
| `receive_count` histogram | Processor logs | Distribution of redelivery counts |

**Health invariant:** `published_count = events_count + quarantined_count + dlq_count`. A mismatch indicates either a loss or a counting gap in measurement.
