# Sender Service Design

The Sender is the downstream consumer of persisted events. It reads from the DynamoDB Stream on the `events` table and delivers events to external consumers (APIs, other queues, webhooks). This component is **out of scope** for the current implementation, but its design is described here to demonstrate a complete architectural vision.

---

## Trigger Mechanism: DynamoDB Streams

The `events` table has `stream_enabled = true` with `stream_view_type = NEW_IMAGE`. Every `PutItem` that successfully persists a new event emits a stream record containing the full item as written.

This provides:
- **Ordered delivery per partition key**: stream records for the same `id` arrive in write order (in practice, there is only one write per `id` due to idempotency).
- **At-least-once delivery**: stream records are retained for 24 hours. The consumer can replay within this window.
- **Decoupling**: the Sender is developed and deployed independently of the Processor.

---

## Lambda Trigger vs KCL Worker

| Dimension | Lambda Trigger | KCL Worker (on EC2/ECS) |
|---|---|---|
| Ops overhead | Minimal — AWS manages scaling and shard assignment | Requires cluster management and shard lease coordination |
| Cold start | Latency spike on first invocation after idle | Always warm; predictable latency |
| Concurrency control | Managed via `ParallelizationFactor` and reserved concurrency | Manual shard-to-worker mapping |
| Cost | Pay-per-invocation; free tier covers light workloads | Always-on cost |
| Suitable for | Event-driven, variable workload | High-throughput, continuous workload |

**Recommendation for this use case:** Lambda trigger. The event volume is moderate, ops overhead should be minimal, and the `ReportBatchItemFailures` feature (see below) makes partial failure handling straightforward.

---

## Lambda Event Source Mapping Configuration

```hcl
resource "aws_lambda_event_source_mapping" "sender" {
  event_source_arn              = aws_dynamodb_table.events.stream_arn
  function_name                 = aws_lambda_function.sender.arn
  starting_position             = "TRIM_HORIZON"
  batch_size                    = 10
  maximum_retry_attempts        = 3
  bisect_batch_on_function_error = true
  function_response_types        = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.sender_dlq.arn
    }
  }
}
```

Key settings:

| Setting | Value | Rationale |
|---|---|---|
| `starting_position` | `TRIM_HORIZON` | Process all events from the beginning of the stream on first deploy |
| `batch_size` | 10 | Balance throughput vs blast radius on failure |
| `maximum_retry_attempts` | 3 | Transient failures get 3 retries before going to DLQ |
| `bisect_batch_on_function_error` | `true` | On batch failure, splits the batch to isolate the poison message |
| `function_response_types` | `["ReportBatchItemFailures"]` | Lambda can mark individual items as failed, retrying only those |
| `on_failure.destination_arn` | SQS DLQ | Unprocessable events are preserved for inspection |

---

## Idempotency in the Sender

The Sender must be idempotent because Lambda invocations are at-least-once. Options:

1. **Conditional write at destination**: if the downstream system is a database, use a conditional insert keyed on the event `id`.
2. **Idempotency token**: pass the `id` as an idempotency key to the target API (many AWS services support this natively).
3. **Deduplication table**: maintain a DynamoDB table of delivered `id`s with a TTL equal to the SQS `MessageRetentionPeriod`. Check before delivery; write after.

Option 3 is the safest general-purpose approach when the downstream system doesn't natively support idempotency keys.

---

## 24-Hour Stream Window and Catchup

DynamoDB Streams retain records for **24 hours**. If the Sender is offline for more than 24 hours, stream records are lost. Mitigation:

1. **Alarm on `IteratorAge` > threshold**: CloudWatch metric `IteratorAgeMilliseconds` measures how far behind the consumer is. Alert when it exceeds 1 hour.
2. **Catchup from DynamoDB Scan**: if the window is missed, the Sender can scan the `events` table directly (filtering by `received_at`) and replay events. This is a recovery procedure, not normal operation.
3. **Retain `TRIM_HORIZON`**: the event source mapping is configured with `TRIM_HORIZON` so a redeployment starts from the oldest available record.

---

## Reconciliation Against Silent Failure

Silent delivery failures (Sender appeared to succeed but downstream didn't receive) are detected by reconciliation:

1. The Sender writes a `delivered_at` timestamp to the event record (via `UpdateItem`) after confirmed delivery.
2. A periodic reconciliation job scans events where `delivered_at` is absent and `received_at` is older than the expected delivery SLA.
3. Undelivered events are re-queued to the Sender via a dedicated SQS queue.

This provides end-to-end delivery guarantee without requiring the main pipeline to be two-phase.

---

## Sequence Diagram

```
Processor ──PutItem──► DynamoDB (events)
                              │
                              │ DynamoDB Stream (NEW_IMAGE)
                              ▼
                       Lambda (Sender)
                              │
                    ┌─────────┴─────────┐
                    │                   │
               [Success]          [Transient fail]
                    │                   │
              UpdateItem            Lambda retries
              (delivered_at)            │
                                  [Max retries]
                                        │
                                   SQS DLQ
```
