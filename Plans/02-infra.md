# Sub-plano 2: Infra

**Objetivo:** ambiente local completo funcional antes de qualquer lógica de negócio.

## Arquivos a criar

```
Dockerfile
.dockerignore
docker-compose.yml
Makefile
terraform/
├── main.tf
├── variables.tf
└── outputs.tf
scripts/
└── bootstrap.sh
```

## Tarefas

### 1. Dockerfile (multi-stage)

**Stage 1** — `golang:1.22-alpine`:
```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/processor ./cmd/processor
RUN go build -o /out/producer ./cmd/producer
```

**Stage 2** — `gcr.io/distroless/static-debian12:nonroot`:
```dockerfile
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/processor /processor
COPY --from=builder /out/producer /producer
COPY schemas/ /schemas/
```

Sem ENTRYPOINT fixo — `command:` no compose seleciona o binário.

### 2. docker-compose.yml

**Serviço `localstack`:**
- Image: `localstack/localstack:latest`
- Porta: `4566:4566`
- Env: `SERVICES=sqs,dynamodb,dynamodbstreams`
- Healthcheck: `curl -s http://localhost:4566/_localstack/health | grep '"sqs": "available"'`
- Interval: 5s, timeout: 5s, retries: 10

**Serviço `terraform`:**
- Image: `hashicorp/terraform:latest`
- Working dir: `/terraform`
- Volume: `./terraform:/terraform:ro`
- Command: `sh -c "terraform init && terraform apply -auto-approve"`
- Env: `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`, `AWS_DEFAULT_REGION=us-east-1`
- `depends_on: localstack: condition: service_healthy`
- `restart: on-failure`

**Serviço `processor`:**
- Build: `.` (Dockerfile local)
- Command: `["/processor"]`
- `depends_on: terraform: condition: service_completed_successfully`
- Volumes: `./schemas:/schemas:ro`
- Env:
  - `SQS_QUEUE_URL=http://localstack:4566/000000000000/events`
  - `DYNAMODB_EVENTS_TABLE=events`
  - `DYNAMODB_QUARANTINE_TABLE=quarantined_events`
  - `AWS_REGION=us-east-1`
  - `AWS_ENDPOINT_URL=http://localstack:4566`
  - `AWS_ACCESS_KEY_ID=test`
  - `AWS_SECRET_ACCESS_KEY=test`
  - `PROCESSOR_WORKERS=5`
  - `SHUTDOWN_GRACE_PERIOD=30s`
- `restart: on-failure`

### 3. terraform/main.tf

Provider:
```hcl
provider "aws" {
  region                      = var.region
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  endpoints {
    sqs             = "http://localstack:4566"
    dynamodb        = "http://localstack:4566"
    dynamodbstreams = "http://localstack:4566"
  }
}
```

Recursos:
- `aws_sqs_queue.events_dlq` — nome: `events-dlq`
- `aws_sqs_queue.events`:
  - Nome: `events`
  - `visibility_timeout_seconds = 30`
  - `message_retention_seconds = 1209600` (14 dias)
  - `receive_wait_time_seconds = 20`
  - `redrive_policy`: `maxReceiveCount=5`, `deadLetterTargetArn=events_dlq.arn`
- `aws_dynamodb_table.events`:
  - Nome: `events`, PK: `id` (String)
  - `stream_enabled = true`, `stream_view_type = "NEW_IMAGE"`
  - `billing_mode = "PAY_PER_REQUEST"`
- `aws_dynamodb_table.quarantined_events`:
  - Nome: `quarantined_events`, PK: `event_id` (String)
  - `billing_mode = "PAY_PER_REQUEST"`

### 4. terraform/variables.tf
```hcl
variable "region" {
  default = "us-east-1"
}
```

### 5. terraform/outputs.tf
```hcl
output "queue_url"              { value = aws_sqs_queue.events.url }
output "dlq_url"                { value = aws_sqs_queue.events_dlq.url }
output "events_table_name"      { value = aws_dynamodb_table.events.name }
output "quarantine_table_name"  { value = aws_dynamodb_table.quarantined_events.name }
```

### 6. scripts/bootstrap.sh
```bash
#!/bin/bash
set -e
docker compose run --rm terraform sh -c "terraform init && terraform apply -auto-approve"
```

### 7. .dockerignore
```
.git/
.terraform/
*.tfstate
*.tfstate.backup
Plans/
docs/
*.md
```

### 8. Makefile (alvos básicos)
```makefile
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
```

## Critério de conclusão
- `make up` executa sem erros
- `docker compose ps` mostra `localstack` healthy e `terraform` exited(0)
- `docker compose logs terraform` mostra `Apply complete!`
- Processor pode falhar (binário não existe ainda) — esperado

## Próximo: Plans/03-validation.md
