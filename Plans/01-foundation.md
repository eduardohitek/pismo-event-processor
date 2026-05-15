# Sub-plano 1: Foundation

**Objetivo:** esqueleto compilável do projeto com tipos de domínio e carregamento de config.

## Arquivos a criar

```
go.mod
go.sum                          (gerado por go mod tidy)
.gitignore
internal/
├── domain/
│   └── event.go
└── config/
    └── config.go
```

## Tarefas

### 1. go.mod
```
module github.com/eduardohitek/pismo-event-processor
go 1.22
```

### 2. Dependências (go get)
```
github.com/aws/aws-sdk-go-v2
github.com/aws/aws-sdk-go-v2/config
github.com/aws/aws-sdk-go-v2/service/sqs
github.com/aws/aws-sdk-go-v2/service/dynamodb
github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue
github.com/cloudevents/sdk-go/v2
github.com/santhosh-tekuri/jsonschema/v5
github.com/oklog/ulid/v2
github.com/stretchr/testify
```

### 3. internal/domain/event.go
- `Event` struct:
  - `ID string`
  - `Source string`
  - `Type string`
  - `TenantID string`
  - `Time time.Time`
  - `SpecVersion string`
  - `DataContentType string`
  - `DataSchema string`
  - `Data json.RawMessage`
  - `ReceivedAt time.Time`
- `Quarantined` struct:
  - `EventID string`
  - `Reason QuarantineReason`
  - `Detail string`
  - `RawMessage []byte`
  - `QuarantinedAt time.Time`
- `QuarantineReason` string type + constantes:
  - `ReasonInvalidEnvelope QuarantineReason = "invalid_envelope"`
  - `ReasonUnknownEventType QuarantineReason = "unknown_event_type"`
  - `ReasonInvalidPayload QuarantineReason = "invalid_payload"`
  - `ReasonMissingTenant QuarantineReason = "missing_tenant"`

### 4. internal/config/config.go
`func Load() (*Config, error)` via `os.Getenv`:

| Env Var | Obrigatório | Default |
|---------|-------------|---------|
| `SQS_QUEUE_URL` | sim | — |
| `DYNAMODB_EVENTS_TABLE` | sim | — |
| `DYNAMODB_QUARANTINE_TABLE` | sim | — |
| `AWS_REGION` | não | `us-east-1` |
| `AWS_ENDPOINT_URL` | não | `""` |
| `PROCESSOR_WORKERS` | não | `5` |
| `SHUTDOWN_GRACE_PERIOD` | não | `30s` |

Retorna `error` se qualquer campo obrigatório estiver vazio.

### 5. .gitignore
```
# Go
*.exe
*.test
dist/
vendor/

# AWS / Terraform
*.tfstate
*.tfstate.backup
.terraform/
.terraform.lock.hcl

# Env
.env
*.env.local

# Editor
.DS_Store
.idea/
.vscode/
```

### 6. go mod tidy
Executar `go mod tidy` para limpar e gerar go.sum.

## Critério de conclusão
- `go build ./...` retorna zero
- `go vet ./...` retorna zero
- Nenhum `TODO` ou `FIXME` no código

## Próximo: Plans/02-infra.md
