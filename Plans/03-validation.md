# Sub-plano 3: Validation

**Objetivo:** validação em duas camadas (CloudEvents + JSON Schema) totalmente testável sem AWS.

## Arquivos a criar

```
schemas/
└── payloads/
    └── com.pismo.payment.authorized.v1.json
internal/
└── validation/
    ├── validator.go
    └── validator_test.go
```

## Tarefas

### 1. schemas/payloads/com.pismo.payment.authorized.v1.json

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "com.pismo.payment.authorized.v1",
  "type": "object",
  "required": ["transaction_id", "amount", "currency"],
  "properties": {
    "transaction_id": {
      "type": "string",
      "minLength": 1
    },
    "amount": {
      "type": "number",
      "exclusiveMinimum": 0
    },
    "currency": {
      "type": "string",
      "minLength": 3,
      "maxLength": 3
    }
  },
  "additionalProperties": false
}
```

### 2. internal/validation/validator.go

**Tipos:**
```go
// ValidationError carries the quarantine reason and diagnostic detail.
type ValidationError struct {
    Reason domain.QuarantineReason
    Detail string
}

func (e *ValidationError) Error() string {
    return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}
```

**Interface** (definida aqui, consumida pelo processor):
```go
// Validator validates a raw SQS message body and returns a parsed Event.
type Validator interface {
    Validate(raw []byte) (*domain.Event, *ValidationError)
}
```

**SchemaValidator:**
```go
type SchemaValidator struct {
    schemas map[string]*jsonschema.Schema
}

// New loads and compiles all *.json schemas from schemasDir.
// Returns error if schemasDir is unreadable or any schema fails to compile.
func New(schemasDir string) (*SchemaValidator, error)
```

**Lógica de Validate (em ordem):**
1. `cloudevents.NewEventFromJSON(raw)` → falha: `ReasonInvalidEnvelope`
2. `event.Validate()` → falha: `ReasonInvalidEnvelope` + detalhe do erro
3. `event.Subject() == ""` → `ReasonMissingTenant`
4. `schemas[event.Type()]` não existe → `ReasonUnknownEventType`
5. `schema.Validate(event.Data())` → falha: `ReasonInvalidPayload` + detalhe
6. Monta e retorna `*domain.Event`:
   - `ID = event.ID()`
   - `Source = event.Source()`
   - `Type = event.Type()`
   - `TenantID = event.Subject()`
   - `Time = event.Time()`
   - `SpecVersion = event.SpecVersion()`
   - `DataContentType = event.DataContentType()`
   - `DataSchema = event.DataSchema()`
   - `Data = event.Data()`
   - `ReceivedAt = time.Now().UTC()`

**Construtor New:**
- `glob.Glob(schemasDir + "/*.json")` ou `os.ReadDir` para listar arquivos
- Para cada arquivo: `compiler.Compile(path)` indexado por `strings.TrimSuffix(filename, ".json")`
- Retorna erro se nenhum schema encontrado (fail-fast no startup)

### 3. internal/validation/validator_test.go

Estrutura table-driven, schemas criados em `t.TempDir()`:

```go
func TestValidate(t *testing.T) {
    cases := []struct {
        name        string
        input       []byte
        wantEventID string          // non-empty = happy path
        wantReason  domain.QuarantineReason
    }{
        {
            name:        "valid event",
            input:       buildValidCloudEvent("tx-001", 99.90, "BRL"),
            wantEventID: "<non-empty>",
        },
        {
            name:       "malformed json",
            input:      []byte("{not-valid"),
            wantReason: domain.ReasonInvalidEnvelope,
        },
        {
            name:       "missing id",
            input:      buildCloudEventWithoutID(),
            wantReason: domain.ReasonInvalidEnvelope,
        },
        {
            name:       "empty subject (tenant)",
            input:      buildCloudEventWithEmptySubject(),
            wantReason: domain.ReasonMissingTenant,
        },
        {
            name:       "unknown event type",
            input:      buildCloudEventWithType("com.pismo.unknown.v99"),
            wantReason: domain.ReasonUnknownEventType,
        },
        {
            name:       "negative amount",
            input:      buildValidCloudEvent("tx-002", -1, "BRL"),
            wantReason: domain.ReasonInvalidPayload,
        },
        {
            name:       "extra field (additionalProperties)",
            input:      buildCloudEventWithExtraField(),
            wantReason: domain.ReasonInvalidPayload,
        },
    }
    // ...
}
```

Helpers de construção criam CloudEvents usando `cloudevents.NewEvent()` para serializar
corretamente — não construir JSON manualmente.

## Critério de conclusão
- `go test ./internal/validation/... -v -race` passa todos os casos
- Cobertura >80% no package `validation`
- `go build ./...` verde

## Próximo: Plans/04-storage-messaging.md
