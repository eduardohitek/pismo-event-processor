# Sub-plano 4: Storage + Messaging

**Objetivo:** camada de persistência (DynamoDB) e consumo (SQS) com interfaces limpas e testáveis via mocks.

## Arquivos a criar

```
internal/
├── storage/
│   ├── dynamo.go
│   └── dynamo_test.go
└── messaging/
    ├── consumer.go
    └── consumer_test.go
```

## Tarefas — Storage

### internal/storage/dynamo.go

**Sentinel error:**
```go
// ErrDuplicate is returned when an event with the same ID already exists.
var ErrDuplicate = errors.New("duplicate event")
```

**Interfaces** (consumidas pelo processor):
```go
// EventStore persists valid events.
type EventStore interface {
    Save(ctx context.Context, event *domain.Event) error
}

// QuarantineStore persists rejected events.
type QuarantineStore interface {
    Save(ctx context.Context, q *domain.Quarantined) error
}
```

**DynamoDBClient interface** (para testabilidade — só os métodos usados):
```go
type DynamoDBClient interface {
    PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}
```

**EventDynamo** implementa `EventStore`:
- Construtor: `NewEventStore(client DynamoDBClient, tableName string) *EventDynamo`
- `Save`: `attributevalue.MarshalMap(event)`, override do campo `data` para `types.AttributeValueMemberS{Value: string(event.Data)}`, `PutItem` com:
  ```
  ConditionExpression: aws.String("attribute_not_exists(id)")
  ```
- Detecção de duplicata: `errors.As(err, new(*types.ConditionalCheckFailedException))` → retorna `ErrDuplicate`
- Demais erros → retorna wrapped error

**QuarantineDynamo** implementa `QuarantineStore`:
- Construtor: `NewQuarantineStore(client DynamoDBClient, tableName string) *QuarantineDynamo`
- `Save`: se `q.EventID == ""`, gera `ulid.Make().String()` como fallback
- `attributevalue.MarshalMap(q)` + `PutItem` sem condition (quarentena sobrescreve)

### internal/storage/dynamo_test.go

Usa mock manual do `DynamoDBClient`:

```go
type mockDynamo struct {
    putItemFn func(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

func (m *mockDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
    return m.putItemFn(ctx, in, opts...)
}
```

**Casos:**
1. `Save` evento válido → `PutItem` chamado com `ConditionExpression` correto
2. `Save` duplicata (mock retorna `&types.ConditionalCheckFailedException{}`) → retorna `ErrDuplicate`
3. `Save` erro transiente (mock retorna erro genérico) → retorna erro wrapped
4. `Save` quarentena sem `EventID` → ULID gerado (verificar que `event_id` não vazio no input do PutItem)
5. `Save` quarentena com `EventID` → usa o ID fornecido

---

## Tarefas — Messaging

### internal/messaging/consumer.go

**Message:**
```go
// Message is a provider-neutral representation of a queue message.
type Message struct {
    ID            string
    Body          string
    ReceiptHandle string
    ReceiveCount  int
}
```

**Interface Consumer** (consumida pelo processor):
```go
// Consumer reads messages from a queue and acknowledges processed ones.
type Consumer interface {
    Receive(ctx context.Context) ([]Message, error)
    Ack(ctx context.Context, msg Message) error
}
```

**SQSClient interface** (para testabilidade):
```go
type SQSClient interface {
    ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
    DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}
```

**SQSConsumer** implementa `Consumer`:
- Construtor: `NewSQSConsumer(client SQSClient, queueURL string) *SQSConsumer`
- `Receive`: `ReceiveMessage` com `WaitTimeSeconds=20`, `MaxNumberOfMessages=10`, `AttributeNames=["ApproximateReceiveCount"]`
  - Mapeia `sqsMsg.ApproximateReceiveCount` → `Message.ReceiveCount`
- `Ack`: `DeleteMessage` com `ReceiptHandle`

### internal/messaging/consumer_test.go

Usa mock manual do `SQSClient`.

**Casos:**
1. `Receive` retorna 2 mensagens → ambas mapeadas corretamente (`ID`, `Body`, `ReceiptHandle`, `ReceiveCount`)
2. `Receive` retorna lista vazia → retorna slice vazio sem erro
3. `Ack` chama `DeleteMessage` com o `ReceiptHandle` correto
4. `Receive` com context já cancelado → retorna erro de contexto
5. `Receive` com erro do SQS → retorna erro wrapped

---

## Critério de conclusão
- `go test ./internal/storage/... ./internal/messaging/... -v -race` passa todos os casos
- `go build ./...` verde
- `go vet ./...` zero saída

## Próximo: Plans/05-processor-wiring.md
