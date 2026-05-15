# Sub-plano 6: Producer

**Objetivo:** ferramenta de teste que publica eventos válidos e inválidos no SQS para exercitar todos os caminhos do processor.

## Arquivos a criar / modificar

```
cmd/
└── producer/
    └── main.go
Makefile                    (completar alvos pendentes)
```

## Tarefas

### 1. cmd/producer/main.go — Estrutura geral

**Flags** (stdlib `flag`, sem cobra/viper):
```
--count    int     Número de eventos válidos (default: 5)
--invalid  int     Número de eventos inválidos (default: 2)
--scenario string  Cenário nomeado: idempotency | mixed-load | duplicate-burst
--rate     int     Mensagens por segundo (modo contínuo)
--duration string  Duração do modo contínuo, ex: "30s" (default: "30s")
--error-rate float Fração de inválidos no modo rate (default: 0.1)
```

`--scenario` é mutuamente exclusivo com `--count`/`--invalid`. Detectar conflito e imprimir erro + usage.

**Tenants fixos:**
```go
var tenants = []string{"tenant-001", "tenant-002", "tenant-003"}
```

**Merchants/amounts:** aleatórios usando `math/rand` seeded com time.

### 2. Construção de CloudEvent válido

```go
func buildValidEvent(id string, tenantID string) cloudevents.Event {
    e := cloudevents.NewEvent()
    e.SetID(id)
    e.SetType("com.pismo.payment.authorized.v1")
    e.SetSource("producer")
    e.SetSubject(tenantID)
    e.SetDataContentType("application/json")
    e.SetData("application/json", map[string]interface{}{
        "transaction_id": ulid.Make().String(),
        "amount":         randomAmount(), // 1.00 a 9999.99
        "currency":       "BRL",
    })
    return e
}
```

### 3. Modos de evento inválido (4 tipos, rotacionados por índice % 4)

| Índice % 4 | Tipo | Descrição |
|------------|------|-----------|
| 0 | `invalid_json` | Body não é JSON: `[]byte("{not-valid-json}")` |
| 1 | `missing_id` | CloudEvent sem SetID |
| 2 | `unknown_type` | `e.SetType("com.pismo.unknown.v99")` |
| 3 | `invalid_payload` | `amount: -1` (viola exclusiveMinimum) |

Para tipos 1-3: serializar via `json.Marshal` da struct CloudEvent.
Para tipo 0: enviar raw bytes diretamente.

### 4. Publicação no SQS

```go
func publish(ctx context.Context, client *sqs.Client, queueURL string, body []byte) error {
    _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
        QueueUrl:    aws.String(queueURL),
        MessageBody: aws.String(string(body)),
    })
    return err
}
```

Serializar CloudEvent: `json.Marshal(event)` — produz JSON compatível com CloudEvents.

### 5. Cenários --scenario

**`idempotency`:**
```go
// Mesmo ULID, 3 publicações → processor deve salvar apenas 1
fixedID := ulid.Make().String()
for i := 0; i < 3; i++ {
    publish(ctx, client, queueURL, buildValidEvent(fixedID, tenants[0]))
}
fmt.Printf("Published 3x event ID=%s. Expect 1 record in events table.\n", fixedID)
```

**`mixed-load`:**
```go
// 50 válidos + 10 inválidos, embaralhados
msgs := make([][]byte, 0, 60)
for i := 0; i < 50; i++ { msgs = append(msgs, buildValidEventBytes(...)) }
for i := 0; i < 10; i++ { msgs = append(msgs, buildInvalidEventBytes(i)) }
rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
// publicar em sequência
```

**`duplicate-burst`:**
```go
// 100 mensagens, 10 IDs únicos (10 publicações cada)
ids := make([]string, 10)
for i := range ids { ids[i] = ulid.Make().String() }
msgs := make([]string, 100)
for i := range msgs { msgs[i] = ids[i%10] }
rand.Shuffle(len(msgs), ...)
// publicar usando cada ID como event ID
```

### 6. Modo --rate (demo visual)

```go
if *rate > 0 {
    dur, _ := time.ParseDuration(*duration)
    ticker := time.NewTicker(time.Second / time.Duration(*rate))
    deadline := time.Now().Add(dur)
    var sent int
    for time.Now().Before(deadline) {
        <-ticker.C
        if rand.Float64() < *errorRate {
            publish(ctx, client, queueURL, buildInvalidEventBytes(sent))
        } else {
            publish(ctx, client, queueURL, buildValidEventBytes(sent))
        }
        sent++
        fmt.Printf("\rSent: %d", sent)
    }
    ticker.Stop()
    fmt.Printf("\nDone. Sent %d messages at %d msg/s for %s.\n", sent, *rate, *duration)
}
```

### 7. --help com lista de cenários

Customizar `flag.Usage`:
```go
flag.Usage = func() {
    fmt.Fprintf(os.Stderr, "Usage: producer [flags]\n\nFlags:\n")
    flag.PrintDefaults()
    fmt.Fprintf(os.Stderr, `
Scenarios (--scenario):
  idempotency     Sends the same event 3x to verify deduplication (expect 1 record)
  mixed-load      50 valid + 10 invalid events, randomly interleaved
  duplicate-burst 100 messages across 10 unique IDs (stress-tests idempotency)
`)
}
```

### 8. Completar Makefile

Adicionar/completar alvos:

```makefile
QUEUE_URL := http://localhost:4566/000000000000/events

publish:
    go run ./cmd/producer --count 5 --invalid 2

demo-idempotency:
    go run ./cmd/producer --scenario idempotency
    @echo "Sleeping 5s for processor to consume..."
    @sleep 5
    @$(MAKE) inspect

demo-mixed:
    go run ./cmd/producer --scenario mixed-load
    @echo "Sleeping 10s for processor to consume..."
    @sleep 10
    @$(MAKE) inspect
    @$(MAKE) inspect-quarantine

demo-stream:
    go run ./cmd/producer --rate 5 --duration 30s

inspect:
    aws --endpoint-url=http://localhost:4566 dynamodb scan \
        --table-name events \
        --output table \
        --region us-east-1

inspect-quarantine:
    aws --endpoint-url=http://localhost:4566 dynamodb scan \
        --table-name quarantined_events \
        --output table \
        --region us-east-1

test:
    go test ./... -race -cover

test-integration:
    go test -tags=integration -timeout 60s -v ./test/integration/...
```

**Variáveis de ambiente necessárias para producer local:**
```makefile
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export SQS_QUEUE_URL=http://localhost:4566/000000000000/events
```

Adicionar ao início do Makefile ou como target `env:`.

---

## Critério de conclusão

1. `make publish` executa sem erros e imprime confirmação de envio
2. `make inspect` mostra 5 registros em `events`
3. `make inspect-quarantine` mostra 2 registros em `quarantined_events`
4. `make demo-idempotency` resulta em exatamente 1 registro (mesmo com 3 publicações)
5. `make demo-stream` imprime contador de mensagens enviadas por 30s
6. `--help` lista os 3 cenários com descrição

## Próximo: Plans/07-tests-docs.md
