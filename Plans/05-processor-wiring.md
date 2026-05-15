# Sub-plano 5: Processor + Wiring

**Objetivo:** orquestração completa receive→validate→persist com política de erro da seção 2.3 do kickoff e graceful shutdown.

## Arquivos a criar

```
internal/
└── processor/
    ├── processor.go
    └── processor_test.go
cmd/
└── processor/
    └── main.go
```

## Tarefas — Processor

### internal/processor/processor.go

**Config:**
```go
// Config holds all dependencies for the Processor.
type Config struct {
    Consumer        messaging.Consumer
    Validator       validation.Validator
    EventStore      storage.EventStore
    QuarantineStore storage.QuarantineStore
    Logger          *slog.Logger
    Workers         int
}
```

**Processor:**
```go
type Processor struct{ cfg Config }

func New(cfg Config) *Processor
```

**Run(ctx context.Context) error:**
1. Cria `jobs := make(chan messaging.Message, cfg.Workers)`
2. Spawna `cfg.Workers` goroutines — cada uma faz `for msg := range jobs { p.handle(ctx, msg) }`
3. Loop principal:
   ```
   for {
       select {
       case <-ctx.Done():
           close(jobs)
           wg.Wait()
           return nil
       default:
           subCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
           msgs, err := p.cfg.Consumer.Receive(subCtx)
           cancel()
           // envia msgs para jobs
       }
   }
   ```
4. Em `ctx.Done()`: fecha `jobs`, `wg.Wait()`

**handle(ctx context.Context, msg messaging.Message):**

Implementa exatamente a tabela de política da seção 2.3:

| Situação | Ação |
|----------|------|
| `ValidationError` retornado | `QuarantineStore.Save` + `Ack` + log warn |
| `ErrDuplicate` do EventStore | log info "duplicate, skipping" + `Ack` |
| Erro transiente do EventStore | log error + **NÃO ack** |
| Sucesso | `Ack` + log info |
| `Ack` falhou | log error (mensagem volta, idempotência absorve) |

Campos de log estruturado: `event_id`, `event_type`, `tenant_id`, `message_id`, `reason` (quando aplicável).

### internal/processor/processor_test.go

**Fakes:**
```go
type fakeConsumer struct {
    messages []messaging.Message
    ackCalls []string // receipt handles
    receiveErr error
    ackErr     error
}

type fakeValidator struct {
    result *domain.Event
    valErr *validation.ValidationError
}

type fakeEventStore struct {
    saveErr error
    saved   []*domain.Event
}

type fakeQuarantineStore struct {
    saveErr    error
    quarantined []*domain.Quarantined
}
```

**8 casos de teste (cada um em função separada):**

1. `TestHandleValidEvent` — válido → EventStore.Save chamado, Ack chamado
2. `TestHandleMalformedEnvelope` — ValidationError(invalid_envelope) → QuarantineStore.Save, Ack chamado, EventStore.Save NÃO chamado
3. `TestHandleInvalidPayload` — ValidationError(invalid_payload) → quarentena + Ack
4. `TestHandleUnknownEventType` — ValidationError(unknown_event_type) → quarentena + Ack
5. `TestHandleMissingTenant` — ValidationError(missing_tenant) → quarentena + Ack
6. `TestHandleDuplicate` — EventStore retorna ErrDuplicate → Ack chamado, QuarantineStore.Save NÃO chamado
7. `TestHandleTransientError` — EventStore retorna erro genérico → **Ack NÃO chamado** (invariante crítica)
8. `TestGracefulShutdown` — cancelar ctx com mensagem in-flight → worker termina a mensagem antes de encerrar

---

## Tarefas — Wiring

### cmd/processor/main.go

```go
func main() {
    // 1. Carregar config
    cfg, err := config.Load()
    if err != nil {
        log.Fatal("failed to load config:", err)
    }

    // 2. Logger JSON
    logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelInfo,
    }))
    slog.SetDefault(logger)

    // 3. Signal context
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer stop()

    // 4. AWS config com endpoint override para LocalStack
    awsOpts := []func(*awsconfig.LoadOptions) error{
        awsconfig.WithRegion(cfg.AWSRegion),
    }
    if cfg.AWSEndpointURL != "" {
        customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: cfg.AWSEndpointURL}, nil
        })
        awsOpts = append(awsOpts, awsconfig.WithEndpointResolverWithOptions(customResolver))
    }
    awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
    // ...

    // 5. Wire dependências
    sqsClient := sqs.NewFromConfig(awsCfg)
    dynamoClient := dynamodb.NewFromConfig(awsCfg)

    consumer := messaging.NewSQSConsumer(sqsClient, cfg.SQSQueueURL)
    validator, err := validation.New("/schemas/payloads")
    eventStore := storage.NewEventStore(dynamoClient, cfg.DynamoDBEventsTable)
    quarantineStore := storage.NewQuarantineStore(dynamoClient, cfg.DynamoDBQuarantineTable)

    proc := processor.New(processor.Config{
        Consumer:        consumer,
        Validator:       validator,
        EventStore:      eventStore,
        QuarantineStore: quarantineStore,
        Logger:          logger,
        Workers:         cfg.ProcessorWorkers,
    })

    // 6. Run com grace period
    logger.Info("processor starting", "workers", cfg.ProcessorWorkers)
    if err := proc.Run(ctx); err != nil {
        logger.Error("processor error", "error", err)
    }

    // Grace period após sinal
    shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
    defer cancel()
    <-shutdownCtx.Done()
    logger.Info("processor stopped")
}
```

**Notas de implementação:**
- Usar `aws.EndpointResolverWithOptionsFunc` ou o endpoint override disponível no SDK v2 (verificar versão exata)
- Schemas path: `/schemas/payloads` no container (montado via compose); em dev local usar `./schemas/payloads`
- Falha catastrófica no startup (validator, config) usa `log.Fatal` ou `os.Exit(1)` — aceitável em `main`

---

## Critério de conclusão
- `go test ./internal/processor/... -v -race` passa todos os 8 casos
- `make up && docker compose logs -f processor` mostra processor iniciando e aguardando mensagens
- Logs são JSON estruturado (verificar com `docker compose logs processor | head -5 | jq .`)

## Próximo: Plans/06-producer.md
