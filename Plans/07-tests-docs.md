# Sub-plano 7: Tests + Docs

**Objetivo:** cobertura de integração e2e completa + documentação profissional para o avaliador da Pismo.

## Arquivos a criar

```
test/
└── integration/
    └── e2e_test.go
README.md
docs/
├── architecture.md
├── resilience.md
└── sender-design.md
```

## Tarefas — Testes de Integração

### test/integration/e2e_test.go

**Build tag obrigatório** (primeira linha após package):
```go
//go:build integration

package integration
```

**Pré-requisito:** `make up` já executado. Testes leem env vars do ambiente ou usam defaults para LocalStack local.

**TestMain:**
```go
func TestMain(m *testing.M) {
    // Valida que o ambiente está pronto
    if os.Getenv("SQS_QUEUE_URL") == "" {
        // usar default de LocalStack local
        os.Setenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/events")
    }
    // setup AWS clients para SQS e DynamoDB
    // Rodar tests
    os.Exit(m.Run())
}
```

**Helper eventuallyAssert:**
```go
func eventuallyAssert(t *testing.T, predicate func() bool, timeout time.Duration, msg string) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if predicate() {
            return
        }
        time.Sleep(100 * time.Millisecond)
    }
    t.Fatalf("condition not met within %s: %s", timeout, msg)
}
```

**Helper clearTables:**
```go
func clearTables(t *testing.T) {
    t.Helper()
    // Scan + BatchWriteItem delete em events e quarantined_events
    // Chamado no início de cada teste para garantir estado limpo
}
```

**Helper publishEvent:**
```go
func publishEvent(t *testing.T, body []byte) {
    t.Helper()
    // sqs.SendMessage para a fila configurada
}
```

**Helper countRecords:**
```go
func countRecords(t *testing.T, table string) int {
    t.Helper()
    // dynamodb.Scan e retorna Count
}
```

### Os 5 casos de teste

**Caso 1: Happy Path**
```go
func TestHappyPath(t *testing.T) {
    clearTables(t)
    event := buildValidCloudEvent(ulid.Make().String(), "tenant-001")
    publishEvent(t, marshal(event))
    
    eventuallyAssert(t, func() bool {
        return countRecords(t, "events") == 1
    }, 10*time.Second, "event should appear in events table")
    
    assert.Equal(t, 0, countRecords(t, "quarantined_events"))
}
```

**Caso 2: Quarentena**
```go
func TestQuarantine(t *testing.T) {
    clearTables(t)
    publishEvent(t, []byte("{not-valid-json}"))
    
    eventuallyAssert(t, func() bool {
        return countRecords(t, "quarantined_events") == 1
    }, 10*time.Second, "invalid event should appear in quarantined_events")
    
    // Verificar reason=invalid_envelope via GetItem
    item := getQuarantinedItem(t)
    assert.Equal(t, "invalid_envelope", item["reason"])
    assert.Equal(t, 0, countRecords(t, "events"))
}
```

**Caso 3: Idempotência**
```go
func TestIdempotency(t *testing.T) {
    clearTables(t)
    fixedID := ulid.Make().String()
    event := buildValidCloudEvent(fixedID, "tenant-001")
    body := marshal(event)
    
    publishEvent(t, body)
    publishEvent(t, body)
    publishEvent(t, body)
    
    // Aguarda processamento das 3 mensagens
    time.Sleep(2 * time.Second)
    
    eventuallyAssert(t, func() bool {
        return countRecords(t, "events") == 1
    }, 15*time.Second, "exactly 1 record despite 3 publishes")
}
```

**Caso 4: Multi-tenancy**
```go
func TestMultiTenancy(t *testing.T) {
    clearTables(t)
    tenants := []string{"tenant-001", "tenant-002", "tenant-003"}
    for _, tenant := range tenants {
        publishEvent(t, marshal(buildValidCloudEvent(ulid.Make().String(), tenant)))
    }
    
    eventuallyAssert(t, func() bool {
        return countRecords(t, "events") == 3
    }, 15*time.Second, "all 3 tenants should have records")
    
    // Verificar que tenant_id está correto em cada registro
    items := scanTable(t, "events")
    tenantIDs := extractField(items, "tenant_id")
    assert.ElementsMatch(t, tenants, tenantIDs)
}
```

**Caso 5: Mix válido/inválido**
```go
func TestMixedValidInvalid(t *testing.T) {
    clearTables(t)
    for i := 0; i < 5; i++ {
        publishEvent(t, marshal(buildValidCloudEvent(ulid.Make().String(), "tenant-001")))
    }
    for i := 0; i < 3; i++ {
        publishEvent(t, buildInvalidEvent(i))
    }
    
    eventuallyAssert(t, func() bool {
        return countRecords(t, "events") == 5 && countRecords(t, "quarantined_events") == 3
    }, 20*time.Second, "5 valid + 3 quarantined")
}
```

---

## Tarefas — Documentação

### README.md (seções obrigatórias — kickoff 8.1)

1. **Título + descrição** — 1 parágrafo, profissional, em inglês
2. **Diagrama ASCII:**
```
Producer ──► SQS Queue ──► Processor ──► DynamoDB (events)
                │                   └──► DynamoDB (quarantined_events)
                └─► DLQ (after 5 failures)
```
3. **Quick start** — 3 comandos:
```bash
git clone https://github.com/eduardohitek/pismo-event-processor
cd pismo-event-processor
make up && make publish && make inspect
```
4. **Como rodar/inspecionar/parar** — `make up/down/logs/publish/inspect/inspect-quarantine/demo-*`
5. **Why these choices** — CloudEvents vs custom envelope, JSON Schema vs code validation, SQS vs Kafka, DynamoDB vs Postgres, alternativas rejeitadas com racional
6. **Resilience model** — tabela: tipo de falha → ação → garantia
7. **Schema versioning** — convenção `com.pismo.<domain>.<event>.<vN>`
8. **Project layout** — árvore com comentários em cada diretório
9. **Future evolution** — Sender via DynamoDB Streams, Schema Registry, Outbox no producer
10. **Evaluation criteria mapping** — tabela dos 6 critérios oficiais → partes do projeto

### docs/architecture.md

- Por que CloudEvents v1.0 (interoperabilidade, spec matura, SDK oficial)
- Por que JSON Schema Draft 2020-12 vs go-playground/validator (contract validation vs struct validation)
- Por que DynamoDB vs Postgres (serverless, sem schema migration, streams nativo)
- Design da Primary Key `id` (ULID): distribuição uniforme, idempotência via conditional write
- Trade-off aceito: sem ordering por tenant (aceitável para o case)
- Schema evolution strategy: `vN` no type permite múltiplas versões em paralelo
- Por que at-least-once + idempotent consumer (exactly-once é ilusão em sistemas distribuídos)

### docs/resilience.md

- Mapa completo do pipeline: Producer → SQS → Processor → DynamoDB
- Onde eventos podem ser perdidos e como é mitigado:
  - Producer falha antes do SendMessage → mensagem nunca entra na fila
  - SQS perde mensagem → MessageRetentionPeriod 14d, standard queue durabilidade 99.9%
  - Processor crash durante processamento → VisibilityTimeout 30s, reentrega automática
  - DynamoDB transiente → não ack, SQS reentrega, DLQ após 5 tentativas
  - Duplicata → idempotência via attribute_not_exists(id)
- Métricas que provam "no event is lost": DLQ depth, quarantine rate, events count vs published count

### docs/sender-design.md

Design proposto (out of scope, demonstra visão do ciclo completo):
- DynamoDB Streams → Lambda trigger vs KCL worker (trade-offs)
- Configuração do event source mapping: `MaximumRetryAttempts`, `BisectBatchOnFunctionError`, `ReportBatchItemFailures`, `OnFailure destination` (SQS DLQ ou S3)
- Idempotência de entrega no Sender
- Catchup contra janela de 24h dos Streams
- Reconciliação contra falha silenciosa

---

## Verificação final (todos os critérios do kickoff seção 10)

```bash
# 1. Fluxo completo
make up && make publish && make inspect
# Esperado: 5 registros em events sem erro

# 2. Quarentena
# Publicar evento inválido e verificar quarantined_events
# Esperado: aparece em quarantined_events, não em events

# 3. Idempotência
make demo-idempotency
# Esperado: 1 registro mesmo com 3 publicações

# 4. SIGTERM sem perda (manual)
# kill -SIGTERM <processor-pid> durante processamento → restart → mensagens reprocessadas

# 5. Testes unitários
make test
# Esperado: >70% cobertura em internal/

# 6. Testes de integração
make test-integration
# Esperado: 5 casos passam

# 7. README completo (verificar seções 1-10 manualmente)

# 8. Sem TODO/FIXME
grep -r "TODO\|FIXME" --include="*.go" .
# Esperado: zero saída

# 9. Qualidade de código
go vet ./...    # zero saída
gofmt -l .      # zero saída

# 10. Logs JSON
docker compose logs processor | head -5 | jq .
# Esperado: JSON estruturado parseável
```

## Critério de conclusão
Todos os 10 critérios da seção 10 do kickoff satisfeitos. Projeto pronto para push no GitHub.
