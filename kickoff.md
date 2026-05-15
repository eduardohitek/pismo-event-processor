# Kick-off: Pismo Event Processor (Go)

> **Para o Claude Code**: este documento é o briefing completo do projeto. Leia integralmente antes de gerar um plano. Faça perguntas se algo estiver ambíguo. Ao terminar de planejar, use o `plan-exporter` para gravar o plano em `Plans/` antes de começar a implementar.

---

## 1. Contexto

Implementação de referência para o desafio prático da Pismo para vaga de Software Engineer / Staff Engineer (Golang). O case pede o componente **Event Processor**: um serviço reativo que consome eventos de uma fila, valida contra contratos, e persiste para consumo posterior por outro serviço.

Critérios de avaliação da Pismo (em ordem oficial): **Problem Understanding, Maintainability, Simplicity, Testability, Documentation, Reproducibility**. Note que performance **não** está na lista — otimização prematura conta contra.

A entrega final é um repositório público no GitHub que o avaliador clona e roda. O sucesso da entrega é definido por: avaliador consegue rodar tudo com um único comando, entende as decisões pelo README, e o código é legível sem precisar de explicação adicional.

---

## 2. Decisões arquiteturais já tomadas

Estas decisões foram debatidas e definidas — **não revisitar sem motivo forte**. Estão aqui como input, não como tema aberto.

### 2.1 Stack

- **Linguagem**: Go 1.22+
- **Fila**: AWS SQS (standard, não FIFO) + DLQ
- **Persistência**: AWS DynamoDB com Streams habilitado (`NEW_IMAGE`)
- **Envelope de eventos**: CloudEvents v1.0 (CNCF spec)
- **Validação de payload**: JSON Schema (Draft 2020-12) por event type
- **Infraestrutura local**: LocalStack via docker-compose
- **IaC**: Terraform aplicado contra LocalStack
- **Orquestração local**: docker-compose com health checks e dependências

### 2.2 Modelo de dados no DynamoDB

**Tabela `events`** (eventos válidos persistidos):
- Primary Key: `id` (String) — `event_id` do CloudEvent, ULID
- **Justificativa**: distribui uniformemente, idempotência trivial via `attribute_not_exists(id)`
- **Trade-off aceito**: sem ordering por tenant. Aceitável para o case; em produção com ordering requirement, mudaria para `PK=tenant_id, SK=event_id`
- Stream: `NEW_IMAGE`
- Billing: `PAY_PER_REQUEST` (on-demand)

**Tabela `quarantined_events`** (eventos rejeitados deterministicamente):
- Primary Key: `event_id` (String) — pode ser vazio se envelope era unparseable, então usar fallback de `ulid.Make()` quando ausente
- Atributos: `reason`, `detail`, `raw_message` (binary), `quarantined_at`

### 2.3 Modelo de resiliência

**At-least-once delivery + idempotent consumer**. "Exactly-once" não é prometido nem perseguido.

Política de erro no consumer:

| Tipo de falha | Ação |
|---|---|
| Validação falhou (determinístico) | Persiste em `quarantined_events` + Ack |
| Persistência: `ConditionalCheckFailedException` | Log como duplicata + Ack (idempotência funcionando) |
| Persistência: erro transiente | **Não acka** — SQS reentrega, DLQ pega após `maxReceiveCount: 5` |
| `Ack` falhou | Log — mensagem volta, idempotência absorve |

**SQS config**:
- `VisibilityTimeout`: 30s
- `MessageRetentionPeriod`: 14 dias
- `ReceiveMessageWaitTimeSeconds`: 20 (long polling)
- `maxReceiveCount`: 5

### 2.4 Validação em duas camadas

1. **Envelope CloudEvents** — usar `github.com/cloudevents/sdk-go/v2` para parse + `.Validate()`
2. **Payload por event type** — usar `github.com/santhosh-tekuri/jsonschema/v5`, schemas compilados no startup, lookup por `event.Type()`

Mapeamento de atributos CloudEvents → domínio:
- `id` → idempotency key
- `subject` → `tenant_id` (multi-tenancy)
- `type` → event type identifier (com versão embutida: `com.pismo.payment.authorized.v1`)
- `source` → producer identification
- `data` → payload validado pelo schema específico do `type`

### 2.5 Escopo

**Dentro do escopo:**
- Event Processor (consumer SQS → validate → persist DynamoDB)
- Producer simulado em Go (publica eventos válidos e inválidos em SQS)
- Infraestrutura local completa (docker-compose + Terraform + LocalStack)
- Documentação: README principal + decisões arquiteturais
- Testes unitários e de integração

**Fora do escopo (mencionar no README como evolução futura):**
- Sender service (consumer de DynamoDB Streams) — só descrever design, não implementar
- Schema Registry (schemas ficam como arquivos no repo)
- Outbox pattern no producer (producer é simulado, não precisa)
- Observabilidade real (Prometheus/OpenTelemetry) — só estrutura de logs e métricas-conceito

---

## 3. Estrutura proposta do projeto

```
event-processor/
├── cmd/
│   ├── processor/          # main.go do Event Processor
│   └── producer/           # main.go do producer simulado
├── internal/
│   ├── config/             # carregamento de env vars
│   ├── domain/             # tipos puros: Event, Quarantined, QuarantineReason
│   ├── messaging/          # interface Consumer + impl SQS
│   ├── validation/         # CloudEvents + JSON Schema
│   ├── storage/            # interface EventStore + impl DynamoDB
│   └── processor/          # orquestração receive→validate→persist
├── test/
│   └── integration/        # testes e2e com build tag `integration`
├── schemas/
│   ├── cloudevents.json    # opcional, para referência
│   └── payloads/
│       └── com.pismo.payment.authorized.v1.json
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── scripts/
│   └── bootstrap.sh        # wrapper que roda terraform contra localstack
├── docs/
│   ├── architecture.md     # decisões arquiteturais detalhadas
│   ├── resilience.md       # modelo de resiliência + cenários de falha
│   └── sender-design.md    # design proposto do Sender (out of scope)
├── docker-compose.yml
├── Dockerfile              # multi-stage build para o processor + producer
├── Makefile                # alvos: up, down, test, publish, inspect, logs, demo-*
├── README.md
├── .gitignore
├── .dockerignore
└── go.mod
```

**Princípios da estrutura:**
- `internal/` para impedir importação externa acidental
- Cada package tem **uma responsabilidade clara** e **uma interface** para testabilidade
- `cmd/` apenas faz wiring; toda lógica está em `internal/`
- Schemas como arquivos JSON versionados, **não** como structs Go

---

## 4. Bibliotecas alvo (go.mod)

Manter mínimo — Simplicity é critério oficial. Lista exata:

```
github.com/aws/aws-sdk-go-v2                                 // SDK base
github.com/aws/aws-sdk-go-v2/config                          // config loader
github.com/aws/aws-sdk-go-v2/service/sqs                     // SQS client
github.com/aws/aws-sdk-go-v2/service/dynamodb                // DynamoDB client
github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue // marshal helper
github.com/cloudevents/sdk-go/v2                             // CloudEvents
github.com/santhosh-tekuri/jsonschema/v5                     // JSON Schema validator
github.com/oklog/ulid/v2                                     // ULID para event_id
github.com/stretchr/testify                                  // assertions em testes (apenas)
```

**Não usar:**
- Framework HTTP (não tem HTTP)
- ORM (DynamoDB é key-value)
- `go-playground/validator` (já discutido — não é "contract validation")
- Logging libraries externas — usar `log/slog` da stdlib
- Cobra/viper para CLI — flags da stdlib bastam

---

## 5. Pontos de implementação críticos

### 5.1 Domínio

`internal/domain/event.go` define:
- `Event` struct com todos os campos extraídos do CloudEvent (id, source, type, tenant_id, time, spec_version, data_content_type, data_schema, data como `json.RawMessage`, received_at)
- `Quarantined` struct (event_id opcional, reason, detail, raw_message, quarantined_at)
- `QuarantineReason` como string type com constantes: `invalid_envelope`, `unknown_event_type`, `invalid_payload`, `missing_tenant`

### 5.2 Validação

`internal/validation/validator.go`:
- Interface `Validator` com `Validate(raw []byte) (*domain.Event, *ValidationError)`
- Implementação `SchemaValidator` que:
  - Carrega todos os schemas de `schemas/payloads/*.json` no construtor
  - Compila com `jsonschema.NewCompiler().Compile(path)`
  - Indexa por nome do arquivo sem extensão (`com.pismo.payment.authorized.v1.json` → key `com.pismo.payment.authorized.v1`)
- `ValidationError` carrega `Reason` (domain.QuarantineReason) + `Detail` para routing correto pelo orquestrador
- Falha em carregar schemas no startup = panic/return error (fail fast)

### 5.3 Messaging

`internal/messaging/consumer.go`:
- Interface `Consumer` com `Receive(ctx) ([]Message, error)` e `Ack(ctx, Message) error`
- `Message` é tipo neutro (ID, Body, ReceiptHandle, ReceiveCount)
- Implementação `SQSConsumer` que usa long polling
- `SQSClient` definido como interface pequena (só `ReceiveMessage` e `DeleteMessage`) para testabilidade

### 5.4 Storage

`internal/storage/dynamo.go`:
- Interfaces `EventStore` e `QuarantineStore` (cada uma com seu `Save`)
- `ErrDuplicate` exportada para o processor distinguir caso de duplicata
- Implementação usa `attributevalue.MarshalMap` + override do campo `data` para armazenar como string JSON crua (preserva representação)
- `PutItem` no event store usa `ConditionExpression: attribute_not_exists(id)` — esse é o coração da idempotência
- Detecção de duplicata via `errors.As(err, &types.ConditionalCheckFailedException{})`

### 5.5 Processor

`internal/processor/processor.go`:
- Construtor recebe `Config` com as 4 dependências (Consumer, Validator, EventStore, QuarantineStore) + Logger + Workers
- `Run(ctx)` faz:
  1. Spawna N workers consumindo de um channel `jobs`
  2. Loop principal chama `Receive` e empurra para `jobs`
  3. Em `ctx.Done()`, fecha `jobs` e espera workers (graceful shutdown)
- `handle(msg)` é onde a política de erro vive — implementa a tabela da seção 2.3
- Cada Receive tem sub-context com timeout de 25s (menor que long poll de 20s + folga) para não travar shutdown

### 5.6 Cmd/processor

`cmd/processor/main.go`:
- Carrega config via `internal/config`
- Configura `slog` JSON handler
- Usa `signal.NotifyContext` para SIGTERM/SIGINT
- Cria AWS config com `BaseEndpoint` override quando `AWS_ENDPOINT_URL` estiver definido (LocalStack)
- Wire de tudo + `processor.Run(ctx)`
- Tem grace period configurável após sinal de shutdown

### 5.7 Cmd/producer

`cmd/producer/main.go`:
- Flags básicas: `--count` (eventos válidos) e `--invalid` (eventos inválidos para exercitar quarentena)
- Constrói CloudEvents usando o SDK oficial
- 4 modos de "evento inválido" rotacionados: JSON malformado, envelope sem `id`, type desconhecido, payload com schema violation
- Embaralha a ordem antes de enviar para interleaving
- Tenants e merchants aleatórios entre um conjunto fixo para demonstrar multi-tenancy

**Modo `--scenario <name>`** para exercitar cenários específicos com semântica nomeada:
- `idempotency`: envia o mesmo evento 3 vezes (mesmo `id`) — exercita o conditional write
- `mixed-load`: 50 válidos + 10 inválidos interleaved — exercita ambos os caminhos
- `duplicate-burst`: 100 mensagens distribuídas entre 10 IDs únicos — exercita idempotência em volume

Cada cenário é uma função no producer que compõe `--count`, `--invalid` e comportamento específico (ex: repetir IDs). Flag `--scenario` é mutuamente exclusiva com `--count`/`--invalid`.

**Modo `--rate N --duration <D>`** para envio sustentado: N msg/s durante D (ex: `5s`, `30s`). Usa `time.NewTicker` para regular o envio. **Não é benchmark** — é demonstração visual de fluxo contínuo para o avaliador ver logs estruturados rolando e métricas conceituais em movimento. Mix válido/inválido com taxa configurável via `--error-rate 0.1` (10% inválidos por default neste modo).

Todas as flags são opcionais com defaults sensíveis. Help (`--help`) deve listar os cenários disponíveis com 1 linha de descrição cada.

---

## 6. Infraestrutura

### 6.1 docker-compose.yml

Serviços:

1. **localstack** — `localstack/localstack:latest`, expõe `4566`, services: `sqs,dynamodb,dynamodbstreams`, healthcheck via `awslocal sqs list-queues` ou `/_localstack/health`
2. **terraform** — imagem `hashicorp/terraform`, depende de `localstack` saudável, roda `init && apply -auto-approve`, exits após aplicar
3. **processor** — built via `Dockerfile`, depende de terraform completo, env vars apontando para localstack, restart on-failure
4. **producer** — opcional como serviço, mais útil rodado on-demand via `make publish`

**Volume mount**: o terraform precisa do diretório `terraform/` montado read-only. O processor precisa de `schemas/` montado read-only.

**Networking**: rede default do compose; processor referencia localstack como hostname `localstack`.

### 6.2 Terraform

`terraform/main.tf` provisiona:

- `aws_sqs_queue.events_dlq`
- `aws_sqs_queue.events` (com `redrive_policy` apontando para DLQ)
- `aws_dynamodb_table.events` (PK `id`, stream `NEW_IMAGE`, billing on-demand)
- `aws_dynamodb_table.quarantined_events` (PK `event_id`, sem stream)

Provider configurado para LocalStack:
```hcl
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  endpoints {
    sqs            = "http://localstack:4566"
    dynamodb       = "http://localstack:4566"
    dynamodbstreams = "http://localstack:4566"
  }
}
```

Output: `queue_url`, `dlq_url`, `events_table_name`, `quarantine_table_name`.

### 6.3 Dockerfile

Multi-stage:
- Stage 1: `golang:1.22-alpine` para `go build -o /out/processor ./cmd/processor` e `/out/producer ./cmd/producer`
- Stage 2: `gcr.io/distroless/static-debian12:nonroot` com os dois binários
- ENTRYPOINT decidido por argumento ou variável (ou Dockerfile separado por binário — escolher o mais simples)

### 6.4 Makefile

Alvos esperados:
- `make up` — `docker-compose up -d` + aguarda terraform completar
- `make down` — `docker-compose down -v`
- `make logs` — `docker-compose logs -f processor`
- `make test` — `go test ./... -race -cover` (apenas unitários)
- `make test-integration` — `go test -tags=integration -timeout 60s ./test/integration/...` (requer `make up`)
- `make publish` — roda producer com defaults (ex: `--count 5 --invalid 2`)
- `make demo-idempotency` — `producer --scenario idempotency` + sleep + inspect
- `make demo-mixed` — `producer --scenario mixed-load` + sleep + inspect
- `make demo-stream` — `producer --rate 5 --duration 30s` (demo visual de fluxo contínuo)
- `make inspect` — scan da tabela `events` via aws CLI com endpoint LocalStack
- `make inspect-quarantine` — scan da tabela `quarantined_events`
- `make build` — `go build` local sem docker
- `make tidy` — `go mod tidy`

---

## 7. Testes

### 7.1 Testes unitários

- `internal/validation/validator_test.go`: table-driven com casos de sucesso e cada tipo de falha. Schema de teste é criado em `t.TempDir()` para hermeticidade.
- `internal/processor/processor_test.go`: usa fakes (`fakeConsumer`, `fakeValidator`, `fakeEventStore`, `fakeQuarantineStore`) para verificar a tabela de política de erro da seção 2.3. Cada caso é um teste separado.
- `internal/storage/dynamo_test.go`: pode usar mock simples do `DynamoDBClient` ou testcontainer com LocalStack (preferível se simples).

### 7.2 Critério de cobertura

Apontar para >70% de cobertura nas packages do `internal/`. Não buscar 100% — testes triviais (getters, wiring em `cmd/`) não agregam.

### 7.3 Casos críticos para cobrir explicitamente

- Mensagem válida → persistida + acked
- Envelope malformado → quarentena + acked
- Payload inválido → quarentena + acked
- Tipo desconhecido → quarentena + acked
- Tenant ausente → quarentena + acked
- Duplicata → log de duplicata + acked (não re-salva)
- Erro transiente no DynamoDB → **NÃO acka** (invariante crítica)
- Shutdown gracioso com mensagens in-flight

### 7.4 Testes de integração end-to-end

Criar `test/integration/e2e_test.go` com build tag `integration` — protege contra rodar acidentalmente em `go test ./...` sem o ambiente preparado:

```go
//go:build integration
```

**Pré-requisito**: `make up` foi executado e o ambiente está saudável. Os testes assumem que a fila SQS e as tabelas DynamoDB existem no LocalStack.

**Helper essencial**: `eventuallyAssert(t, predicate func() bool, timeout time.Duration, msg string)` — faz polling com intervalo curto (100ms) até o predicado retornar true ou timeout. Sem isso, testes ficam flaky por causa da natureza assíncrona do pipeline.

**Casos a cobrir:**

1. **Happy path**: publica 1 evento válido → eventualmente aparece em `events`, não aparece em `quarantined_events`
2. **Quarentena**: publica 1 evento com envelope malformado → eventualmente aparece em `quarantined_events` com `reason=invalid_envelope`
3. **Idempotência**: publica o mesmo evento (mesmo `id`) 3 vezes → após processamento, há exatamente 1 registro em `events`
4. **Multi-tenancy**: publica eventos para 3 tenants diferentes → todos os 3 aparecem em `events` com `tenant_id` correto
5. **Mix válido/inválido**: publica 5 válidos e 3 inválidos → exatamente 5 em `events` e 3 em `quarantined_events`

**Helper de cleanup**: antes de cada teste, fazer scan + delete em ambas as tabelas para garantir estado limpo. Idealmente em um `TestMain` que prepara/limpa o ambiente, e helpers `clearTables(t)` chamados em cada teste.

**Rodado via**: `make test-integration` que faz `go test -tags=integration -timeout 60s ./test/integration/...`. Documentar no README que requer `make up` prévio.

---

## 8. Documentação

### 8.1 README.md principal

Estrutura obrigatória:

1. **Título + descrição em 1 parágrafo**
2. **Diagrama da arquitetura** em ASCII art
3. **Quick start** — 3 a 5 comandos para subir tudo
4. **Como rodar/inspecionar/parar**
5. **Why these choices** — seção explicando trade-offs (CloudEvents, JSON Schema, SQS, DynamoDB), incluindo alternativas consideradas e por que foram rejeitadas
6. **Resilience model** — tabela de cenários de falha → mitigação
7. **Schema versioning** — convenção `vN` no event type
8. **Project layout** — árvore comentada
9. **Future evolution** — mencionar Sender via Streams, Schema Registry, Outbox no producer (mostra visão sem implementar)
10. **Evaluation criteria mapping** — tabela conectando os 6 critérios oficiais a partes específicas do projeto

Tom: profissional, denso, sem hype. Escrever em inglês (Pismo é internacional). Cada decisão tem racional.

### 8.2 docs/architecture.md

Aprofundamento das decisões: por que CloudEvents, por que JSON Schema vs alternatives, por que DynamoDB vs Postgres com benchmarks conceituais, design da Primary Key e implicações, schema evolution strategy.

### 8.3 docs/resilience.md

Mapa do pipeline e onde eventos podem ser perdidos (do producer ao Sender), com mitigação em cada ponto. Inclui métricas que provam "no event is lost" operacionalmente.

### 8.4 docs/sender-design.md

Design proposto do Sender (out of scope, mas demonstra ciclo arquitetural fechado): Lambda trigger vs KCL worker, idempotência de entrega, configuração de retry e DLQ no event source mapping (`MaximumRetryAttempts`, `BisectBatchOnFunctionError`, `ReportBatchItemFailures`, `OnFailure destination`), catchup contra janela de 24h dos Streams, reconciliação contra falha silenciosa.

---

## 9. Convenções de código

- **Comentários**: em inglês, no estilo godoc (package comment, doc comment por tipo exportado, doc comment por função exportada)
- **Nomes**: pacotes em singular minúsculo, sem stutter (não `validation.Validator` se puder ser `validation.Schema`)
- **Erros**: sentinel errors exportados (ex: `ErrDuplicate`); wrap com `fmt.Errorf("context: %w", err)` em camadas inferiores
- **Contexts**: primeiro parâmetro sempre `ctx context.Context`
- **Interfaces**: definidas no package que **consome**, não no que implementa (princípio Go)
- **Não**: panics em lógica de runtime (só em construtores em falha catastrófica)
- **Logging**: `slog` estruturado, sempre com keys consistentes (`event_id`, `event_type`, `tenant_id`, `message_id`)
- **Imports**: agrupar stdlib / terceiros / internos com linhas em branco

---

## 10. Critérios de aceitação do projeto completo

A entrega está pronta quando:

1. ✅ `git clone && make up && make publish && make inspect` mostra eventos persistidos no DynamoDB sem erro
2. ✅ Publicar evento inválido aparece em `quarantined_events`, não em `events`
3. ✅ `make demo-idempotency` resulta em 1 registro em `events` mesmo com 3 publicações
4. ✅ Matar o processor (SIGTERM) durante processamento não perde mensagens — após restart, mensagens em flight são reprocessadas
5. ✅ `make test` passa com >70% cobertura no `internal/`
6. ✅ `make test-integration` passa com ambiente do `make up` rodando
7. ✅ README contém todas as seções da 8.1, sem erros de inglês ou ambiguidades
8. ✅ Sem TODO ou comentários de FIXME no código entregue
9. ✅ `go vet ./...` e `gofmt -l .` retornam vazio
10. ✅ Logs do processor são JSON estruturado, parseáveis
11. ✅ Avaliador da Pismo consegue rodar o projeto seguindo apenas o README, em ambiente macOS limpo com Docker, em menos de 5 minutos

---

## 11. Ordem sugerida de implementação

Para o Claude Code, sugerir ao final do plano esta ordem (vertical slices, valor incremental):

1. **Fundação**: `go.mod`, estrutura de pastas, `domain/event.go`
2. **Validação**: `validation/validator.go` + testes (sem AWS ainda — totalmente unit-testável)
3. **Schemas**: `schemas/payloads/com.pismo.payment.authorized.v1.json`
4. **Storage**: `storage/dynamo.go` com interface limpa
5. **Messaging**: `messaging/consumer.go`
6. **Processor**: `processor/processor.go` + testes com fakes
7. **Wiring**: `cmd/processor/main.go`
8. **Producer básico**: `cmd/producer/main.go` com `--count` e `--invalid`
9. **Config**: `config/config.go` (extrair do main quando der trabalho)
10. **Infra**: `docker-compose.yml`, `Dockerfile`, `terraform/`
11. **Makefile** (alvos básicos: up, down, test, publish, inspect)
12. **Validação end-to-end manual** — confirmar que `make up && make publish && make inspect` funciona
13. **Producer estendido**: cenários `--scenario` (idempotency, mixed-load, duplicate-burst) e modo `--rate`
14. **Makefile**: alvos `demo-*` e `test-integration`
15. **Testes de integração**: `test/integration/e2e_test.go` com helper `eventuallyAssert`
16. **Docs**: README + docs/*.md por último, com o sistema funcionando à mão

Cada etapa deve compilar e (quando aplicável) ter testes passando antes de avançar.

---

## 12. Anti-objetivos

Para não desviar:

- ❌ **Não** implementar o Sender — apenas documentar design
- ❌ **Não** adicionar Prometheus/OpenTelemetry de verdade — apenas mencionar como evolução
- ❌ **Não** criar um framework genérico de event processing — código é específico para o case
- ❌ **Não** adicionar autenticação/autorização — escopo do case não pede
- ❌ **Não** usar Kafka/Kinesis — overkill, contra critério de Simplicity
- ❌ **Não** suportar protocolos além de SQS — `Consumer` é abstrato mas só uma impl
- ❌ **Não** otimizar para throughput além do trivial — performance não é critério
- ❌ **Não** adicionar Helm charts, Kubernetes manifests — case é local
- ❌ **Não** usar AWS reais — LocalStack é suficiente e barato
- ❌ **Não** construir ferramenta de load test / benchmark separada — performance não está nos critérios de avaliação e benchmark contra LocalStack é meaningless. O modo `--rate` do producer é demo visual, não benchmark.

---

## 13. Pontos para confirmar antes de implementar

Se algum destes não estiver claro depois de ler este documento, **perguntar antes de planejar**:

- Path do repositório local onde deve ser criado o projeto
- Nome do GitHub repo de destino (ex: `eduardohitek/pismo-event-processor`)
- Se deve usar o nome `eduardohitek` no `go.mod` ou outro
- Versão exata do Go disponível no ambiente
- Se há preferência por estrutura de branches (main only? develop?)

---

**Fim do briefing.** Após ler, gerar plano detalhado, exportar com `plan-exporter`, e iniciar pela etapa 1 da seção 11.
