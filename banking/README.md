# Banking

Módulo Spring Boot que demonstra **eventos de negócio de transferência bancária** publicados e consumidos via Apache Kafka.

- **Porta HTTP:** `8082`
- **Consumer group:** `banking-group`
- **Broker:** `localhost:9092`

## Fluxo

```text
POST /api/transfers
        │
        ▼
TransferController ──► BankingEventProducer ──► Kafka topics
                                                      │
                                                      ▼
                                              BankingEventConsumer
                                              (loga cada evento)
```

Ao receber um `POST`, a API gera um `transferId` e publica **três eventos**, nesta ordem:

1. `transfer.initiated` — transferência iniciada
2. `account.debited` — conta de origem debitada
3. `transfer.settled` — transferência liquidada

O mesmo aplicativo consome esses tópicos com `@KafkaListener` e registra no log.

## Topics Kafka

| Topic | Propriedade (`application.yml`) | Evento |
|-------|----------------------------------|--------|
| `transfer.initiated` | `banking.topics.transfer-initiated` | `TransferInitiatedEvent` |
| `account.debited` | `banking.topics.account-debited` | `AccountDebitedEvent` |
| `transfer.settled` | `banking.topics.transfer-settled` | `TransferSettledEvent` |

Os tópicos são criados automaticamente na subida da aplicação (`KafkaTopicConfig`: 1 partição, 1 réplica).

## Estrutura do módulo

```text
banking/
├── pom.xml
├── BankingApplication.launch
├── src/main/java/com/example/banking/
│   ├── BankingApplication.java
│   ├── config/KafkaTopicConfig.java
│   ├── event/
│   │   ├── TransferInitiatedEvent.java
│   │   ├── AccountDebitedEvent.java
│   │   └── TransferSettledEvent.java
│   ├── producer/BankingEventProducer.java
│   ├── consumer/BankingEventConsumer.java
│   └── web/TransferController.java
└── src/main/resources/application.yml
```

## O que cada arquivo faz

### `pom.xml`

Define o módulo Maven `banking`, herda do parent `kafka-example` e declara:

- `spring-boot-starter-web` — API REST
- `spring-boot-starter-kafka` — producer/consumer Kafka
- `spring-boot-maven-plugin` — empacota e permite `spring-boot:run`

### `BankingApplication.java`

Ponto de entrada Spring Boot (`@SpringBootApplication`). Sobe o contexto, o servidor HTTP na porta `8082` e a integração Kafka.

### `application.yml`

Configuração da aplicação:

| Seção | Função |
|-------|--------|
| `server.port: 8082` | Porta HTTP do módulo |
| `spring.kafka.bootstrap-servers` | Endereço do broker |
| `spring.kafka.consumer.group-id` | Grupo de consumo (`banking-group`) |
| `spring.kafka.consumer.auto-offset-reset: earliest` | Lê desde o início se não houver offset |
| Deserializers / serializers Jackson JSON | Payload JSON tipado |
| `spring.json.trusted.packages` | Pacotes confiáveis para desserializar (`com.example.banking.event`) |
| `banking.topics.*` | Nomes dos tópicos Kafka |

### `config/KafkaTopicConfig.java`

Declara beans `NewTopic` para os três tópicos. Na inicialização, o Spring Kafka Admin cria os tópicos no broker se ainda não existirem.

### `event/` — eventos de negócio

Records imutáveis que representam o payload publicado/consumido:

| Classe | Campos | Significado |
|--------|--------|-------------|
| `TransferInitiatedEvent` | `transferId`, `fromAccount`, `toAccount`, `amount`, `initiatedAt` | Transferência solicitada entre contas |
| `AccountDebitedEvent` | `transferId`, `accountId`, `amount`, `debitedAt` | Débito na conta de origem |
| `TransferSettledEvent` | `transferId`, `settledAt` | Transferência concluída/liquidada |

### `producer/BankingEventProducer.java`

Encapsula o `KafkaTemplate` e publica cada evento no tópico correspondente. A **chave** da mensagem Kafka é o `transferId` (agrupa mensagens da mesma transferência).

Métodos:

- `publishTransferInitiated`
- `publishAccountDebited`
- `publishTransferSettled`

### `consumer/BankingEventConsumer.java`

Três métodos `@KafkaListener`, um por tópico. Cada um recebe o evento tipado e faz `log.info("Consumed ...")`.

No cenário atual, producer e consumer vivem no **mesmo processo** — útil para ver o ciclo completo no console.

### `web/TransferController.java`

Endpoint REST que dispara o fluxo:

- **Método:** `POST /api/transfers`
- **Status:** `202 Accepted`
- **Body de entrada:**

```json
{
  "fromAccount": "ACC-001",
  "toAccount": "ACC-002",
  "amount": 150.00
}
```

- **Resposta:**

```json
{
  "transferId": "<uuid>",
  "status": "events-published"
}
```

O controller monta os três eventos (mesmo `transferId`) e chama o producer na sequência iniciada → debitada → liquidada.

### `BankingApplication.launch`

Arquivo de launch do Eclipse para rodar como **Spring Boot App** sem configurar manualmente.

## Como rodar

### Pré-requisitos

1. Kafka no ar (na raiz do monorepo):

```bash
docker compose up -d
```

2. JDK 21 e o módulo importado no Eclipse (ou Maven no terminal).

### Eclipse

1. Clique direito em `BankingApplication.launch`
2. **Run As → Spring Boot App**

Ou abra `BankingApplication.java` → **Run As → Spring Boot App**.

### Maven

Na raiz do projeto:

```bash
mvn -pl banking spring-boot:run
```

### Disparar uma transferência

```bash
curl -X POST http://localhost:8082/api/transfers ^
  -H "Content-Type: application/json" ^
  -d "{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":150.00}"
```

No console da aplicação devem aparecer logs de publicação e linhas `Consumed transfer.initiated`, `Consumed account.debited` e `Consumed transfer.settled`.
