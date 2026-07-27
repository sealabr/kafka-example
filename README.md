# Kafka Business Events Base Project

Spec-Driven Development baseline for Kafka business-event examples with Spring Boot 4.1 and Java 21.

## Spec

### Goal

Provide a multi-module Maven project that demonstrates **Kafka as a backbone for business events**, with three independent domain applications ready to produce and consume events.

### Requirements

| Item | Value |
|------|--------|
| Language | Java 21 |
| Framework | Spring Boot 4.1.x |
| Messaging | Spring for Apache Kafka (`spring-boot-starter-kafka`) |
| Build | Maven multi-module |
| Broker | Apache Kafka (KRaft) via Docker Compose on `localhost:9092` |
| Naming | English only |

### Domains

| Module | Domain | Topics |
|--------|--------|--------|
| `ride-hailing` | Call-a-driver | `ride.requested`, `driver.assigned`, `ride.completed` |
| `banking` | Banking | `transfer.initiated`, `transfer.settled`, `account.debited` |
| `subscription` | Subscription | `subscription.created`, `payment.charged`, `subscription.cancelled` |

### Acceptance criteria

- [ ] Kafka broker starts with `docker compose up -d`
- [ ] Each module builds with Maven (`mvn -pl <module> spring-boot:run`)
- [ ] Each module exposes a REST endpoint that publishes a business event
- [ ] Each module consumes its own events via `@KafkaListener` and logs them

### Out of scope

- Unit / integration tests
- Schema Registry / Avro
- Kafka authentication / ACLs
- Shared library module

---

## Plan

1. Create parent Maven POM (`packaging: pom`) with Spring Boot 4.1 and Java 21.
2. Add `docker-compose.yml` with a single Kafka KRaft broker advertised on `localhost:9092`.
3. Scaffold three Spring Boot modules with aligned package layout:
   - `event` — business event records
   - `producer` — publish to Kafka topics
   - `consumer` — `@KafkaListener` handlers
   - `web` — REST trigger endpoints
4. Configure distinct HTTP ports (`8081` / `8082` / `8083`) and consumer group IDs per module.
5. Document run steps in this README (Spec / Plan / Tasks).

### Target flow

```text
HTTP client → Spring Boot module → Kafka topic → same module (@KafkaListener)
```

---

## Tasks

### Infrastructure

- [x] Create parent `pom.xml` (Spring Boot 4.1, Java 21, three modules)
- [x] Add `docker-compose.yml` (Kafka KRaft, port 9092)
- [x] Write Spec / Plan / Tasks in `README.md`

### ride-hailing

- [x] Module POM and `RideHailingApplication`
- [x] Events: `RideRequestedEvent`, `DriverAssignedEvent`, `RideCompletedEvent`
- [x] Producer + consumer + REST trigger (`POST /api/rides`)

### banking

- [x] Module POM and `BankingApplication`
- [x] Events: `TransferInitiatedEvent`, `TransferSettledEvent`, `AccountDebitedEvent`
- [x] Producer + consumer + REST trigger (`POST /api/transfers`)

### subscription

- [x] Module POM and `SubscriptionApplication`
- [x] Events: `SubscriptionCreatedEvent`, `PaymentChargedEvent`, `SubscriptionCancelledEvent`
- [x] Producer + consumer + REST trigger (`POST /api/subscriptions`)

### Future (optional)

- [ ] Add Schema Registry
- [ ] Split producer and consumer into separate processes
- [ ] Add Kafka UI (e.g. Kafdrop)

---

## Conceitos Kafka (KRaft, quorum e afins)

Esta seção explica o modelo usado pelo `docker-compose.yml` deste projeto e o que muda em produção.

### Broker, topic, partition e offset

| Conceito | O que é |
|----------|---------|
| **Broker** | Processo Kafka que armazena dados e atende producers/consumers |
| **Topic** | Canal lógico de eventos (ex.: `transfer.initiated`) |
| **Partition** | Fatia de um topic; permite paralelismo e ordenação **dentro** da partição |
| **Offset** | Posição sequencial de uma mensagem numa partição (0, 1, 2…) |
| **Consumer group** | Conjunto de consumers que dividem as partições entre si; cada mensagem é processada por **um** membro do grupo |

```text
Topic: transfer.initiated
┌─────────────┬─────────────┐
│ Partition 0 │ Partition 1 │
│ offset 0..N │ offset 0..M │
└─────────────┴─────────────┘
        ▲
        │  chave = transferId → mesma key cai na mesma partition
   Producer / Consumer
```

### Producer e consumer

- **Producer** publica mensagens em um topic (neste projeto: `KafkaTemplate` / `BankingEventProducer`).
- **Consumer** lê mensagens (`@KafkaListener`). O broker guarda o **offset commitado** do group no topic interno `__consumer_offsets`.
- `auto-offset-reset: earliest` (nos `application.yml`): se o group ainda não tem offset, começa do início do log.

### ZooKeeper vs KRaft

Historicamente o Kafka usava **Apache ZooKeeper** para metadados do cluster (quem é o controller, quais brokers estão vivos, eleição de líderes de partição, etc.).

**KRaft** (*Kafka Raft*) substitui o ZooKeeper: o próprio Kafka gerencia metadados com um protocolo de consenso **Raft**.

| | ZooKeeper (legado) | KRaft (este projeto) |
|--|--------------------|---------------------|
| Metadados | Processo externo (ZK) | Controllers Kafka |
| Peças a operar | Kafka + ZK | Só Kafka |
| Imagem deste repo | — | `apache/kafka:4.1.0` em modo KRaft |

No lab, **um único container** cumpre os dois papéis: `KAFKA_PROCESS_ROLES: broker,controller`.

### Controller e quorum

- **Controller**: nó que coordena o cluster (eleição de líderes de partição, membership, criação de topics, etc.).
- **Quorum**: conjunto de controllers que devem **concordar** (maioria) para decisões de metadados. Evita “cérebro dividido” se a rede particionar.

```text
Lab (este repo)              Produção típica
┌──────────────────┐         ┌────────────┐ ┌────────────┐ ┌────────────┐
│ broker+controller│         │ controller │ │ controller │ │ controller │
│   (1 votante)    │         │     1      │ │     2      │ │     3      │
└──────────────────┘         └─────▲──────┘ └─────▲──────┘ └─────▲──────┘
                                   │              │              │
                                   └──────── quorum Raft ────────┘
                                              │
                                   ┌──────────┴──────────┐
                                   │ brokers (N nós)     │
                                   │ só role=broker      │
                                   └─────────────────────┘
```

**Regra prática:** número de votantes **ímpar** (`1`, `3`, `5`…). Tolerância a falha ≈ `(N − 1) / 2`.

- 1 votante → zero tolerância (lab ok; produção não).
- 3 votantes → aguenta 1 falha.
- 5 votantes → aguenta 2 falhas.

No Compose: `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` significa “o nó `1` vota em `kafka:9093`”.

### Replication factor, líder e ISR

Cada partition pode ter **réplicas** em vários brokers:

| Termo | Significado |
|-------|-------------|
| **Replication factor (RF)** | Quantas cópias da partition existem |
| **Leader** | Réplica que recebe writes e serve a maioria dos reads |
| **Follower** | Copia o log do líder |
| **ISR** (*In-Sync Replicas*) | Réplicas que estão atualizadas o suficiente |
| **min.insync.replicas** | Mínimo de réplicas no ISR para aceitar writes com `acks=all` |

```text
RF = 3, min.insync.replicas = 2
Broker A (leader)  ●── sync ──► Broker B (ISR)
                   ●── sync ──► Broker C (ISR)
Se B cair, ainda há 2 no ISR → writes com acks=all continuam
Se B e C caírem → writes com acks=all param (proteção de durabilidade)
```

Por isso no lab `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` (só 1 broker). Em produção o alvo típico é **RF=3** e **min ISR=2**.

### Listeners e advertised listeners

- **`listeners`**: onde o processo **escuta** dentro do container/host.
- **`advertised.listeners`**: endereço que o broker **anuncia** aos clients. O client conecta no bootstrap (`localhost:9092`) e depois usa o endereço anunciado.

Neste projeto:

- Listener de clients: `PLAINTEXT://0.0.0.0:9092`
- Anunciado: `PLAINTEXT://localhost:9092` (apps no host Windows/Eclipse)
- Listener `CONTROLLER` na porta `9093`: só quorum KRaft, não é para producers/consumers

Em produção costuma-se separar **INTERNAL** (inter-broker) e **EXTERNAL** (apps), com **SSL/SASL**.

### Consumer group e rebalance

Quando members entram/saem do group, o Kafka redistribui partitions (**rebalance**).

`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` no lab faz o primeiro rebalance **imediato**. Em produção o default (`3000` ms) evita vários rebalances seguidos enquanto vários consumers sobem juntos.

### Auto-create topics

Com `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true`, o primeiro produce/consume pode criar o topic. Conveniente no lab; em produção prefere-se **`false`** e criar topics via Admin API / IaC com RF, partições e retenção explícitos.

Neste projeto os módulos Spring também provisionam os tópicos na subida via `KafkaTopicConfig` (beans `NewTopic` + Spring Kafka Admin). Se o tópico não existir, o Spring cria; se já existir, não recria.

Exemplo (`ride-hailing/config/KafkaTopicConfig.java`):

| Bean | Property (`application.yml`) | Topic |
|------|------------------------------|-------|
| `rideRequestedTopic` | `ride-hailing.topics.ride-requested` | `ride.requested` |
| `driverAssignedTopic` | `ride-hailing.topics.driver-assigned` | `driver.assigned` |
| `rideCompletedTopic` | `ride-hailing.topics.ride-completed` | `ride.completed` |

Cada bean usa `TopicBuilder.name(topic).partitions(1).replicas(1)`:

- **1 partição** — ordenação total das mensagens no tópico (ok para lab; pouco paralelismo)
- **1 réplica** — uma cópia apenas (broker único no Docker local; em produção tipicamente RF ≥ 3)

Os nomes vêm do `application.yml` via `@Value`, então dá para renomear sem alterar o Java. O mesmo padrão existe em `banking` e `subscription`.

#### Partitions e replicas em produção

**Réplicas (RF)** definem durabilidade e disponibilidade. **Partições** definem paralelismo (throughput e quantos consumers no mesmo group podem trabalhar ao mesmo tempo).

| Dimensão | Regra prática | Por quê |
|----------|---------------|---------|
| **Replication factor** | **3** em cluster com ≥ 3 brokers | Aguenta 1 broker fora sem perder dados; padrão de mercado |
| **min.insync.replicas** | **2** (com RF=3) | Com `acks=all`, exige pelo menos 2 réplicas sincronizadas antes de confirmar o write |
| **Partitions** | Começar baixo e **aumentar** conforme carga | Dá para subir o número depois; **não dá para reduzir** sem recriar o topic |
| **Consumers no group** | ≤ número de partitions | 1 partition = no máximo 1 consumer ativo por group; o resto fica ocioso |

Ordenação: Kafka garante ordem **só dentro da mesma partition**. Para ordenar por entidade (ex.: `rideId`, `accountId`), use essa chave no produce — mensagens da mesma key caem na mesma partition.

**Exemplos por cenário**

| Cenário | Partitions | RF | min ISR | Observação |
|---------|------------|----|---------|------------|
| Lab / demo (este repo) | 1 | 1 | 1 | Um broker; simples e suficiente |
| Staging / time pequeno | 3–6 | 3 | 2 | Espelha produção em escala reduzida |
| Eventos de negócio com ordem por entidade (ride, transfer) | 6–24 | 3 | 2 | Key = id da entidade; paralelismo sem perder ordem por id |
| Alto volume / analytics / logs | 24–100+ | 3 | 2 | Mais partitions = mais consumers em paralelo; ordem global não importa |
| Auditoria / ledger crítico | 3–12 | 3 | 2 | Preferir menos partitions bem chaveadas + `acks=all` |
| Cluster com 3 brokers | — | **3** | **2** | RF não deve ultrapassar o número de brokers |
| Cluster com 5+ brokers | — | 3 (às vezes 5) | 2 | RF=5 só se a latência/custo de sync valer a pena |

Exemplo concreto para `ride-hailing` em produção típica (3 brokers):

```java
TopicBuilder.name(topic)
    .partitions(12)
    .replicas(3)
    .config("min.insync.replicas", "2")
    .build();
```

Assim: até 12 consumers no mesmo group processam em paralelo; `rideId` como key mantém a ordem por corrida; RF=3 + min ISR=2 protege contra falha de um broker.

### CLUSTER_ID

Identificador do cluster KRaft. Deve ser o **mesmo** em todos os nós e **estável** após o storage ser formatado. Trocar o ID com disco antigo costuma impedir o broker de subir.

### Como isso se encaixa no fluxo do projeto

```text
curl POST /api/transfers
        → TransferController
        → BankingEventProducer (KafkaTemplate → localhost:9092)
        → topic (partition + offset no broker KRaft)
        → BankingEventConsumer (@KafkaListener, group banking-group)
        → log "Consumed ..."
```

Detalhes dos parâmetros do Compose (lab vs produção) estão comentados em `docker-compose.yml`. Detalhes do módulo banking estão em `banking/README.md`.

---

## How to run

### Prerequisites

- JDK 21 configured in Eclipse (**Window → Preferences → Java → Installed JREs**)
- [Spring Tools](https://spring.io/tools) installed in Eclipse (Marketplace → search `Spring Tools`)
- Docker Desktop (or Docker Engine) running

### 1. Start Kafka

In a terminal, from the project root (`kafka-example`):

```bash
docker compose up -d
```

Broker available at `localhost:9092`. Check with `docker compose ps`.

### 2. Import into Eclipse

1. **File → Import… → Maven → Existing Maven Projects**
2. **Root Directory:** select `kafka-example` (the folder that contains the parent `pom.xml`)
3. Ensure these projects are checked: `kafka-example`, `ride-hailing`, `banking`, `subscription`
4. **Finish**
5. Wait for Maven to download dependencies
6. If versions look wrong: right-click parent → **Maven → Update Project…** → select all modules → **OK**

### 3. Run a module (Spring Boot App)

Each module has a shared launch file:

| Module | Launch file | Port |
|--------|-------------|------|
| ride-hailing | `ride-hailing/RideHailingApplication.launch` | 8081 |
| banking | `banking/BankingApplication.launch` | 8082 |
| subscription | `subscription/SubscriptionApplication.launch` | 8083 |

**Option A — from the `.launch` file**

1. In Package Explorer, open the module folder
2. Right-click `*Application.launch`
3. **Run As → Spring Boot App**

**Option B — from Run Configurations**

1. **Run → Run Configurations…**
2. Open **Spring Boot App**
3. Select `RideHailingApplication`, `BankingApplication`, or `SubscriptionApplication`
4. **Run**

**Option C — from the main class**

1. Open `*Application.java`
2. Right-click → **Run As → Spring Boot App**

You can run one or more modules at the same time (different ports).

### 4. Trigger a business event

With the app running, call the REST endpoint (terminal, Postman, or Eclipse Rest Client):

```bash
# Ride-hailing (8081)
curl -X POST http://localhost:8081/api/rides -H "Content-Type: application/json" -d "{\"passengerId\":\"p-1\",\"pickup\":\"Airport\",\"dropoff\":\"Downtown\"}"

# Banking (8082)
curl -X POST http://localhost:8082/api/transfers -H "Content-Type: application/json" -d "{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":150.00}"

# Subscription (8083)
curl -X POST http://localhost:8083/api/subscriptions -H "Content-Type: application/json" -d "{\"customerId\":\"c-1\",\"plan\":\"PRO\"}"
```

In the Eclipse **Console**, you should see producer activity and `Consumed ...` log lines from `@KafkaListener`.

### 5. Stop

- Stop the app: red square on the Console toolbar (or Boot Dashboard if using Spring Tools)
- Stop Kafka: `docker compose down` from the project root

### Alternative: Maven from terminal

```bash
mvn -pl ride-hailing spring-boot:run
mvn -pl banking spring-boot:run
mvn -pl subscription spring-boot:run
```
