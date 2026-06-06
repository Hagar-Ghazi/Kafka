# Multi-Node Event-Driven Streaming Architecture
### Fraud Detection & Schema-Enforced Sales Pipeline

A production-grade streaming analytics ecosystem built with **Apache Kafka 4.0.0** in **KRaft mode**, **Confluent Schema Registry**, **PyArrow Parquet Lakehouse Sinks** and **Serverless DuckDB OLAP Engine**.

---

## System Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    1. INGESTION LAYER                    │
│  JSON Production  — Targeted Routing via Partitions      │
│  Avro Production  — Schema Registry & Binary Encoding    │
└────────────────────────────┬─────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────┐
│                  2. STREAMING BACKBONE                   │
│  Apache Kafka Multi-Node KRaft Cluster (3 Brokers)       │
│  Confluent Schema Registry — Schema Contracts            │
└────────────────────────────┬─────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────┐
│                  3. PROCESSING ENGINE                    │
│  Isolated Microservice Consumers (Partition 0 Pinned)    │
└────────────────────────────┬─────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────┐
│                 4. STORAGE & OLAP LAYER                  │
│  Columnar PyArrow Ingestion — Local Parquet Data Lake    │
│  Serverless DuckDB SQL Analytical Query Engine           │
└──────────────────────────────────────────────────────────┘
```

### Key Architectural Milestones

| # | Milestone | Description |
|---|---|---|
| 1 | **Multi-Node KRaft Resilience** | Eliminates ZooKeeper dependencies — low-latency distributed metadata sync across `kafka1`, `kafka2`, `kafka3` |
| 2 | **Targeted Partition Routing** | Explicit partition pinning by geospatial boundary — Cairo transactions bound to Partition 0 |
| 3 | **Decoupled Real-Time Filtering** | Isolated consumer filters high-exposure fraud candidates (> 100,000 EGP) and alerts downstream |
| 4 | **Schema Contract Enforcement** | Avro + Schema Registry strips payload footprint and guarantees zero field-mutation exceptions |
| 5 | **Columnar Lakehouse Ingestion** | Micro-batches streamed directly into PyArrow structures and serialized as compressed `.parquet` files |
| 6 | **Zero-Infra Serverless Analytics** | DuckDB executes expressive SQL directly over cold directory wildcard paths — no server required |

---

## Repository Structure

```
.
├── docker-compose.yaml                   # Infrastructure: Kafka ×3, Kafdrop, Schema Registry
├── README.md
├──screenshots
└── kafka_lab/
    ├── data_lake/
    │   ├── fraud_batch_*.parquet         # High-value fraud incident columnar files
    │   └── sales_batch_*.parquet         # Avro-decoded commercial sales records
    ├── producer_advanced.py              # Geospatial-partition targeted JSON producer
    ├── producer_avro.py                  # Schema-enforced Avro event producer
    ├── consumer_partition.py             # Partition 0 real-time microservice filter
    ├── consumer_to_parquet.py            # JSON event micro-batch Parquet storage sink
    ├── consumer_avro_to_parquet.py       # Schema Registry Avro → Parquet lakehouse engine
    └── analytics.py                      # Serverless DuckDB OLAP analytics engine
```

---

## Infrastructure Configuration

<details>
<summary><strong>docker-compose.yaml</strong></summary>

```yaml
version: '3.8'

services:
  kafka1:
    image: apache/kafka:4.0.0
    container_name: kafka1
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka1:9094,2@kafka2:9094,3@kafka3:9094'
      KAFKA_LISTENERS: 'INSIDE://:9093,OUTSIDE://:9092,CONTROLLER://:9094'
      KAFKA_ADVERTISED_LISTENERS: 'INSIDE://kafka1:9093,OUTSIDE://localhost:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'INSIDE'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJDRDRCMEY'

  kafka2:
    image: apache/kafka:4.0.0
    container_name: kafka2
    ports: ["9095:9095"]
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka1:9094,2@kafka2:9094,3@kafka3:9094'
      KAFKA_LISTENERS: 'INSIDE://:9093,OUTSIDE://:9095,CONTROLLER://:9094'
      KAFKA_ADVERTISED_LISTENERS: 'INSIDE://kafka2:9093,OUTSIDE://localhost:9095'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'INSIDE'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJDRDRCMEY'

  kafka3:
    image: apache/kafka:4.0.0
    container_name: kafka3
    ports: ["9096:9096"]
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka1:9094,2@kafka2:9094,3@kafka3:9094'
      KAFKA_LISTENERS: 'INSIDE://:9093,OUTSIDE://:9096,CONTROLLER://:9094'
      KAFKA_ADVERTISED_LISTENERS: 'INSIDE://kafka3:9093,OUTSIDE://localhost:9096'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'INSIDE'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJDRDRCMEY'

  kafdrop:
    image: obsidiandynamics/kafdrop
    container_name: kafdrop
    depends_on: [kafka1, kafka2, kafka3]
    ports: ["9002:9000"]
    environment:
      KAFKA_BROKERCONNECT: "kafka1:9093,kafka2:9093,kafka3:9093"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    container_name: schema-registry
    depends_on: [kafka1, kafka2, kafka3]
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'kafka1:9093,kafka2:9093,kafka3:9093'
      SCHEMA_REGISTRY_LISTENERS: 'http://0.0.0.0:8081'
```

</details>

---

## Execution Guide

### Prerequisites

```bash
pip install confluent-kafka[avro] fastavro pandas pyarrow duckdb
```

---

### Stage 1 — Environment Initialization

```bash
# Start all 5 containers in detached mode
docker-compose up -d


# Wait 20 seconds for KRaft elections to settle, then verify
docker ps


# Create cluster topics
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server kafka1:9093 \
  --partitions 2 --replication-factor 1 --topic topic_raw

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server kafka1:9093 \
  --partitions 1 --replication-factor 1 --topic topic_fraud

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server kafka1:9093 \
  --partitions 1 --replication-factor 1 --topic sales_topic


# Verify topic registration
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9093 --list
```

---

### Stage 2 — Pipeline Activation

Open **4 terminal windows** side by side and run each in order.

**Terminal 1 — Storage Consumers**
```bash
cd kafka_lab

# JSON fraud-alert lakehouse sink
python consumer_to_parquet.py

# Avro sales lakehouse sink (separate panel)
python consumer_avro_to_parquet.py
```


**Terminal 2 — Real-Time Filter Stream**
```bash
cd kafka_lab
python consumer_partition.py
```


**Terminal 3 — Traffic Producers**
```bash
cd kafka_lab

# Emit geo-routed JSON transactions
python producer_advanced.py

# Emit schema-validated Avro records
python producer_avro.py
```


**Terminal 4 — Serverless Analytics**
```bash
cd kafka_lab
python analytics.py
```

---

## Output Profiles

### Microservice Stream Isolation

`consumer_partition.py` binds exclusively to **Partition 0** (Cairo region), scanning transactions and flagging values exceeding 100,000 EGP:

```
🕵️  Cairo Filter Engine Active. Watching Partition 0 exclusively...
📦 Scanned Record: T101 | Amt: 150000.0 | Loc: Cairo
🚨 ALERT: High Value Fraud Candidate Detected: T101
📦 Scanned Record: T103 | Amt: 230000.0 | Loc: Cairo
🚨 ALERT: High Value Fraud Candidate Detected: T103
📦 Scanned Record: T104 | Amt: 95000.0  | Loc: Cairo
```

### Serverless DuckDB Analytics

```
📊 Connecting to data lake via serverless DuckDB Engine...
🔍 Found 1 Fraud data files and 1 Sales data files in data lake.

🚨 [REPORT 1] FRAUD ANALYTICAL METRICS:
┌──────────┬───────────────────────┬──────────────────────┬──────────────────────┐
│ location │ total_fraud_incidents │ average_fraud_amount │ maximum_fraud_amount │
├──────────┼───────────────────────┼──────────────────────┼──────────────────────┤
│ Cairo    │           2           │       190000.0       │       230000.0       │
└──────────┴───────────────────────┴──────────────────────┴──────────────────────┘

🏆 [REPORT 2] SALES PERFORMANCE (AVRO):
┌─────────────┬─────────────────────┬─────────────────────────┬────────────────────┐
│  item_name  │ total_orders_placed │ total_revenue_generated │ average_item_price │
├─────────────┼─────────────────────┼─────────────────────────┼────────────────────┤
│ MacBook Pro │          1          │         2499.99         │       2499.99      │
│ Dell XPS    │          1          │         1450.50         │       1450.50      │
│ iPad Air    │          1          │          599.00         │        599.00      │
└─────────────┴─────────────────────┴─────────────────────────┴────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 4.0.0 (KRaft) |
| Schema Management | Confluent Schema Registry 7.6.0 |
| Serialization | Apache Avro + fastavro |
| Monitoring | Kafdrop |
| Storage Format | Apache Parquet via PyArrow |
| Analytics Engine | DuckDB (serverless) |
| Containerization | Docker Compose |