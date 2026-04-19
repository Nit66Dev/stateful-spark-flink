# Real-Time Subscription Analytics Pipeline

> A production-grade, dual-engine stream processing pipeline built on **Apache Kafka**, **Apache Spark Structured Streaming**, and **Apache Flink** — designed to ingest, aggregate, and persist subscription event data at scale.

---

## Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Infrastructure Setup](#1-infrastructure-setup)
  - [Database Initialization](#2-database-initialization)
  - [Running the Event Generator](#3-running-the-event-generator)
  - [Running the Stream Processors](#4-running-the-stream-processors)
- [Stream Processing Engines](#-stream-processing-engines)
  - [Spark Structured Streaming](#spark-structured-streaming)
  - [Apache Flink](#apache-flink)
- [Data Model](#-data-model)
- [Configuration](#-configuration)
- [Contributing](#-contributing)
- [License](#-license)

---

## Overview

This project implements a real-time analytics pipeline that simulates a subscription platform (e.g., a streaming service). It continuously generates user subscription events — `buy` and `cancel` actions across multiple pricing tiers — and processes them through **tumbling window aggregations** using two independent stream processing engines.

**Key capabilities:**

- High-throughput event ingestion via Apache Kafka
- Stateful 1-minute tumbling window aggregations
- Dual processing implementations: **Spark** and **Flink** for comparative benchmarking
- Persistent aggregated metrics stored in **MySQL**
- Fully containerised infrastructure via Docker Compose

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Event Generator                              │
│              generator.py  →  Kafka Topic: subscriptions            │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Apache Kafka Broker   │
                    │   (+ Zookeeper)         │
                    └──────┬──────────┬───────┘
                           │          │
             ┌─────────────▼──┐  ┌────▼──────────────┐
             │  Spark         │  │  Apache Flink      │
             │  Structured    │  │  (PyFlink Table    │
             │  Streaming     │  │   API)             │
             └──────┬─────────┘  └────────┬───────────┘
                    │                     │
             ┌──────▼─────────────────────▼───────────┐
             │              MySQL 8.0                  │
             │  ┌──────────────────────────────────┐  │
             │  │  subscription_metrics (Spark)    │  │
             │  │  subscription_metrics_flink      │  │
             │  └──────────────────────────────────┘  │
             └────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Message Broker | Apache Kafka | 7.3.2 (Confluent) |
| Coordination | Apache Zookeeper | 7.3.2 (Confluent) |
| Stream Processing | Apache Spark Structured Streaming | 3.5.0 |
| Stream Processing | Apache Flink (PyFlink) | 1.18 |
| Database | MySQL | 8.0 |
| Infrastructure | Docker & Docker Compose | — |
| Language | Python | 3.8+ |

---

## Project Structure

```
.
├── docker-compose.yml        # Containerised Kafka, Zookeeper, and MySQL
├── generator.py              # Synthetic subscription event producer
├── stateful_mysql.py         # Spark Structured Streaming → MySQL
├── stateful_flink.py         # Apache Flink (PyFlink) → MySQL
└── README.md
```

---

## Getting Started

### Prerequisites

Ensure the following are installed on your system:

- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)
- Python **3.8+**
- Java **11** (required for Spark and Flink)
- pip packages:

```bash
pip install kafka-python pyspark apache-flink
```

For the Flink processor, download the required JARs into your project root:

```bash
# Kafka connector for Flink
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

# JDBC connector for Flink
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# MySQL JDBC driver
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
```

---

### 1. Infrastructure Setup

Spin up Kafka, Zookeeper, and MySQL using Docker Compose:

```bash
docker-compose up -d
```

Verify all services are healthy:

```bash
docker-compose ps
```

Create the Kafka topic before producing events:

```bash
docker exec -it <kafka-container-id> \
  kafka-topics --create \
  --topic subscriptions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

### 2. Database Initialization

Connect to MySQL and create the required sink tables:

```bash
mysql -h 127.0.0.1 -P 3306 -u admin -ppassword streaming_db
```

```sql
-- Table for Spark Structured Streaming output
CREATE TABLE IF NOT EXISTS subscription_metrics (
    window_start  DATETIME,
    window_end    DATETIME,
    plan          VARCHAR(50),
    action        VARCHAR(10),
    total_revenue DOUBLE,
    event_count   BIGINT
);

-- Table for Flink output
CREATE TABLE IF NOT EXISTS subscription_metrics_flink (
    window_start  DATETIME(3),
    window_end    DATETIME(3),
    plan          VARCHAR(50),
    action        VARCHAR(10),
    total_revenue DOUBLE,
    event_count   BIGINT
);
```

---

### 3. Running the Event Generator

The generator simulates subscription events at 2 events/second with a weighted distribution favouring `buy` actions over `cancel`:

```bash
python generator.py
```

**Sample output:**

```
Sending subscription data to Kafka topic 'subscriptions'...
Sent: {'user_id': 'user_4821', 'action': 'buy', 'plan': '4K', 'revenue': 649, 'timestamp': 1718000000}
Sent: {'user_id': 'user_3104', 'action': 'cancel', 'plan': 'HD', 'revenue': 0.0, 'timestamp': 1718000001}
```

---

### 4. Running the Stream Processors

Run **either** processor (or both simultaneously for comparison):

**Spark Structured Streaming:**
```bash
python stateful_mysql.py
```

**Apache Flink:**
```bash
python stateful_flink.py
```

---

## Stream Processing Engines

### Spark Structured Streaming

`stateful_mysql.py` uses **PySpark** with the Kafka source connector to perform stateful aggregations.

**Behaviour:**
- Reads from the `subscriptions` Kafka topic
- Applies a **1-minute tumbling window** with a **10-second watermark** for late data tolerance
- Groups by `window`, `plan`, and `action`
- Computes `total_revenue` and `event_count` per group
- Writes results to MySQL via JDBC using `foreachBatch` in `update` output mode

**Key configuration:**

| Parameter | Value |
|---|---|
| Window Duration | 1 minute |
| Watermark | 10 seconds |
| Output Mode | Update |
| Sink | MySQL (`subscription_metrics`) |

---

### Apache Flink

`stateful_flink.py` uses the **PyFlink Table API** with processing-time semantics.

**Behaviour:**
- Reads from the `subscriptions` Kafka topic using `latest-offset` startup mode
- Uses `PROCTIME()` for processing-time windowing (no event-time skew)
- Executes a **1-minute TUMBLE window** via Flink SQL
- Computes revenue with an inline `CASE` expression per plan tier
- Writes results to MySQL via the JDBC connector

**Key configuration:**

| Parameter | Value |
|---|---|
| Window Type | TUMBLE (Processing Time) |
| Window Duration | 1 minute |
| Startup Mode | latest-offset |
| Sink | MySQL (`subscription_metrics_flink`) |

---

## Data Model

### Event Schema (Kafka)

```json
{
  "user_id":   "user_4821",
  "action":    "buy",
  "plan":      "4K",
  "revenue":   649.0,
  "timestamp": 1718000000
}
```

### Subscription Plans

| Plan | Monthly Revenue (₹) |
|---|---|
| Basic | ₹149 |
| HD | ₹199 |
| FullHD | ₹499 |
| 4K | ₹649 |

### Aggregated Output Schema

```sql
window_start  DATETIME    -- Start of the tumbling window
window_end    DATETIME    -- End of the tumbling window
plan          VARCHAR(50) -- Subscription tier (Basic, HD, FullHD, 4K)
action        VARCHAR(10) -- Event type (buy / cancel)
total_revenue DOUBLE      -- Sum of revenue within the window
event_count   BIGINT      -- Number of events within the window
```

---

## Configuration

All infrastructure connection parameters are configurable. The following defaults are used:

| Service | Host | Port | Credentials |
|---|---|---|---|
| Kafka | `localhost` | `9092` | — |
| Zookeeper | `zookeeper` | `2181` | — |
| MySQL | `localhost` | `3306` | `admin` / `password` |

> **Security Notice:** Default credentials are provided for local development only. Rotate all passwords and use environment variables or a secrets manager before deploying to any non-local environment.

---

## Contributing

Contributions are welcome. Please follow the steps below:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Commit your changes: `git commit -m 'feat: add your feature'`
4. Push to the branch: `git push origin feature/your-feature-name`
5. Open a Pull Request

Please ensure your code is formatted with `black` and passes any existing tests before submitting.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

<p align="center">Built with Apache Kafka · Apache Spark · Apache Flink · MySQL</p>
