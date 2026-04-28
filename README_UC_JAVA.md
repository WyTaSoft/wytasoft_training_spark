# **Java Spark Project — Enhancement: HOCON Config, Log4j2, JUnit 5 & Structured Streaming**

> Branch: **`features/UC_Java_Enhancement_Config_Log4j2_UnitTest`**

This branch builds on top of the *Analyse des Commandes Clients* use case (ud04) and turns the
prototype Java Spark job into a **production-shaped application**. It adds:

- **Externalised configuration** via Typesafe Config (HOCON) — no more hard-coded paths.
- **Structured logging** with Log4j2 — Spark/Hadoop noise silenced, application logs visible.
- **JUnit 5 unit tests** with a shared `SparkSession` lifecycle — fast, repeatable, CI-friendly.
- **Spark Structured Streaming** pipeline — log file ingestion with checkpointing & micro-batch trigger.

---

## **Table of Contents**
1. [What This Branch Adds](#what-this-branch-adds)
2. [Available Branches](#available-branches)
3. [Project Structure](#project-structure)
4. [Technologies Used](#technologies-used)
5. [Configuration (`application.conf`)](#configuration-applicationconf)
6. [Logging (`log4j2.xml`)](#logging-log4j2xml)
7. [Unit Tests (JUnit 5)](#unit-tests-junit-5)
8. [Setup and Installation](#setup-and-installation)
9. [Execution](#execution)
10. [Batch Pipeline — `MainDriver`](#batch-pipeline--maindriver)
11. [Streaming Pipeline — `StreamingDriver`](#streaming-pipeline--streamingdriver)
12. [License](#license)
13. [Contact](#contact)

---

## **What This Branch Adds**

| Concern        | Before (ud04)                              | This branch                                                              |
|----------------|--------------------------------------------|--------------------------------------------------------------------------|
| Configuration  | Magic strings / hard-coded paths           | `application.conf` (HOCON) loaded via `AppConfig` / `StreamingAppConfig` |
| Logging        | Default Spark `log4j2-defaults.properties` | Custom `log4j2.xml` — INFO for `com.wts`, WARN for Spark/Hadoop          |
| Tests          | None                                       | JUnit 5 + shared `BaseSparkTest` (`local[2]`, no UI, no event log)       |
| Pipeline scope | Batch ETL only (clients + orders)          | Batch ETL **+** Structured Streaming (log file ingestion)                |
| Env switching  | Code change required                       | Single CLI arg — `dev`, `test`, or `prod`                                |

---

## **Available Branches**

| Branch Name                                             | Description                                                            |
|---------------------------------------------------------|------------------------------------------------------------------------|
| **features/UC_Java_Enhancement_Config_Log4j2_UnitTest** | **(this branch)** HOCON config, Log4j2, JUnit 5, Structured Streaming. |
| **features/UC_Clients_Orders_Docker**                   | ETL pipeline for Clients & Orders with Docker integration.             |
| **features/UC_Analyse_des_Commandes_Clients_ud04**      | Java analysis use case (v4) of client orders.                          |
| **features/UC_Analyse_des_Commandes_Clients_ud03**      | Java analysis use case (v3) of client orders — adds SQL enrichment.    |
| **features/UC_Analyse_des_Commandes_Clients_ud02**      | Java analysis use case (v2) of client orders.                          |

```bash
git checkout features/UC_Java_Enhancement_Config_Log4j2_UnitTest
```

---

## **Project Structure**

```plaintext
src/
├── main/
│   ├── java/com/wts/kayan/
│   │   ├── app/
│   │   │   ├── job/
│   │   │   │   ├── MainDriver.java          # Batch entry point
│   │   │   │   └── StreamingDriver.java     # Streaming entry point
│   │   │   ├── common/
│   │   │   │   ├── PrimaryRunner.java       # Batch orchestrator (Reader → Mapper)
│   │   │   │   └── StreamingRunner.java     # Streaming orchestrator
│   │   │   ├── reader/
│   │   │   │   ├── PrimaryReader.java       # CSV → DataFrame
│   │   │   │   └── StreamingReader.java     # readStream("text") over logs/
│   │   │   ├── mapping/
│   │   │   │   ├── PrimaryMapper.java       # Batch transformations
│   │   │   │   ├── PrimaryView.java         # Batch SQL constants
│   │   │   │   ├── StreamingMapper.java     # regexp_extract on log lines
│   │   │   │   └── LogView.java             # Streaming SQL constants
│   │   │   ├── writer/
│   │   │   │   ├── PrimaryWriter.java       # Parquet sink (partitioned)
│   │   │   │   └── StreamingWriter.java     # Console/Parquet streaming sink
│   │   │   └── utility/
│   │   │       ├── AppConfig.java           # HOCON wrapper (batch)
│   │   │       ├── StreamingAppConfig.java  # HOCON wrapper (streaming)
│   │   │       ├── PrimaryConstants.java
│   │   │       ├── StreamingConstants.java  # Includes LOG_PATTERN regex
│   │   │       ├── SchemaSelector.java
│   │   │       └── LogSchemaSelector.java
│   │   └── sessionmanager/
│   │       └── SparkSessionManager.java     # SparkSession factory
│   └── resources/
│       ├── application.conf                 # HOCON — paths, writer, trigger, checkpoint
│       ├── log4j2.xml                       # Log4j2 configuration
│       └── data/
│           ├── clients/clients.csv
│           └── orders/date=YYYY-MM-DD/orders.csv
└── test/
    ├── java/com/wts/kayan/
    │   ├── BaseSparkTest.java               # Shared SparkSession lifecycle (PER_CLASS)
    │   └── app/
    │       ├── mapping/
    │       │   ├── PrimaryMapperTest.java
    │       │   └── StreamingMapperTest.java
    │       └── utility/
    │           ├── SchemaTest.java
    │           └── LogSchemaTest.java
    └── resources/data/
        ├── clients/clients.csv
        └── orders/date=2023-09-20/orders.csv
```

---

## **Technologies Used**

- **Java 11** — primary language on this branch.
- **Apache Spark 3.5.4** — `spark-sql` for batch + `spark-sql` Structured Streaming.
- **Typesafe Config (HOCON)** — externalised, environment-aware configuration.
- **Log4j2** — SLF4J backend, custom `log4j2.xml`.
- **JUnit 5 (Jupiter)** — unit testing framework with `@TestInstance(PER_CLASS)`.
- **Maven** — build & dependency management. Vulnerable transitive deps (Jackson, Netty, Log4j,
  Guava, ZooKeeper, Avro, Logback, Commons-Compress) are pinned to safe versions via
  `<dependencyManagement>`. `spark-hive` is excluded to avoid Hive 2.3.9 CVEs.
- **Parquet / CSV / text** — input and output formats.

---

## **Configuration (`application.conf`)**

All paths and writer settings live in `src/main/resources/application.conf`. The runtime
environment (`dev`, `test`, `prod`) is selected by the **first CLI argument** to either driver.

```hocon
training_spark {
  app_name: "training_spark_java"
  data {
    clients { dev: "src/main/resources/data/clients/", test: "...", prod: "hdfs:///..." }
    orders  { dev: "src/main/resources/data/orders/",  test: "...", prod: "hdfs:///..." }
    output  { dev: "src/main/resources/data/datalake/clients_orders/", test: "...", prod: "..." }
  }
  writer {
    format:        "parquet"
    partition_by:  "location"
    num_partitions: 2
    mode:          "overwrite"
  }
  partition { column: "date" }
}

training_spark_streaming {
  data {
    logs   { dev: "src/main/resources/data/logs/", test: "...", prod: "..." }
    output { dev: "src/main/resources/data/datalake/logs_analysis/", test: "...", prod: "..." }
  }
  writer     { format: "console", output_mode: "append" }
  checkpoint { dev: "target/checkpoint/logs/", test: "...", prod: "hdfs:///..." }
  trigger    { interval_ms: 5000 }
}
```

`AppConfig` and `StreamingAppConfig` wrap `ConfigFactory.load()` and expose typed accessors —
no string keys leak out into business code.

---

## **Logging (`log4j2.xml`)**

- `com.wts` → **INFO** (full visibility on the application).
- `org.apache.spark`, `org.apache.hadoop`, `org.apache.hive` → **WARN** (silence framework noise).
- `org.apache.spark.sql.execution.streaming`, `org.sparkproject.jetty` → **ERROR** (suppress
  trigger/watermark/socket DEBUG chatter under streaming).
- A `RollingFile` appender is included (commented out) for cluster deployments.

Both drivers force the file to load **before** SLF4J first touches the logger context:

```java
static {
    System.setProperty("log4j2.configurationFile", "log4j2.xml");
}
```

---

## **Unit Tests (JUnit 5)**

`BaseSparkTest` is an abstract class that owns a single `SparkSession` per test class:

```java
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseSparkTest {
    protected SparkSession spark;

    @BeforeAll public void setUpSpark()    { /* local[2], no UI, 2 shuffle partitions */ }
    @AfterAll  public void tearDownSpark() { spark.stop(); }
}
```

Run all tests:

```bash
mvn test
```

Run a single test class:

```bash
mvn -Dtest=StreamingMapperTest test
```

Coverage on this branch:
- `PrimaryMapperTest` — verifies the join + window-function SQL produces the expected enriched rows.
- `StreamingMapperTest` — exercises the regex parser in batch mode (the same code path is reused under streaming).
- `SchemaTest` / `LogSchemaTest` — guard the explicit `StructType` definitions against drift.

---

## **Setup and Installation**

```bash
git clone https://github.com/WyTaSoft/training_spark.git
cd training_spark
git checkout features/UC_Java_Enhancement_Config_Log4j2_UnitTest

mvn clean package
```

Requirements:
- **Java 11** (`JAVA_HOME` set).
- **Maven 3.8+**.
- A local Spark install is **not** required — the project bundles `spark-sql` and runs in
  `local[*]` mode out of the box.

---

## **Execution**

Both drivers accept the environment as the first argument (defaults to `dev`):

### **Batch Pipeline — `MainDriver`**

```bash
spark-submit \
    --class com.wts.kayan.app.job.MainDriver \
    --master local[*] \
    target/wts-training-spark-java.jar dev
```

Pipeline (Extract → Transform → Load):
1. `PrimaryReader` reads `clients.csv` and the latest `orders/date=…` partition.
2. `PrimaryRunner` registers `clients_view` / `orders_view` and runs `PrimaryView.GET_CLIENT_ORDER_SQL`
   (`BROADCAST` hint + `SUM OVER PARTITION BY clientId`).
3. `PrimaryWriter` writes the enriched DataFrame to Parquet, partitioned by `location`.

### **Streaming Pipeline — `StreamingDriver`**

```bash
spark-submit \
    --class com.wts.kayan.app.job.StreamingDriver \
    --master local[*] \
    target/wts-training-spark-java.jar dev
```

Pipeline:
1. `StreamingReader` opens a `readStream("text")` over `data/logs/` with `maxFilesPerTrigger=1`.
2. `StreamingMapper` extracts `(timestamp, level, thread, logger, message)` via `regexp_extract`,
   registers `parsed_logs_view`, and runs `LogView.ENRICH_LOGS_SQL`.
3. `StreamingWriter` starts the query against the configured sink (`console` in dev, `parquet`
   in prod), with checkpointing and a `ProcessingTime(5 s)` trigger.

The driver blocks on `query.awaitTermination()` — stop the job with `Ctrl+C` / `SIGTERM`.

To exercise the stream, drop a `.log` file with lines matching `StreamingConstants.LOG_PATTERN`
into `src/main/resources/data/logs/` while the driver is running:

```
2024-01-15 08:00:00.001 INFO  [main] com.wts.kayan.app.job.StreamingDriver - Job started
```

---

## **License**

This project is licensed under the MIT License.

---

## **Contact**

For any questions, please reach out:

**Mehdi Tajmouati**
[Website](https://www.wytasoft.com) | [LinkedIn](https://www.linkedin.com/in/mtajmouati/) | [Medium](https://medium.com/@mehdi.tajmouati.wytasoft) | [Udemy](https://www.udemy.com/course/apache-spark-expertise-avancee/) | [Discord](https://discord.gg/NMtBzKFZ)
