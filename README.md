# **Java Spark Training — Starter Skeleton**

> Branch: **`features/UC_Java_Starter_Skeleton`**

This branch is the **starting point** for the WyTaSoft Java Spark training. The build is fully
configured (Maven, dependencies, assembly, logging, sample data) but **no business code is
written yet** — that is what you, the trainee, will add step by step.

The goal: skip the boilerplate, focus on the Spark concepts.

### **Where to start**

| Document                     | Read it when…                                                  |
|------------------------------|----------------------------------------------------------------|
| [SETUP.md](./SETUP.md)       | …you just cloned the repo. JDK 11, IntelliJ, `--add-opens`.    |
| [STEPS.md](./STEPS.md)       | …your build is green and you're ready to write code (12 steps). |
| [SOLUTIONS.md](./SOLUTIONS.md) | …you're stuck on a step and want to peek at the answer.       |

---

## **Architecture — the ETL pattern you will build**

```
                 ┌──────────────────────┐
                 │    application.conf  │  ← HOCON (paths, writer settings, env)
                 └──────────┬───────────┘
                            │ loaded by
                            ▼
                 ┌──────────────────────┐
                 │       AppConfig      │
                 └──────────┬───────────┘
                            │ injected into every layer
       ┌────────────────────┼────────────────────┐
       │                    │                    │
       ▼                    ▼                    ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Reader    │      │   Mapper    │      │   Writer    │
│  (Extract)  │      │ (Transform) │      │   (Load)    │
│             │      │             │      │             │
│ CSV → DF    │      │ SQL on temp │      │ DF → Parquet│
│ + schema    │      │ views       │      │ partitioned │
└──────┬──────┘      └──────┬──────┘      └──────▲──────┘
       │                    ▲                    │
       │  Dataset<Row>      │                    │
       └─────────► Runner ──┘                    │
                  (orchestrator)                 │
                  └──────────────────────────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │      MainDriver      │  ← entry point (main)
                 └──────────────────────┘

  Streaming variant (bonus — Step 12 in STEPS.md):

      logs/*.log  ──►  StreamingReader  ──►  StreamingMapper  ──►  StreamingWriter
      (text source,    (regexp_extract,        (parsed_logs_view,    (console / parquet,
       1 file/trigger)  5 capture groups)       LogView SQL)          checkpointed)
```

Three rules of the pattern:
1. **`Reader`, `Mapper`, `Writer` know nothing about each other** — they only know `AppConfig`.
2. **The `Runner` is the only class that wires them together.**
3. **`MainDriver` only owns the lifecycle**: load config → create session → run runner →
   write → stop session.

---

## **What is already done for you**

- `pom.xml` — Java 11, Spark 3.5.4, JUnit 5, Typesafe Config, Log4j2 — with all vulnerable
  transitive deps pinned to safe versions and `spark-hive` excluded (Hive 2.3.9 CVEs).
- `assembly/assembly.xml` — `mvn package` produces a fat JAR ready for `spark-submit`.
- `.mvn/jvm.config` — JVM flags so Spark + Java 11 reflective access works out of the box.
- `src/main/resources/application.conf` — empty HOCON template (you fill it in).
- `src/main/resources/log4j2.xml` — preconfigured: INFO for `com.wts`, WARN for Spark/Hadoop.
- `src/main/resources/data/` — sample `clients.csv` and partitioned `orders/date=YYYY-MM-DD/`.
- Empty Java packages, ready for you to drop classes into:

```
src/main/java/com/wts/kayan/
├── app/
│   ├── job/           ← MainDriver / StreamingDriver
│   ├── common/        ← Runners (orchestration)
│   ├── reader/        ← Read CSV / open streaming source
│   ├── mapping/       ← Transformations + SQL constants
│   ├── writer/        ← Parquet / streaming sinks
│   └── utility/       ← AppConfig, Constants, SchemaSelector
└── sessionmanager/    ← SparkSessionManager
```

---

## **Suggested learning path — small steps to ship a full Java Spark job**

> **Detailed step-by-step instructions with code snippets live in [STEPS.md](./STEPS.md).**
> The list below is the high-level table of contents.

Each step is small enough to compile, run, and verify before moving on.

### **Step 1 — SparkSession**
- Create `sessionmanager/SparkSessionManager.java`.
- Static factory returning a `SparkSession.builder().master("local[*]").getOrCreate()`.

### **Step 2 — Constants**
- Create `utility/PrimaryConstants.java` — application name, dataset identifiers, write modes.

### **Step 3 — Read CSV**
- Create `reader/PrimaryReader.java` with an explicit `StructType` schema.
- Read `data/clients/clients.csv` and `.show()` it from a temporary `MainDriver`.

### **Step 4 — Read partitioned orders**
- Extend `PrimaryReader` to read the latest partition of `data/orders/date=…/`.
- Add a `SchemaSelector` helper for reusable schemas.

### **Step 5 — SQL transformation**
- Create `mapping/PrimaryView.java` — SQL constant with a `BROADCAST` hint and
  `SUM(amount) OVER (PARTITION BY clientId)` window function.
- Create `mapping/PrimaryMapper.java` — registers `clients_view` / `orders_view` and runs
  `sparkSession.sql(...)`.

### **Step 6 — Orchestrator**
- Create `common/PrimaryRunner.java` — wires `PrimaryReader → PrimaryMapper`.
- Update `MainDriver` to delegate to the runner (Extract → Transform).

### **Step 7 — Externalise configuration (HOCON)**
- Fill in `application.conf` with `dev` / `test` / `prod` blocks for input, output, writer.
- Create `utility/AppConfig.java` — wraps `ConfigFactory.load()` with typed accessors.
- Pass `args[0]` (env) into `AppConfig`.

### **Step 8 — Parquet writer**
- Create `writer/PrimaryWriter.java` — writes the enriched DataFrame to Parquet, partitioned
  by `location`, with `coalesce(numPartitions)` and the configured save mode.
- Add the LOAD step to `MainDriver`.

### **Step 9 — Logging (Log4j2)**
- Add the `static {}` block to `MainDriver` forcing `log4j2.configurationFile=log4j2.xml`.
- Replace `System.out.println` calls with `org.slf4j.Logger`.

### **Step 10 — Unit tests (JUnit 5)**
- Create `test/.../BaseSparkTest.java` with `@TestInstance(PER_CLASS)` and a shared
  `SparkSession` (`local[2]`, no UI, `spark.sql.shuffle.partitions=2`).
- Add `PrimaryMapperTest` covering the join + window function.
- Add `SchemaTest` to guard the `StructType` definitions.

### **Step 11 — Spark Structured Streaming (bonus)**
- Add `StreamingConstants` with a `LOG_PATTERN` regex.
- Create `StreamingReader` (`readStream("text")` with `maxFilesPerTrigger=1`).
- Create `StreamingMapper` using `regexp_extract` + a `parsed_logs_view` temp view.
- Create `StreamingWriter` with checkpointing + `Trigger.ProcessingTime`.
- Create `StreamingDriver` mirroring `MainDriver` and call `query.awaitTermination()`.

---

## **Build & run**

```bash
mvn clean package
mvn test

spark-submit \
    --class com.wts.kayan.app.job.MainDriver \
    --master local[*] \
    target/wts-training-spark-java.jar dev
```

---

## **Reference branches (full solutions)**

If you get stuck, compare against the completed branches:

| Branch                                                  | Covers                                              |
|---------------------------------------------------------|-----------------------------------------------------|
| `features/UC_Analyse_des_Commandes_Clients_ud02`        | Steps 1–4 (read clients + orders).                  |
| `features/UC_Analyse_des_Commandes_Clients_ud03`        | Steps 5–6 (SQL enrichment + runner).                |
| `features/UC_Analyse_des_Commandes_Clients_ud04`        | Step 8 (Parquet writer with partitioning).          |
| `features/UC_Java_Enhancement_Config_Log4j2_UnitTest`   | Steps 7, 9, 10, 11 (HOCON, Log4j2, JUnit, Streaming). |

---

## **License**

This project is licensed under the MIT License.

---

## **Contact**

**Mehdi Tajmouati**
[Website](https://www.wytasoft.com) | [LinkedIn](https://www.linkedin.com/in/mtajmouati/) | [Medium](https://medium.com/@mehdi.tajmouati.wytasoft) | [Udemy](https://www.udemy.com/course/apache-spark-expertise-avancee/) | [Discord](https://discord.gg/NMtBzKFZ)
