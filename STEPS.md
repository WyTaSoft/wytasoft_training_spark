# **STEPS — From the Starter Skeleton to the Full Java Spark Application**

> Target branch (final): `features/UC_Java_Enhancement_Config_Log4j2_UnitTest`

This guide walks you, step by step, from the empty starter skeleton to a production-shaped
Java Spark application with HOCON config, Log4j2 logging, JUnit 5 tests, and Spark Structured
Streaming.

Each step is small, self-contained, and **compilable**. Run `mvn compile` (and `MainDriver`
when applicable) after every step before moving on.

---

## **Conventions**

- All paths are relative to the project root.
- Package root: `com.wts.kayan`
- After every step: `mvn compile` (or `mvn test` once you have tests).
- If you get stuck: `git diff features/UC_Java_Enhancement_Config_Log4j2_UnitTest -- <file>`.

---

## **Step 1 — `SparkSessionManager`**

**File:** `src/main/java/com/wts/kayan/sessionmanager/SparkSessionManager.java`

Goal: a single static factory that returns a configured `SparkSession`.

```java
package com.wts.kayan.sessionmanager;

import org.apache.spark.sql.SparkSession;

public final class SparkSessionManager {

    private SparkSessionManager() {}

    public static SparkSession fetchSparkSession(String appName) {
        SparkSession session = SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.tungsten.enabled", "true")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        return session;
    }
}
```

**Verify:** `mvn compile` succeeds.

---

## **Step 2 — `PrimaryConstants`**

**File:** `src/main/java/com/wts/kayan/app/utility/PrimaryConstants.java`

Goal: one place for all magic strings.

```java
package com.wts.kayan.app.utility;

public final class PrimaryConstants {
    private PrimaryConstants() {}

    public static final String APPLICATION_NAME = "training_spark_java";
    public static final String CLIENTS          = "clients";
    public static final String ORDERS           = "orders";
    public static final String MODE_OVERWRITE   = "overwrite";
    public static final String MODE_APPEND      = "append";
}
```

---

## **Step 3 — `SchemaSelector` (explicit schemas)**

**File:** `src/main/java/com/wts/kayan/app/utility/SchemaSelector.java`

Goal: declare schemas once, reuse everywhere. Avoids `inferSchema` cost.

```java
package com.wts.kayan.app.utility;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public abstract class SchemaSelector {

    protected StructType clientsSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("name",     DataTypes.StringType,  true),
                DataTypes.createStructField("location", DataTypes.StringType,  true)
        });
    }

    protected StructType ordersSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("orderId",  DataTypes.IntegerType, true),
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("amount",   DataTypes.DoubleType,  true),
                DataTypes.createStructField("date",     DataTypes.DateType,    true)
        });
    }
}
```

---

## **Step 4 — Minimal `MainDriver` + `PrimaryReader` (read clients)**

**File:** `src/main/java/com/wts/kayan/app/reader/PrimaryReader.java`

```java
package com.wts.kayan.app.reader;

import com.wts.kayan.app.utility.SchemaSelector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PrimaryReader extends SchemaSelector {

    private final SparkSession spark;

    public PrimaryReader(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> readClients(String path) {
        return spark.read()
                .schema(clientsSchema())
                .option("header", "true")
                .csv(path);
    }
}
```

**File:** `src/main/java/com/wts/kayan/app/job/MainDriver.java`

```java
package com.wts.kayan.app.job;

import com.wts.kayan.app.reader.PrimaryReader;
import com.wts.kayan.app.utility.PrimaryConstants;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.SparkSession;

public class MainDriver {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME);
        new PrimaryReader(spark)
                .readClients("src/main/resources/data/clients/")
                .show();
        spark.stop();
    }
}
```

**Run:** in your IDE, run `MainDriver.main`. You should see the clients DataFrame printed.

---

## **Step 5 — Read the latest `orders` partition**

Add a `ColumnSelector` helper and a partition-detection utility, then wire them into
`PrimaryReader`.

**File:** `src/main/java/com/wts/kayan/app/utility/ColumnSelector.java`

```java
package com.wts.kayan.app.utility;

public final class ColumnSelector {
    private ColumnSelector() {}

    public static String[] getColumnSequence(String sourceName) {
        switch (sourceName.toLowerCase()) {
            case PrimaryConstants.CLIENTS:
                return new String[]{"clientId", "name", "location"};
            case PrimaryConstants.ORDERS:
                return new String[]{"orderId", "clientId", "amount", "date"};
            default:
                throw new IllegalArgumentException("Unknown source: " + sourceName);
        }
    }
}
```

**File:** `src/main/java/com/wts/kayan/app/utility/PrimaryUtilities.java`

Add a `getMaxPartition(path, columnName, spark)` static method that lists sub-folders matching
`<column>=value` and returns the latest as a `yyyy-MM-dd` string. Use Hadoop's
`FileSystem.listStatus` and `SimpleDateFormat`. (Reference implementation lives in the final
branch — diff against it if needed.)

Extend `PrimaryReader` with `readOrders(path, partitionColumn)`:

```java
public Dataset<Row> readOrders(String basePath, String partitionColumn) {
    String latest = PrimaryUtilities.getMaxPartition(basePath, partitionColumn, spark);
    return spark.read()
            .schema(ordersSchema())
            .option("header", "true")
            .csv(basePath + partitionColumn + "=" + latest + "/")
            .withColumn(partitionColumn, org.apache.spark.sql.functions.lit(latest));
}
```

**Verify:** call both `readClients` and `readOrders` from `MainDriver` and `.show()` them.

---

## **Step 6 — SQL transformation (`PrimaryView` + `PrimaryMapper`)**

**File:** `src/main/java/com/wts/kayan/app/mapping/PrimaryView.java`

```java
package com.wts.kayan.app.mapping;

public final class PrimaryView {
    private PrimaryView() {}

    public static final String GET_CLIENT_ORDER_SQL =
            "SELECT /*+ BROADCAST(c) */\n" +
            "  c.clientId, c.name, c.location,\n" +
            "  o.orderId, o.amount, o.date,\n" +
            "  SUM(o.amount) OVER(PARTITION BY c.clientId) AS totalAmountByClient\n" +
            "FROM clients_view c\n" +
            "LEFT JOIN orders_view o ON c.clientId = o.clientId";
}
```

**File:** `src/main/java/com/wts/kayan/app/mapping/PrimaryMapper.java`

```java
package com.wts.kayan.app.mapping;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PrimaryMapper {

    private final Dataset<Row> clients;
    private final Dataset<Row> orders;
    private final SparkSession spark;

    public PrimaryMapper(Dataset<Row> clients, Dataset<Row> orders, SparkSession spark) {
        this.clients = clients;
        this.orders  = orders;
        this.spark   = spark;
    }

    public Dataset<Row> enrichDataFrame() {
        clients.createOrReplaceTempView("clients_view");
        orders.createOrReplaceTempView("orders_view");
        return spark.sql(PrimaryView.GET_CLIENT_ORDER_SQL);
    }
}
```

**Verify:** call from `MainDriver` and `.show()` — every client row now has `totalAmountByClient`.

---

## **Step 7 — Orchestrator (`PrimaryRunner`)**

**File:** `src/main/java/com/wts/kayan/app/common/PrimaryRunner.java`

```java
package com.wts.kayan.app.common;

import com.wts.kayan.app.mapping.PrimaryMapper;
import com.wts.kayan.app.reader.PrimaryReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PrimaryRunner {

    private final PrimaryReader reader;
    private final SparkSession  spark;

    public PrimaryRunner(PrimaryReader reader, SparkSession spark) {
        this.reader = reader;
        this.spark  = spark;
    }

    public Dataset<Row> runPrimaryRunner() {
        Dataset<Row> clients = reader.readClients("src/main/resources/data/clients/");
        Dataset<Row> orders  = reader.readOrders("src/main/resources/data/orders/", "date");
        return new PrimaryMapper(clients, orders, spark).enrichDataFrame();
    }
}
```

`MainDriver` becomes a four-liner: build session → build runner → `runPrimaryRunner().show()` → stop.

---

## **Step 8 — Externalise configuration with HOCON**

**File:** `src/main/resources/application.conf`

```hocon
training_spark {
  app_name: "training_spark_java"

  data {
    clients { dev: "src/main/resources/data/clients/", test: "src/test/resources/data/clients/", prod: "hdfs:///data/training/clients/" }
    orders  { dev: "src/main/resources/data/orders/",  test: "src/test/resources/data/orders/",  prod: "hdfs:///data/training/orders/" }
    output  { dev: "src/main/resources/data/datalake/clients_orders/", test: "target/test-output/clients_orders/", prod: "hdfs:///datalake/clients_orders/" }
  }

  writer {
    format:        "parquet"
    partition_by:  "location"
    num_partitions: 2
    mode:          "overwrite"
  }

  partition { column: "date" }
}
```

**File:** `src/main/java/com/wts/kayan/app/utility/AppConfig.java`

```java
package com.wts.kayan.app.utility;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AppConfig {
    private static final String ROOT = "training_spark";
    private final Config config;
    private final String env;

    public AppConfig(String env) {
        this.config = ConfigFactory.load();
        this.env    = env;
    }

    public String getClientsPath()    { return config.getString(ROOT + ".data.clients." + env); }
    public String getOrdersPath()     { return config.getString(ROOT + ".data.orders."  + env); }
    public String getOutputPath()     { return config.getString(ROOT + ".data.output."  + env); }
    public String getWriterMode()     { return config.getString(ROOT + ".writer.mode"); }
    public int    getNumPartitions()  { return config.getInt   (ROOT + ".writer.num_partitions"); }
    public String getPartitionBy()    { return config.getString(ROOT + ".writer.partition_by"); }
    public String getPartitionColumn(){ return config.getString(ROOT + ".partition.column"); }
}
```

Refactor `PrimaryReader` and `PrimaryRunner` so they take `AppConfig` instead of raw paths.
`MainDriver` reads `args[0]` as the env (default `"dev"`) and passes `new AppConfig(env)`
through.

---

## **Step 9 — Parquet writer (`PrimaryWriter`)**

**File:** `src/main/java/com/wts/kayan/app/writer/PrimaryWriter.java`

```java
package com.wts.kayan.app.writer;

import com.wts.kayan.app.utility.AppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrimaryWriter {
    public void write(Dataset<Row> df, AppConfig cfg) {
        df.coalesce(cfg.getNumPartitions())
          .write()
          .format("parquet")
          .partitionBy(cfg.getPartitionBy())
          .mode(cfg.getWriterMode())
          .save(cfg.getOutputPath());
    }
}
```

Wire it into `MainDriver` after the runner:

```java
new PrimaryWriter().write(enriched, appConfig);
```

**Verify:** `target/.../clients_orders/location=…/part-*.parquet` exists.

---

## **Step 10 — Log4j2 logging**

**File:** `src/main/resources/log4j2.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%t] %logger{40} - %msg%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <Logger name="com.wts"          level="INFO"  additivity="false"><AppenderRef ref="Console"/></Logger>
    <Logger name="org.apache.spark" level="WARN"  additivity="false"><AppenderRef ref="Console"/></Logger>
    <Logger name="org.apache.hadoop" level="WARN" additivity="false"><AppenderRef ref="Console"/></Logger>
    <Root level="WARN"><AppenderRef ref="Console"/></Root>
  </Loggers>
</Configuration>
```

In `MainDriver`, force the file to load **before** any logger is touched:

```java
static {
    System.setProperty("log4j2.configurationFile", "log4j2.xml");
}
private static final Logger logger = LoggerFactory.getLogger(MainDriver.class);
```

Replace any remaining `System.out.println` with `logger.info(...)`.

---

## **Step 11 — JUnit 5 unit tests**

**File:** `src/test/java/com/wts/kayan/BaseSparkTest.java`

```java
package com.wts.kayan;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseSparkTest {
    protected SparkSession spark;

    @BeforeAll
    public void setUpSpark() {
        spark = SparkSession.builder()
                .appName("unit-test")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false")
                .config("spark.eventLog.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    public void tearDownSpark() {
        if (spark != null) { spark.stop(); spark = null; }
    }
}
```

Add `SchemaTest` (assert that `clientsSchema()` / `ordersSchema()` have the expected fields)
and `PrimaryMapperTest` (build small in-memory clients + orders Datasets, run
`enrichDataFrame()`, assert rows + `totalAmountByClient` value).

**Run:** `mvn test`

---

## **Step 12 — Spark Structured Streaming (bonus)**

Add a second pipeline that ingests log files in real time.

### 12.1 — Constants

**File:** `src/main/java/com/wts/kayan/app/utility/StreamingConstants.java`

```java
package com.wts.kayan.app.utility;

public final class StreamingConstants {
    private StreamingConstants() {}

    public static final String APPLICATION_NAME = "training_spark_streaming_java";

    public static final String LOG_PATTERN =
            "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})"
          + "\\s+(INFO|WARN|ERROR|DEBUG)"
          + "\\s+\\[([^\\]]+)\\]"
          + "\\s+(\\S+)"
          + "\\s+-\\s+(.+)$";
}
```

### 12.2 — Add the streaming block to `application.conf`

```hocon
training_spark_streaming {
  data {
    logs   { dev: "src/main/resources/data/logs/", test: "src/test/resources/data/logs/", prod: "hdfs:///data/training/logs/" }
    output { dev: "src/main/resources/data/datalake/logs_analysis/", test: "target/test-output/logs_analysis/", prod: "hdfs:///datalake/logs_analysis/" }
  }
  writer     { format: "console", output_mode: "append" }
  checkpoint { dev: "target/checkpoint/logs/", test: "target/checkpoint-test/logs/", prod: "hdfs:///checkpoint/logs/" }
  trigger    { interval_ms: 5000 }
}
```

### 12.3 — `StreamingAppConfig`

Mirror `AppConfig` but bound to root `training_spark_streaming`. Expose
`getLogsInputPath`, `getSinkFormat`, `getOutputMode`, `getCheckpointPath`,
`getTriggerIntervalMs()`.

### 12.4 — `StreamingReader`

```java
return spark.readStream()
        .format("text")
        .option("maxFilesPerTrigger", 1)
        .load(appConfig.getLogsInputPath());
```

### 12.5 — `StreamingMapper`

Use `regexp_extract(value, LOG_PATTERN, n)` for the five capture groups, register the result
as `parsed_logs_view`, then run a SQL query (`LogView.ENRICH_LOGS_SQL`) that filters out
unparsed rows (`WHERE level != ''`).

### 12.6 — `StreamingWriter`

```java
return df.writeStream()
        .outputMode(cfg.getOutputMode())
        .format(cfg.getSinkFormat())
        .option("checkpointLocation", cfg.getCheckpointPath())
        .trigger(Trigger.ProcessingTime(cfg.getTriggerIntervalMs(), TimeUnit.MILLISECONDS))
        .start();
```

### 12.7 — `StreamingDriver`

Same five-step pattern as `MainDriver`, but ends with `query.awaitTermination()`.

**Run:** start `StreamingDriver`, then drop a `.log` file with a matching line into
`src/main/resources/data/logs/`. The console sink prints each parsed batch.

---

## **Done — final checklist**

- [ ] `mvn clean package` produces a fat JAR.
- [ ] `mvn test` is green (≥ 2 test classes).
- [ ] `MainDriver dev` writes Parquet under `src/main/resources/data/datalake/clients_orders/`.
- [ ] `StreamingDriver dev` prints parsed log rows on the console as files arrive.
- [ ] `application.conf` is the only place where paths and writer settings live.
- [ ] Application logs (INFO) appear via Log4j2; Spark/Hadoop noise is silenced.

You are now at parity with `features/UC_Java_Enhancement_Config_Log4j2_UnitTest`.

---

## **Stuck on a step?**

Diff your file against the reference implementation:

```bash
git diff features/UC_Java_Enhancement_Config_Log4j2_UnitTest -- <path/to/your/file>
```
