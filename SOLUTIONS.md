# **SOLUTIONS — Reference Code for Each Step**

> Pair this file with [STEPS.md](./STEPS.md). Try each step on your own first; expand the
> matching solution only if you're stuck.

The same code is also available on branch `features/UC_Java_Enhancement_Config_Log4j2_UnitTest`.
Use this file when you want a single answer; use the branch when you want the whole project.

---

## **Step 1 — `SparkSessionManager`**

<details>
<summary>Show solution — <code>src/main/java/com/wts/kayan/sessionmanager/SparkSessionManager.java</code></summary>

```java
package com.wts.kayan.sessionmanager;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.SparkSession;

public final class SparkSessionManager {

    private SparkSessionManager() {}

    public static SparkSession fetchSparkSession(String appName) {

        Configurator.setLevel("org.sparkproject.jetty", Level.ERROR);
        Configurator.setLevel("org.apache.hadoop",       Level.ERROR);

        SparkSession session = SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.tungsten.enabled", "true")
                .config("spark.rdd.compress", "true")
                .config("spark.io.compression.codec", "snappy")
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.ui.enabled", "true")
                .config("spark.ui.port", "4040")
                .getOrCreate();

        session.sparkContext().setLogLevel("ERROR");
        Configurator.setLevel("org.apache.spark", Level.ERROR);
        return session;
    }
}
```
</details>

---

## **Step 2 — `PrimaryConstants`**

<details>
<summary>Show solution — <code>src/main/java/com/wts/kayan/app/utility/PrimaryConstants.java</code></summary>

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
</details>

---

## **Step 3 — `SchemaSelector`**

<details>
<summary>Show solution — <code>src/main/java/com/wts/kayan/app/utility/SchemaSelector.java</code></summary>

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
</details>

---

## **Step 4 — Minimal `MainDriver` + `PrimaryReader`**

<details>
<summary>Show <code>PrimaryReader.java</code></summary>

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
</details>

<details>
<summary>Show <code>MainDriver.java</code> (minimal)</summary>

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
</details>

---

## **Step 5 — Read latest `orders` partition**

<details>
<summary>Show <code>ColumnSelector.java</code></summary>

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
</details>

<details>
<summary>Show <code>PrimaryUtilities.getMaxPartition()</code></summary>

```java
package com.wts.kayan.app.utility;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public final class PrimaryUtilities {
    private PrimaryUtilities() {}

    public static String getMaxPartition(String path, String columnPartitioned, SparkSession spark) {
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            FileStatus[] statuses = fs.listStatus(new Path(path));
            String prefix = columnPartitioned + "=";

            String[] values = Arrays.stream(statuses)
                    .filter(FileStatus::isDirectory)
                    .map(s -> s.getPath().getName())
                    .filter(n -> n.startsWith(prefix))
                    .map(n -> n.substring(prefix.length()))
                    .toArray(String[]::new);

            if (values.length == 0) return "2999-01-01";

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date max = Arrays.stream(values)
                    .map(d -> { try { return sdf.parse(d); }
                                catch (ParseException e) { throw new RuntimeException(e); } })
                    .max(Date::compareTo)
                    .orElseThrow();
            return sdf.format(max);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
```
</details>

<details>
<summary>Show <code>PrimaryReader.readOrders()</code> addition</summary>

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
</details>

---

## **Step 6 — `PrimaryView` + `PrimaryMapper`**

<details>
<summary>Show <code>PrimaryView.java</code></summary>

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
</details>

<details>
<summary>Show <code>PrimaryMapper.java</code></summary>

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
</details>

---

## **Step 7 — `PrimaryRunner`**

<details>
<summary>Show <code>PrimaryRunner.java</code></summary>

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
</details>

---

## **Step 8 — HOCON config + `AppConfig`**

<details>
<summary>Show <code>application.conf</code></summary>

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
</details>

<details>
<summary>Show <code>AppConfig.java</code></summary>

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
</details>

---

## **Step 9 — `PrimaryWriter`**

<details>
<summary>Show <code>PrimaryWriter.java</code></summary>

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
</details>

<details>
<summary>Show final <code>MainDriver.java</code> (E → T → L)</summary>

```java
package com.wts.kayan.app.job;

import com.wts.kayan.app.common.PrimaryRunner;
import com.wts.kayan.app.reader.PrimaryReader;
import com.wts.kayan.app.utility.AppConfig;
import com.wts.kayan.app.utility.PrimaryConstants;
import com.wts.kayan.app.writer.PrimaryWriter;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainDriver {

    static {
        System.setProperty("log4j2.configurationFile", "log4j2.xml");
    }

    private static final Logger logger = LoggerFactory.getLogger(MainDriver.class);

    public static void main(String[] args) {
        String env       = (args.length > 0) ? args[0] : "dev";
        AppConfig config = new AppConfig(env);

        SparkSession spark = SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME);
        logger.info("Training job started — env={}", env);

        PrimaryReader reader   = new PrimaryReader(spark, config);
        PrimaryRunner runner   = new PrimaryRunner(reader, spark);
        Dataset<Row>  enriched = runner.runPrimaryRunner();
        enriched.show();

        new PrimaryWriter().write(enriched, config);
        spark.stop();
    }
}
```
</details>

---

## **Step 10 — `log4j2.xml`**

<details>
<summary>Show <code>src/main/resources/log4j2.xml</code></summary>

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN" monitorInterval="30">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%t] %logger{40} - %msg%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <Logger name="com.wts"           level="INFO"  additivity="false"><AppenderRef ref="Console"/></Logger>
    <Logger name="org.apache.spark"  level="WARN"  additivity="false"><AppenderRef ref="Console"/></Logger>
    <Logger name="org.apache.hadoop" level="WARN"  additivity="false"><AppenderRef ref="Console"/></Logger>
    <Logger name="org.apache.hive"   level="WARN"  additivity="false"><AppenderRef ref="Console"/></Logger>
    <Logger name="org.sparkproject.jetty" level="ERROR" additivity="false"><AppenderRef ref="Console"/></Logger>
    <Logger name="org.apache.zookeeper"   level="ERROR" additivity="false"><AppenderRef ref="Console"/></Logger>
    <Root level="WARN"><AppenderRef ref="Console"/></Root>
  </Loggers>
</Configuration>
```
</details>

---

## **Step 11 — JUnit 5 tests**

<details>
<summary>Show <code>BaseSparkTest.java</code></summary>

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
</details>

<details>
<summary>Show a sample <code>SchemaTest.java</code></summary>

```java
package com.wts.kayan.app.utility;

import com.wts.kayan.BaseSparkTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaTest extends BaseSparkTest {

    static class SchemaProbe extends SchemaSelector {
        @Override protected org.apache.spark.sql.types.StructType clientsSchema() { return super.clientsSchema(); }
        @Override protected org.apache.spark.sql.types.StructType ordersSchema()  { return super.ordersSchema();  }
    }

    private final SchemaProbe probe = new SchemaProbe();

    @Test
    void clientsSchemaHasThreeFields() {
        assertEquals(3, probe.clientsSchema().fields().length);
        assertTrue(probe.clientsSchema().fieldNames()[0].equals("clientId"));
    }

    @Test
    void ordersSchemaHasFourFields() {
        assertEquals(4, probe.ordersSchema().fields().length);
    }
}
```
</details>

---

## **Step 12 — Structured Streaming (bonus)**

<details>
<summary>Show <code>StreamingConstants.java</code></summary>

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
</details>

<details>
<summary>Show streaming block in <code>application.conf</code></summary>

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
</details>

<details>
<summary>Show <code>StreamingReader.java</code></summary>

```java
package com.wts.kayan.app.reader;

import com.wts.kayan.app.utility.StreamingAppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StreamingReader {

    private final SparkSession spark;
    private final StreamingAppConfig config;

    public StreamingReader(SparkSession spark, StreamingAppConfig config) {
        this.spark  = spark;
        this.config = config;
    }

    public Dataset<Row> readLogs() {
        return spark.readStream()
                .format("text")
                .option("maxFilesPerTrigger", 1)
                .load(config.getLogsInputPath());
    }
}
```
</details>

<details>
<summary>Show <code>StreamingMapper.java</code> + <code>LogView.java</code></summary>

```java
package com.wts.kayan.app.mapping;

public final class LogView {
    private LogView() {}

    public static final String ENRICH_LOGS_SQL =
            "SELECT log_timestamp, level, thread, logger, message " +
            "FROM parsed_logs_view " +
            "WHERE level != ''";
}
```

```java
package com.wts.kayan.app.mapping;

import com.wts.kayan.app.utility.StreamingConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class StreamingMapper {

    private final SparkSession spark;

    public StreamingMapper(SparkSession spark) { this.spark = spark; }

    public Dataset<Row> parseLogs(Dataset<Row> raw) {
        String p = StreamingConstants.LOG_PATTERN;
        return raw.select(
                functions.regexp_extract(raw.col("value"), p, 1).as("log_timestamp"),
                functions.regexp_extract(raw.col("value"), p, 2).as("level"),
                functions.regexp_extract(raw.col("value"), p, 3).as("thread"),
                functions.regexp_extract(raw.col("value"), p, 4).as("logger"),
                functions.regexp_extract(raw.col("value"), p, 5).as("message")
        );
    }

    public Dataset<Row> enrichLogs(Dataset<Row> parsed) {
        parsed.createOrReplaceTempView("parsed_logs_view");
        return spark.sql(LogView.ENRICH_LOGS_SQL);
    }
}
```
</details>

<details>
<summary>Show <code>StreamingWriter.java</code></summary>

```java
package com.wts.kayan.app.writer;

import com.wts.kayan.app.utility.StreamingAppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeUnit;

public class StreamingWriter {

    public StreamingQuery write(Dataset<Row> df, StreamingAppConfig cfg) throws Exception {
        return df.writeStream()
                .outputMode(cfg.getOutputMode())
                .format(cfg.getSinkFormat())
                .option("checkpointLocation", cfg.getCheckpointPath())
                .trigger(Trigger.ProcessingTime(cfg.getTriggerIntervalMs(), TimeUnit.MILLISECONDS))
                .start();
    }
}
```
</details>

<details>
<summary>Show <code>StreamingDriver.java</code></summary>

```java
package com.wts.kayan.app.job;

import com.wts.kayan.app.common.StreamingRunner;
import com.wts.kayan.app.reader.StreamingReader;
import com.wts.kayan.app.utility.StreamingAppConfig;
import com.wts.kayan.app.utility.StreamingConstants;
import com.wts.kayan.app.writer.StreamingWriter;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StreamingDriver {

    static {
        System.setProperty("log4j2.configurationFile", "log4j2.xml");
    }

    public static void main(String[] args) throws Exception {
        String env = (args.length > 0) ? args[0] : "dev";
        StreamingAppConfig cfg = new StreamingAppConfig(env);

        SparkSession spark = SparkSessionManager.fetchSparkSession(StreamingConstants.APPLICATION_NAME);

        StreamingReader reader  = new StreamingReader(spark, cfg);
        StreamingRunner runner  = new StreamingRunner(reader, spark);
        Dataset<Row>    parsed  = runner.runStreamingRunner();

        StreamingQuery query = new StreamingWriter().write(parsed, cfg);
        query.awaitTermination();
        spark.stop();
    }
}
```
</details>

---

## **Want the full project?**

```bash
git checkout features/UC_Java_Enhancement_Config_Log4j2_UnitTest
```
