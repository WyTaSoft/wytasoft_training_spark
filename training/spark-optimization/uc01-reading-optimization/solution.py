# Databricks notebook source
# MAGIC %md
# MAGIC # UC01 — Reading Data Efficiently (INSTRUCTOR SOLUTION)
# MAGIC
# MAGIC One reasonable optimized version. Don't hand this out until the debrief.
# MAGIC The output is identical to the starter; the read phase is much cheaper.
# MAGIC
# MAGIC Key ideas applied:
# MAGIC 1. **No `inferSchema`** — provide an explicit schema (saves a full extra scan).
# MAGIC 2. **Column pruning** — `select` only the columns used.
# MAGIC 3. **Predicate pushdown / early filter** — filter before aggregating.
# MAGIC 4. **Better format & layout** — convert once to Delta/Parquet partitioned by
# MAGIC    `transaction_date`, so repeated reads benefit from partition + column pruning and file stats.
# MAGIC 5. **No debug actions** in the hot path (`count`/`limit`/`printSchema` each cost a job).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               IntegerType, DoubleType, DateType)

RAW_CSV_PATH = "/tmp/training/uc01/transactions_csv"
DELTA_PATH = "/tmp/training/uc01/transactions_delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 1 + 2 + 3 — explicit schema, column pruning, early filter (CSV path)
# MAGIC
# MAGIC Even if we are stuck with CSV, we can avoid the inference pass and read only what we need.

# COMMAND ----------

schema = StructType([
    StructField("transaction_id", LongType()),
    StructField("transaction_date", DateType()),
    StructField("country", StringType()),
    StructField("customer_id", LongType()),
    StructField("account_id", LongType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("transaction_type", StringType()),
    StructField("channel", StringType()),
    StructField("status", StringType()),
])

report_csv = (spark.read
              .option("header", "true")
              .schema(schema)                       # no inference pass
              .csv(RAW_CSV_PATH)
              .select("transaction_date", "country", "amount")   # column pruning
              .filter(F.col("transaction_date") == "2024-01-15")  # early filter
              .filter(F.col("country").isin("FR", "DE", "ES"))
              .groupBy("country")
              .agg(F.round(F.sum("amount"), 2).alias("total_amount")))

display(report_csv.orderBy("country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 4 — convert once to Delta partitioned by `transaction_date`
# MAGIC
# MAGIC Pay the conversion cost once; every later read is cheap. For repeated/daily reporting this
# MAGIC is the big win: partition pruning means only one day's files are touched.

# COMMAND ----------

(spark.read
    .option("header", "true")
    .schema(schema)
    .csv(RAW_CSV_PATH)
    .write
    .mode("overwrite")
    .partitionBy("transaction_date")
    .format("delta")
    .save(DELTA_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimized report from Delta
# MAGIC
# MAGIC Columnar + partitioned + statistics. The scan reads only the `2024-01-15` partition and
# MAGIC only the columns referenced. No schema inference, no debug jobs.

# COMMAND ----------

report = (spark.read.format("delta").load(DELTA_PATH)
          .filter(F.col("transaction_date") == "2024-01-15")   # partition pruning
          .filter(F.col("country").isin("FR", "DE", "ES"))
          .select("country", "amount")                          # column pruning
          .groupBy("country")
          .agg(F.round(F.sum("amount"), 2).alias("total_amount")))

display(report.orderBy("country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to prove it
# MAGIC
# MAGIC Compare in the **Spark UI → SQL** tab between the starter and this notebook:
# MAGIC - **bytes read** at the scan node (Delta+partition pruning reads a fraction of the data),
# MAGIC - **number of files** read,
# MAGIC - **number of jobs** (no `count()`/`limit()` => fewer jobs),
# MAGIC - **wall-clock time**.
# MAGIC
# MAGIC ### Optional explain
# MAGIC Run `report.explain(True)` and look for `PartitionFilters`, `PushedFilters`, and the pruned
# MAGIC column list in the `FileScan` node.

# COMMAND ----------

report.explain(True)
