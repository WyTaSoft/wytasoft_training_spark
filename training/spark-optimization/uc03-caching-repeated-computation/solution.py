# Databricks notebook source
# MAGIC %md
# MAGIC # UC03 — Caching & Repeated Computation (INSTRUCTOR SOLUTION)
# MAGIC
# MAGIC One reasonable optimized version. Don't hand this out until the debrief.
# MAGIC The outputs are identical to the starter; the shared pipeline runs **once**.
# MAGIC
# MAGIC Key ideas applied:
# MAGIC 1. **Cache the reused node** — the cleaned + filtered DataFrame, not the raw read.
# MAGIC 2. **Materialize once** — the first action populates the cache; later reports read memory.
# MAGIC 3. **One pass for sanity checks** — combine the debug `count()`s into a single aggregation.
# MAGIC 4. **Clean up** — `unpersist()` when done.
# MAGIC 5. **Know when NOT to cache** — for cross-job reuse, write an intermediate Delta table.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import StorageLevel

RAW_CSV_PATH = "/tmp/training/uc03/transactions_csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the shared pipeline (same logic as the starter)
# MAGIC
# MAGIC Also: skip `inferSchema` (UC01 lesson) — give an explicit schema.

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               DoubleType, DateType)

schema = StructType([
    StructField("transaction_id", LongType()),
    StructField("transaction_date", DateType()),
    StructField("country", StringType()),
    StructField("customer_id", LongType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("channel", StringType()),
    StructField("status", StringType()),
])

raw = (spark.read.option("header", "true").schema(schema).csv(RAW_CSV_PATH))

fx = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17}
fx_expr = F.create_map(*[x for k, v in fx.items() for x in (F.lit(k), F.lit(v))])

clean = (raw
         .withColumn("country", F.upper(F.col("country")))
         .withColumn("amount_eur", F.round(F.col("amount") * fx_expr[F.col("currency")], 2))
         .withColumn("amount_bucket",
                     F.when(F.col("amount_eur") < 500, "S")
                      .when(F.col("amount_eur") < 2000, "M")
                      .otherwise("L"))
         .filter(F.col("status") == "POSTED")
         .filter((F.col("transaction_date") >= "2024-01-01") &
                 (F.col("transaction_date") <= "2024-01-31")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 1 + 2 — cache the reused node, materialize once
# MAGIC
# MAGIC MEMORY_AND_DISK spills to disk if it doesn't fit in memory (safer than MEMORY_ONLY).

# COMMAND ----------

clean = clean.persist(StorageLevel.MEMORY_AND_DISK)

# One action populates the cache. After this, every report reads from the cache.
clean_count = clean.count()
print("Clean row count:", clean_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The reports — now served from cache (no re-scan of the CSV)

# COMMAND ----------

total = clean.agg(F.round(F.sum("amount_eur"), 2).alias("total_eur"))
display(total)

# COMMAND ----------

by_country = (clean.groupBy("country")
             .agg(F.round(F.sum("amount_eur"), 2).alias("total_eur"))
             .orderBy("country"))
display(by_country)

# COMMAND ----------

by_channel = (clean.groupBy("channel")
             .agg(F.round(F.sum("amount_eur"), 2).alias("total_eur"))
             .orderBy("channel"))
display(by_channel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 3 — sanity checks in ONE pass instead of two extra full jobs

# COMMAND ----------

checks = clean.agg(
    F.sum(F.when(F.col("amount_bucket") == "L", 1).otherwise(0)).alias("large_transactions"),
    F.countDistinct("customer_id").alias("distinct_customers"),
).collect()[0]
print("Large transactions:", checks["large_transactions"])
print("Distinct customers:", checks["distinct_customers"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 4 — release the cache when done

# COMMAND ----------

clean.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 5 (alternative) — when to materialize to Delta instead of caching
# MAGIC
# MAGIC In-memory cache lives and dies with this Spark session/notebook. If the cleaned table is
# MAGIC consumed by **other notebooks or scheduled jobs**, write it once to **Delta** and let them
# MAGIC read it — that's reuse across sessions, with statistics and time travel as a bonus.

# COMMAND ----------

CLEAN_DELTA_PATH = "/tmp/training/uc03/transactions_clean_delta"

(clean  # if still needed; otherwise rebuild the pipeline before this step
 .write.mode("overwrite").format("delta").save(CLEAN_DELTA_PATH))

# Downstream jobs:  spark.read.format("delta").load(CLEAN_DELTA_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to prove it
# MAGIC
# MAGIC Compare the starter vs this notebook in the **Spark UI**:
# MAGIC - **Jobs tab:** far fewer jobs; the CSV is scanned **once**, not 6×.
# MAGIC - **Storage tab:** a cached entry appears (the `clean` DataFrame) with its size/fraction.
# MAGIC - **Wall-clock:** the 2nd–6th reports return almost instantly (served from cache).
# MAGIC
# MAGIC ### Discussion points
# MAGIC - Cache the **reused** node, not the raw read.
# MAGIC - Caching is lazy — it isn't populated until the first action.
# MAGIC - Don't cache something used only once (pure overhead).
# MAGIC - Always `unpersist()`; prefer Delta for cross-job reuse.
