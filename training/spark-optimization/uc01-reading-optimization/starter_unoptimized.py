# Databricks notebook source
# MAGIC %md
# MAGIC # UC01 — Reading Data Efficiently (STARTER — NOT OPTIMIZED)
# MAGIC
# MAGIC This notebook works but is deliberately inefficient. Your job is to optimize the
# MAGIC **read phase** while keeping the final report identical.
# MAGIC
# MAGIC Read the use-case description in `README.md` first, then open the **Spark UI**
# MAGIC (Jobs / Stages / SQL) while you run the cells to see what is slow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Generate a sample dataset (run once)
# MAGIC
# MAGIC This creates a large-ish CSV folder so the inefficiencies are visible.
# MAGIC In a real session you can point at an existing dataset instead.

# COMMAND ----------

from pyspark.sql import functions as F

# Where the raw CSV will live. Adjust if needed (DBFS path).
RAW_CSV_PATH = "/tmp/training/uc01/orders_csv"

# ~20M rows. Increase rows if your cluster is large and you want the pain to be obvious.
N_ROWS = 20_000_000

countries = ["FR", "DE", "ES", "IT", "US", "UK", "MA", "NL", "BE", "PT"]

orders = (
    spark.range(0, N_ROWS)
    .withColumn("order_id", F.col("id"))
    # spread orders over 60 days
    .withColumn("order_date", F.date_add(F.lit("2024-01-01"), (F.col("id") % 60).cast("int")))
    .withColumn("country", F.element_at(F.array(*[F.lit(c) for c in countries]),
                                        (F.col("id") % len(countries) + 1).cast("int")))
    .withColumn("customer_id", (F.col("id") % 1_000_000).cast("long"))
    .withColumn("product_id", (F.col("id") % 50_000).cast("long"))
    .withColumn("quantity", (F.col("id") % 5 + 1).cast("int"))
    .withColumn("unit_price", F.round(F.rand(seed=42) * 100 + 1, 2))
    .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("payment_method", F.element_at(
        F.array(F.lit("card"), F.lit("paypal"), F.lit("transfer")),
        (F.col("id") % 3 + 1).cast("int")))
    .withColumn("status", F.element_at(
        F.array(F.lit("PAID"), F.lit("PENDING"), F.lit("CANCELLED")),
        (F.col("id") % 3 + 1).cast("int")))
    .drop("id")
)

# Write as plain CSV with header (row-based, no statistics, schema not stored).
(orders.write
    .mode("overwrite")
    .option("header", "true")
    .csv(RAW_CSV_PATH))

print("Sample CSV written to", RAW_CSV_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The non-optimized report
# MAGIC
# MAGIC Goal: total revenue per country for **2024-01-15**, limited to FR / DE / ES.

# COMMAND ----------

# (!) Reads the ENTIRE folder and INFERS the schema by scanning all the data.
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")   # extra full pass over the data just to guess types
      .csv(RAW_CSV_PATH))

# COMMAND ----------

# (!) Debug actions in the hot path — each one launches its own Spark job.
print("Total rows in dataset:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# (!) Filtering happens late, and we never reduce the columns we read.
report = (df
          .filter(df["order_date"] == "2024-01-15")
          .filter(df["country"].isin("FR", "DE", "ES"))
          .groupBy("country")
          .agg(F.round(F.sum("amount"), 2).alias("total_revenue")))

# COMMAND ----------

# (!) Another full action; and below we recompute from `df` again.
print("Rows in report:", report.count())
display(report.orderBy("country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Measure
# MAGIC
# MAGIC Open the **Spark UI** and note:
# MAGIC - total wall-clock time for the report,
# MAGIC - number of jobs and stages,
# MAGIC - bytes/files read in the scan node (SQL tab),
# MAGIC - how many rows were read vs how few you actually needed.
# MAGIC
# MAGIC Now go optimize. Keep the final `display(report...)` output identical.
