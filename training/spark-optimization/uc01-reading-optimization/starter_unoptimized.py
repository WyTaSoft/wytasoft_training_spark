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
# MAGIC This creates a large-ish CSV folder of **banking transactions** so the inefficiencies
# MAGIC are visible. In a real session you can point at an existing dataset instead.
# MAGIC
# MAGIC > ⚠️ **Always run this cell after pulling updates.** It overwrites the dataset. If you only
# MAGIC > re-run the report against an old dataset, the `2024-01-15` / FR-DE-ES slice can be empty
# MAGIC > (an earlier version of this generator produced correlated date/country values).

# COMMAND ----------

from pyspark.sql import functions as F

# Where the raw CSV will live. Adjust if needed (DBFS path).
RAW_CSV_PATH = "/tmp/training/uc01/transactions_csv"

# ~20M rows. Increase rows if your cluster is large and you want the pain to be obvious.
N_ROWS = 20_000_000

countries = ["FR", "DE", "ES", "IT", "US", "UK", "MA", "NL", "BE", "PT"]

# NOTE: categorical columns are drawn with rand() so they are INDEPENDENT of the date.
# (Deriving both date and country from `id % N` correlates them: because the date period
#  is a multiple of the number of countries, every row on a given day lands on the same
#  country, which makes the FR/DE/ES filter return 0 rows.)
transactions = (
    spark.range(0, N_ROWS)
    .withColumn("transaction_id", F.col("id"))
    # spread transactions over 60 days
    .withColumn("transaction_date", F.date_add(F.lit("2024-01-01"), (F.col("id") % 60).cast("int")))
    .withColumn("country", F.element_at(F.array(*[F.lit(c) for c in countries]),
                                        (F.floor(F.rand(seed=1) * len(countries)) + 1).cast("int")))
    .withColumn("customer_id", (F.col("id") % 1_000_000).cast("long"))
    .withColumn("account_id", (F.col("id") % 2_000_000).cast("long"))
    .withColumn("amount", F.round(F.rand(seed=42) * 5000 + 1, 2))
    .withColumn("currency", F.element_at(
        F.array(F.lit("EUR"), F.lit("USD"), F.lit("GBP")),
        (F.floor(F.rand(seed=2) * 3) + 1).cast("int")))
    .withColumn("transaction_type", F.element_at(
        F.array(F.lit("DEBIT"), F.lit("CREDIT"), F.lit("TRANSFER")),
        (F.floor(F.rand(seed=3) * 3) + 1).cast("int")))
    .withColumn("channel", F.element_at(
        F.array(F.lit("ATM"), F.lit("ONLINE"), F.lit("BRANCH"), F.lit("MOBILE")),
        (F.floor(F.rand(seed=4) * 4) + 1).cast("int")))
    .withColumn("status", F.element_at(
        F.array(F.lit("POSTED"), F.lit("PENDING"), F.lit("REVERSED")),
        (F.floor(F.rand(seed=5) * 3) + 1).cast("int")))
    .drop("id")
)

# Write as plain CSV with header (row-based, no statistics, schema not stored).
(transactions.write
    .mode("overwrite")
    .option("header", "true")
    .csv(RAW_CSV_PATH))

# Sanity check: the report slice (2024-01-15, FR/DE/ES) must be non-empty.
# If this prints 0, your dataset is stale — re-run THIS cell.
sanity = (transactions
          .filter(F.col("transaction_date") == "2024-01-15")
          .filter(F.col("country").isin("FR", "DE", "ES"))
          .count())
print("Sanity check — rows in the report slice (2024-01-15, FR/DE/ES):", sanity)
assert sanity > 0, "Report slice is empty — re-run this generation cell to refresh the data."

print("Sample CSV written to", RAW_CSV_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The non-optimized report
# MAGIC
# MAGIC Goal: total transaction amount per country for **2024-01-15**, limited to FR / DE / ES.

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
          .filter(df["transaction_date"] == "2024-01-15")
          .filter(df["country"].isin("FR", "DE", "ES"))
          .groupBy("country")
          .agg(F.round(F.sum("amount"), 2).alias("total_amount")))

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
