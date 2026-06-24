# Databricks notebook source
# MAGIC %md
# MAGIC # UC03 — Caching & Repeated Computation (STARTER — NOT OPTIMIZED)
# MAGIC
# MAGIC This notebook works but is deliberately inefficient. The **same derived DataFrame** is
# MAGIC reused for several reports, and because nothing is cached Spark **recomputes the whole
# MAGIC pipeline from the source every time**.
# MAGIC
# MAGIC Read the use-case description in `README.md` first, then open the **Spark UI**
# MAGIC (Jobs tab + Storage tab) while you run the cells to count the repeated scans.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Generate a sample dataset (run once)

# COMMAND ----------

from pyspark.sql import functions as F

RAW_CSV_PATH = "/tmp/training/uc03/transactions_csv"
N_ROWS = 20_000_000

countries = ["FR", "DE", "ES", "IT", "US", "UK", "MA", "NL", "BE", "PT"]

# Categorical columns are drawn with rand() so they are independent of the date.
transactions = (
    spark.range(0, N_ROWS)
    .withColumn("transaction_id", F.col("id"))
    .withColumn("transaction_date", F.date_add(F.lit("2024-01-01"), (F.col("id") % 90).cast("int")))
    .withColumn("country", F.element_at(F.array(*[F.lit(c) for c in countries]),
                                        (F.floor(F.rand(seed=1) * len(countries)) + 1).cast("int")))
    .withColumn("customer_id", (F.col("id") % 1_000_000).cast("long"))
    .withColumn("amount", F.round(F.rand(seed=42) * 5000 + 1, 2))
    .withColumn("currency", F.element_at(
        F.array(F.lit("EUR"), F.lit("USD"), F.lit("GBP")),
        (F.floor(F.rand(seed=2) * 3) + 1).cast("int")))
    .withColumn("channel", F.element_at(
        F.array(F.lit("ATM"), F.lit("ONLINE"), F.lit("BRANCH"), F.lit("MOBILE")),
        (F.floor(F.rand(seed=4) * 4) + 1).cast("int")))
    .withColumn("status", F.element_at(
        F.array(F.lit("POSTED"), F.lit("PENDING"), F.lit("REVERSED")),
        (F.floor(F.rand(seed=5) * 3) + 1).cast("int")))
    .drop("id"))

(transactions.write.mode("overwrite").option("header", "true").csv(RAW_CSV_PATH))
print("Sample CSV written to", RAW_CSV_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A moderately expensive shared transform
# MAGIC
# MAGIC Read + clean + derive columns, then keep only POSTED transactions for January 2024.
# MAGIC Every report below starts from this `clean` DataFrame.

# COMMAND ----------

raw = (spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .csv(RAW_CSV_PATH))

# Some non-trivial derivation (uppercasing, EUR conversion, a derived bucket).
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

# (!) `clean` is NOT cached. Each action below recomputes it from the CSV source.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Several reports — each one re-runs the whole pipeline

# COMMAND ----------

# (!) Action 1: full scan + transform
print("Clean row count:", clean.count())

# COMMAND ----------

# (!) Action 2: full scan + transform AGAIN
total = clean.agg(F.round(F.sum("amount_eur"), 2).alias("total_eur"))
display(total)

# COMMAND ----------

# (!) Action 3: full scan + transform AGAIN
by_country = (clean.groupBy("country")
             .agg(F.round(F.sum("amount_eur"), 2).alias("total_eur"))
             .orderBy("country"))
display(by_country)

# COMMAND ----------

# (!) Action 4: full scan + transform AGAIN
by_channel = (clean.groupBy("channel")
             .agg(F.round(F.sum("amount_eur"), 2).alias("total_eur"))
             .orderBy("channel"))
display(by_channel)

# COMMAND ----------

# (!) Action 5 & 6: two more full scans just for debug sanity checks
print("Large transactions:", clean.filter(F.col("amount_bucket") == "L").count())
print("Distinct customers:", clean.select("customer_id").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Measure
# MAGIC
# MAGIC In the **Spark UI → Jobs** tab, count how many jobs ran and how many times the CSV was
# MAGIC scanned (look at the SQL scan nodes). The **Storage** tab is empty — nothing is cached.
# MAGIC
# MAGIC Now optimize: compute the shared pipeline once. Keep every report's output identical.
