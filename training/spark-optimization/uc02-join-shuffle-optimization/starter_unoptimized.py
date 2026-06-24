# Databricks notebook source
# MAGIC %md
# MAGIC # UC02 — Joins & Shuffles (STARTER — NOT OPTIMIZED) — Spark 3.5
# MAGIC
# MAGIC This notebook works but is deliberately inefficient. Your job is to optimize the
# MAGIC **join** while keeping the final report identical.
# MAGIC
# MAGIC Read the use-case description in `README.md` first, then open the **Spark UI**
# MAGIC (SQL plan + Stages) while you run the cells to see the join strategy and the skew.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Generate sample data (run once)
# MAGIC
# MAGIC A large **transactions** fact table and a small **customers** dimension.
# MAGIC We deliberately inject **skew**: one hot customer + some NULL keys.

# COMMAND ----------

from pyspark.sql import functions as F

TX_PATH = "/tmp/training/uc02/transactions_delta"
CUST_PATH = "/tmp/training/uc02/customers_delta"

N_TX = 20_000_000        # fact rows
N_CUST = 200_000         # dimension rows
HOT_CUSTOMER_ID = 0      # this id will be massively over-represented (skew)

countries = ["FR", "DE", "ES", "IT", "US", "UK", "MA", "NL", "BE", "PT"]

# --- customers dimension (small) ---
customers = (
    spark.range(0, N_CUST)
    .withColumn("customer_id", F.col("id"))
    .withColumn("customer_name", F.concat(F.lit("customer_"), F.col("id")))
    .withColumn("segment", F.element_at(
        F.array(F.lit("RETAIL"), F.lit("PREMIUM"), F.lit("PRIVATE"), F.lit("CORPORATE")),
        (F.col("id") % 4 + 1).cast("int")))
    .withColumn("home_country", F.element_at(F.array(*[F.lit(c) for c in countries]),
                                             (F.col("id") % len(countries) + 1).cast("int")))
    .withColumn("risk_score", F.round(F.rand(seed=7) * 100, 1))
    .drop("id"))

customers.write.mode("overwrite").format("delta").save(CUST_PATH)

# --- transactions fact (large, skewed) ---
# 70% of rows go to the hot customer, 5% have a NULL customer_id, the rest are spread out.
transactions = (
    spark.range(0, N_TX)
    .withColumn("transaction_id", F.col("id"))
    .withColumn("rnd", F.rand(seed=13))
    .withColumn("customer_id",
                F.when(F.col("rnd") < 0.70, F.lit(HOT_CUSTOMER_ID))
                 .when(F.col("rnd") < 0.75, F.lit(None).cast("long"))
                 .otherwise((F.col("id") % N_CUST).cast("long")))
    .withColumn("transaction_date", F.date_add(F.lit("2024-01-01"), (F.col("id") % 90).cast("int")))
    .withColumn("amount", F.round(F.rand(seed=42) * 5000 + 1, 2))
    .withColumn("currency", F.element_at(
        F.array(F.lit("EUR"), F.lit("USD"), F.lit("GBP")), (F.col("id") % 3 + 1).cast("int")))
    .withColumn("transaction_type", F.element_at(
        F.array(F.lit("DEBIT"), F.lit("CREDIT"), F.lit("TRANSFER")), (F.col("id") % 3 + 1).cast("int")))
    .withColumn("channel", F.element_at(
        F.array(F.lit("ATM"), F.lit("ONLINE"), F.lit("BRANCH"), F.lit("MOBILE")),
        (F.col("id") % 4 + 1).cast("int")))
    .withColumn("status", F.element_at(
        F.array(F.lit("POSTED"), F.lit("PENDING"), F.lit("REVERSED")), (F.col("id") % 3 + 1).cast("int")))
    .drop("id", "rnd"))

transactions.write.mode("overwrite").format("delta").save(TX_PATH)

print("Sample data written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The non-optimized join
# MAGIC
# MAGIC Goal: total transaction amount per customer **segment** for **January 2024**.

# COMMAND ----------

# (!) Force the "naive" setup so the inefficiency is visible:
#     - AQE OFF (in 3.5 it's ON by default and would help a lot)
#     - broadcast DISABLED (forces a Sort-Merge Join that shuffles BOTH sides)
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.shuffle.partitions", "200")  # default; way too many for this data

tx = spark.read.format("delta").load(TX_PATH)
cust = spark.read.format("delta").load(CUST_PATH)

# COMMAND ----------

# (!) Debug action in the hot path — extra job.
print("Transactions:", tx.count(), "Customers:", cust.count())

# COMMAND ----------

# (!) Join FIRST (full big-side shuffle), keep ALL columns, NULL keys still present,
#     then filter and aggregate. The hot customer makes one task run forever.
joined = tx.join(cust, on="customer_id", how="inner")

report = (joined
          .filter((F.col("transaction_date") >= "2024-01-01") &
                  (F.col("transaction_date") <= "2024-01-31"))
          .groupBy("segment")
          .agg(F.round(F.sum("amount"), 2).alias("total_amount")))

# COMMAND ----------

# (!) Another full action.
print("Rows in report:", report.count())
display(report.orderBy("segment"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Measure
# MAGIC
# MAGIC In the **Spark UI** note:
# MAGIC - the join node type (Sort-Merge Join) in the SQL tab,
# MAGIC - shuffle read/write bytes in the Stages tab,
# MAGIC - the **task time skew**: max task time vs median (the hot customer),
# MAGIC - the number of shuffle partitions (200, mostly tiny).
# MAGIC
# MAGIC Now optimize. Keep the final `display(report...)` output identical.
# MAGIC
# MAGIC > Tip: run `joined.explain(True)` to see the chosen join strategy.
