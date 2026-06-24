# Databricks notebook source
# MAGIC %md
# MAGIC # UC02 — Joins & Shuffles (INSTRUCTOR SOLUTION) — Spark 3.5
# MAGIC
# MAGIC One reasonable optimized version. Don't hand this out until the debrief.
# MAGIC The output is identical to the starter; the join is far cheaper and balanced.
# MAGIC
# MAGIC Key ideas applied:
# MAGIC 1. **AQE on** — it's the Spark 3.5 default; it coalesces shuffle partitions, can switch to
# MAGIC    broadcast at runtime, and splits skewed partitions (`skewJoin`).
# MAGIC 2. **Broadcast the small dimension** — no shuffle of the big fact table.
# MAGIC 3. **Shrink the shuffle first** — filter to the month, select only needed columns, and
# MAGIC    drop NULL join keys before the join.
# MAGIC 4. **No debug actions** in the hot path.
# MAGIC 5. **Salting** — shown as an *optional* technique for when AQE skew handling isn't enough.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

TX_PATH = "/tmp/training/uc02/transactions_delta"
CUST_PATH = "/tmp/training/uc02/customers_delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 1 — restore the Spark 3.5 defaults (AQE on, broadcast enabled)
# MAGIC
# MAGIC These are already the defaults in 3.5; we set them explicitly to undo the starter's sabotage
# MAGIC and to be self-documenting.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")                 # default in 3.5
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")        # default in 3.5
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB default

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 2 + 3 — prune & filter before the join, broadcast the dimension

# COMMAND ----------

# Big side: keep only what we need, filter the month, drop NULL keys early.
tx = (spark.read.format("delta").load(TX_PATH)
      .filter((F.col("transaction_date") >= "2024-01-01") &
              (F.col("transaction_date") <= "2024-01-31"))
      .filter(F.col("customer_id").isNotNull())
      .select("customer_id", "amount"))

# Small side: only the columns used in the result.
cust = (spark.read.format("delta").load(CUST_PATH)
        .select("customer_id", "segment"))

# Broadcast the small dimension -> Broadcast Hash Join, no shuffle of the big fact table.
report = (tx.join(broadcast(cust), on="customer_id", how="inner")
          .groupBy("segment")
          .agg(F.round(F.sum("amount"), 2).alias("total_amount")))

display(report.orderBy("segment"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the plan
# MAGIC
# MAGIC Look for `BroadcastHashJoin` (not `SortMergeJoin`) and a `BroadcastExchange` on the small
# MAGIC side. With AQE on you'll also see `AdaptiveSparkPlan` and coalesced partitions.

# COMMAND ----------

report.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional — salting (only if a key is so hot that even AQE struggles)
# MAGIC
# MAGIC Broadcast already removes the big-side shuffle here, so salting isn't needed for THIS report.
# MAGIC But if you had to do a **shuffle join** (e.g. both sides are large) and one key dominated,
# MAGIC salting spreads that hot key across multiple partitions.
# MAGIC
# MAGIC Idea: append a random salt `0..N-1` to the fact-side key, and replicate each dimension row
# MAGIC across the same N salt values, then join on `(customer_id, salt)`.

# COMMAND ----------

SALT_BUCKETS = 16

tx_salted = (spark.read.format("delta").load(TX_PATH)
             .filter((F.col("transaction_date") >= "2024-01-01") &
                     (F.col("transaction_date") <= "2024-01-31"))
             .filter(F.col("customer_id").isNotNull())
             .select("customer_id", "amount")
             .withColumn("salt", (F.rand(seed=1) * SALT_BUCKETS).cast("int")))

# Replicate each dimension row across all salt buckets.
cust_salted = (spark.read.format("delta").load(CUST_PATH)
               .select("customer_id", "segment")
               .withColumn("salt", F.explode(F.array(*[F.lit(i) for i in range(SALT_BUCKETS)]))))

report_salted = (tx_salted.join(cust_salted, on=["customer_id", "salt"], how="inner")
                 .groupBy("segment")
                 .agg(F.round(F.sum("amount"), 2).alias("total_amount")))

display(report_salted.orderBy("segment"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to prove it
# MAGIC
# MAGIC Compare the starter vs this notebook in the **Spark UI**:
# MAGIC - **SQL tab:** `BroadcastHashJoin` instead of `SortMergeJoin`; no exchange on the big side.
# MAGIC - **Stages:** shuffle read/write bytes drop sharply; task times are balanced (max ≈ median).
# MAGIC - **Jobs:** fewer jobs (no debug `count()` calls).
# MAGIC - **Wall-clock:** noticeably lower.
