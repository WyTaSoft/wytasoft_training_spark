# UC4 — Skewed Join & Broadcast (INSTRUCTOR SOLUTION)
#
# Corrected + optimized version of w004.py:
#   1. explicit schema (no inferSchema scan); dropped the unused `year` derivation
#   2. BUG FIX: group by `region` (from clients) -- the original grouped by a non-existent `country`
#   3. broadcast the small clients dimension -> Broadcast Hash Join, no big-side shuffle
#   4. AQE on (Spark 3.5 default) for skew handling on the per-client aggregation
#
# Skew note: C00001 / C00002 dominate the data. sum()/count() are combinable, so Spark does a
# partial (map-side) aggregation before the shuffle, which already absorbs most of the skew in the
# per-client groupBy. The remaining shuffle (the join) is removed entirely by broadcasting clients.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, broadcast
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, TimestampType)

spark = SparkSession.builder.appName("SkewTraining").getOrCreate()

# AQE + skew-join are defaults in 3.5; set explicitly to be self-documenting.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

tx_schema = StructType([
    StructField("client_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("transaction_type", StringType()),
    StructField("country", StringType()),
    StructField("timestamp", TimestampType()),
])

clients_schema = StructType([
    StructField("client_id", StringType()),
    StructField("client_name", StringType()),
    StructField("region", StringType()),
])

transactions = (spark.read.option("header", "true").schema(tx_schema)
                .csv("dbfs:/training/transactions/")
                .select("client_id", "amount"))          # only what we aggregate

clients = (spark.read.option("header", "true").schema(clients_schema)
           .csv("dbfs:/training/clients/")
           .select("client_id", "region"))               # only what we report

# Per-client aggregation (partial aggregation absorbs most of the key skew).
total_amount_per_client = (
    transactions
    .groupBy("client_id")
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("tx_count"),
    )
)

# Broadcast the tiny dimension -> no shuffle of the aggregated fact side.
joined_df = total_amount_per_client.join(broadcast(clients), "client_id", "left")

# Report by region (corrected grouping key).
amount_by_region = (
    joined_df
    .groupBy("region")
    .agg(
        sum("total_amount").alias("region_total_amount"),
        avg("tx_count").alias("avg_tx_per_client"),
    )
)

amount_by_region.write.mode("overwrite").parquet("dbfs:/training/output/amount_by_region")
