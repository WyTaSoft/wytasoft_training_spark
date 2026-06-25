# UC2 — Repeated Computation & an Expensive Join (INSTRUCTOR SOLUTION)
#
# Same suspect-client logic as w002.py, but:
#   1. explicit schema (no inferSchema scan)
#   2. cache the parsed, column-pruned base DataFrame (reused by all 3 rules) and materialize once
#   3. replace the rule-3 self-join with a window (lag) -> linear, one shuffle, no O(n^2)
#   4. unpersist when done

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, TimestampType)

schema = StructType([
    StructField("client_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("transaction_type", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", TimestampType()),
])

# Read once, keep only the columns the three rules use, then cache + materialize.
df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv("dbfs:/FileStore/raw_layer/w002/transactions/")
    .select("client_id", "amount", "transaction_type", "country",
            "latitude", "longitude", "timestamp")
).cache()
df.count()  # one action populates the cache; rules below read from memory

# RULE 1: withdrawals over a rolling 7-day window > €10,000
rule1 = (
    df.filter(F.col("transaction_type") == "withdrawal")
    .withColumn("window_7d", F.window(F.col("timestamp"), "7 days"))
    .groupBy("client_id", "window_7d")
    .agg(F.sum("amount").alias("total_withdrawal"))
    .filter(F.col("total_withdrawal") > 10000)
)

# RULE 2: > 5 foreign transactions in a rolling 3-day window
rule2 = (
    df.filter(F.col("country") != "FR")
    .withColumn("window_3d", F.window(F.col("timestamp"), "3 days"))
    .groupBy("client_id", "window_3d")
    .agg(F.count("*").alias("nb_tx"))
    .filter(F.col("nb_tx") > 5)
)

# RULE 3: impossible travel — > 1000 km vs the PREVIOUS transaction in < 2 hours.
# Window + lag instead of a self-join: one shuffle, linear in rows.
w = Window.partitionBy("client_id").orderBy("timestamp")

moves = (
    df.select("client_id", "timestamp", "latitude", "longitude")
    .withColumn("prev_lat", F.lag("latitude").over(w))
    .withColumn("prev_lon", F.lag("longitude").over(w))
    .withColumn("prev_ts", F.lag("timestamp").over(w))
    .filter(F.col("prev_ts").isNotNull())
    .withColumn("elapsed_s",
                F.unix_timestamp("timestamp") - F.unix_timestamp("prev_ts"))
)

# Haversine distance (km) between consecutive points.
dlat = F.radians(F.col("latitude") - F.col("prev_lat"))
dlon = F.radians(F.col("longitude") - F.col("prev_lon"))
a = (F.pow(F.sin(dlat / 2), 2)
     + F.cos(F.radians(F.col("prev_lat"))) * F.cos(F.radians(F.col("latitude")))
     * F.pow(F.sin(dlon / 2), 2))
distance_km = F.lit(6371.0) * F.lit(2) * F.atan2(F.sqrt(a), F.sqrt(F.lit(1) - a))

rule3 = (
    moves
    .withColumn("distance", distance_km)
    .filter((F.col("elapsed_s") < 7200) & (F.col("distance") > 1000))
    .select("client_id")
    .distinct()
)

# MERGE ALL SUSPICIOUS CLIENTS
suspects = (
    rule1.select("client_id")
    .union(rule2.select("client_id"))
    .union(rule3.select("client_id"))
    .distinct()
)

suspects.write.mode("overwrite").csv("dbfs:/FileStore/bronze_layer/suspects")

df.unpersist()
