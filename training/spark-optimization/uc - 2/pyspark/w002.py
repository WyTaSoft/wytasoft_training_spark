from pyspark.sql.functions import *

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("dbfs:/FileStore/raw_layer/w002/transactions/")
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
)

# RULE 1: withdrawals over 7 days > €10,000
withdraw_df = df.filter(col("transaction_type") == "withdrawal")
withdraw_with_window = withdraw_df.withColumn("window_7d", window(col("timestamp"), "7 days"))
rule1 = (
    withdraw_with_window.groupBy("client_id", "window_7d")
    .agg(sum("amount").alias("total_withdrawal"))
    .filter(col("total_withdrawal") > 10000)
)

# RULE 2: >5 foreign transactions in 3 days
foreign_df = df.filter(col("country") != "FR")
foreign_with_window = foreign_df.withColumn("window_3d", window(col("timestamp"), "3 days"))
rule2 = (
    foreign_with_window.groupBy("client_id", "window_3d")
    .agg(count("*").alias("nb_tx"))
    .filter(col("nb_tx") > 5)
)

# RULE 3: >1000 km movement in <2 hours
df2 = df.select("client_id", "timestamp", "latitude", "longitude") \
    .withColumnRenamed("timestamp", "ts")

a = df2.alias("a")
b = df2.alias("b")

joined = (
    a.join(b, (col("a.client_id") == col("b.client_id")) & (col("a.ts") < col("b.ts")))
    .filter(unix_timestamp(col("b.ts")) - unix_timestamp(col("a.ts")) < 7200)
    .withColumn(
        "distance",
        lit(6371.0) * lit(2) * atan2(
            sqrt(
                pow(sin(radians(col("b.latitude") - col("a.latitude")) / 2), 2) +
                cos(radians(col("a.latitude"))) * cos(radians(col("b.latitude"))) *
                pow(sin(radians(col("b.longitude") - col("a.longitude")) / 2), 2)
            ),
            sqrt(
                1 - (
                        pow(sin(radians(col("b.latitude") - col("a.latitude")) / 2), 2) +
                        cos(radians(col("a.latitude"))) * cos(radians(col("b.latitude"))) *
                        pow(sin(radians(col("b.longitude") - col("a.longitude")) / 2), 2)
                )
            )
        )
    )
    .filter(col("distance") > 1000)
)

rule3 = joined.select(col("a.client_id").alias("client_id")).distinct()

# MERGE ALL SUSPICIOUS CLIENTS
suspects = (
    rule1.select("client_id")
    .union(rule2.select("client_id"))
    .union(rule3.select("client_id"))
    .distinct()
)

# SAVE
suspects.write.mode("overwrite").csv("dbfs:/FileStore/bronze_layer/suspects")
