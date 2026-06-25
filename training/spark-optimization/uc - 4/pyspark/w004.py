from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SkewTraining").getOrCreate()

transactions = spark.read.csv("dbfs:/training/transactions/", header=True, inferSchema=True)
clients = spark.read.csv("dbfs:/training/clients/", header=True, inferSchema=True)

transactions = transactions.withColumn("year", year(col("timestamp")))

total_amount_per_client = (
    transactions
    .groupBy("client_id")
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("tx_count")
    )
)

joined_df = (
    total_amount_per_client
    .join(clients, "client_id", "left")
)

amount_by_country = (
    joined_df
    .groupBy("country")
    .agg(
        sum("total_amount").alias("country_total_amount"),
        avg("tx_count").alias("avg_tx_per_client")
    )
)

amount_by_country.write.mode("overwrite").parquet("dbfs:/training/output/amount_by_country")
