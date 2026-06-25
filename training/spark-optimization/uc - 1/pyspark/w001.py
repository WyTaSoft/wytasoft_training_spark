from pyspark.sql import functions as F

# Read the CSV file from DBFS
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load("dbfs:/FileStore/raw_layer/transactions/")
)

# Filter rows where "amount" is not empty
filtered_df = df.filter(F.col("amount") != "")

# Cast the "amount" column to Double
casted_df = filtered_df.withColumn("amount", F.col("amount").cast("double"))

# Aggregate by client_id and transaction_type
grouped_df = (
    casted_df.groupBy("client_id", "transaction_type")
    .agg(F.sum("amount").alias("total_amount"))
)

# Write the result in Parquet format
(
    grouped_df.write
    .format("parquet")
    .mode("overwrite")
    .save("dbfs:/FileStore/bronze_layer/transactions_by_client")
)
