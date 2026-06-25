# UC1 — Reading & Basic Aggregation (INSTRUCTOR SOLUTION)
#
# Same output as w001.py, but:
#   1. explicit schema (correct types up front, no string->double cast pass, no inferSchema scan)
#   2. column pruning (read only the 3 columns we use)
#   3. a single typed filter (amount IS NOT NULL) instead of empty-string filter + cast
#   4. coalesce the tiny aggregated result to avoid many small Parquet files

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("id", StringType()),
    StructField("client_id", StringType()),
    StructField("amount", DoubleType()),          # typed directly
    StructField("transaction_type", StringType()),
    StructField("date", StringType()),
])

grouped_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema)                                # no inference, no string casts
    .load("dbfs:/FileStore/raw_layer/transactions/")
    .select("client_id", "transaction_type", "amount")   # column pruning
    .filter(F.col("amount").isNotNull())                 # replaces amount != "" + cast
    .groupBy("client_id", "transaction_type")
    .agg(F.sum("amount").alias("total_amount"))
)

(
    grouped_df
    .coalesce(2)                                   # small result -> avoid tiny-file explosion
    .write
    .format("parquet")
    .mode("overwrite")
    .save("dbfs:/FileStore/bronze_layer/transactions_by_client")
)
