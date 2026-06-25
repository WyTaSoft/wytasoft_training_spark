# UC3 — Reading & Caching a Reused DataFrame (INSTRUCTOR SOLUTION)
#
# Same two reports as w003.py, but:
#   1. explicit schema (no inferSchema scan)
#   2. column pruning (read only product_id, category, client_id, amount)
#   3. cache the filtered DataFrame (reused by both reports) and materialize once
#   4. unpersist when done

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("client_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("date", StringType()),
])

priorityProducts = [
    "P001", "P002", "P003", "P004", "P005",
    "P006", "P007", "P008", "P009", "P010",
]

filteredDf = (
    spark.read
    .option("header", "true")
    .schema(schema)                                   # no inferSchema
    .csv("dbfs:/FileStore/raw_layer/w003/sales/")
    .select("product_id", "category", "client_id", "amount")   # column pruning
    .filter(col("product_id").isin(priorityProducts))
).cache()                                             # reused by both reports
filteredDf.count()                                   # materialize once

# 1. Total sales by category
totalByCategory = (
    filteredDf.groupBy("category")
    .agg(sum("amount").alias("total_sales"))
)

# 2. Top 5 clients by sales
topClients = (
    filteredDf.groupBy("client_id")
    .agg(sum("amount").alias("total_client_sales"))
    .orderBy(desc("total_client_sales"))
    .limit(5)
)

totalByCategory.show()
topClients.show()

filteredDf.unpersist()
