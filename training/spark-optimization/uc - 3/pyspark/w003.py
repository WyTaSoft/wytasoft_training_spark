from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc

# Create Spark session if needed
spark = SparkSession.builder.getOrCreate()

# Load the sales file
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("dbfs:/FileStore/raw_layer/w003/sales/")
)

# List of priority products
priorityProducts = [
    "P001", "P002", "P003", "P004", "P005",
    "P006", "P007", "P008", "P009", "P010"
]

# Filter for priority products
filteredDf = df.filter(col("product_id").isin(priorityProducts))

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

# Display results
totalByCategory.show()
topClients.show()
