import org.apache.spark.sql.functions._

// Load the sales file
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("dbfs:/FileStore/raw_layer/w003/sales/")

// List of priority products
val priorityProducts = List(
  "P001", "P002", "P003", "P004", "P005",
  "P006", "P007", "P008", "P009", "P010"
)

// Filter only priority products
val filteredDf = df.filter(col("product_id").isin(priorityProducts: _*))

// 1. Total sales by category
val totalByCategory = filteredDf
  .groupBy("category")
  .agg(sum("amount").alias("total_sales"))

// 2. Top 5 clients by sales
val topClients = filteredDf
  .groupBy("client_id")
  .agg(sum("amount").alias("total_client_sales"))
  .orderBy(desc("total_client_sales"))
  .limit(5)

// Display results
display(totalByCategory)
display(topClients)
