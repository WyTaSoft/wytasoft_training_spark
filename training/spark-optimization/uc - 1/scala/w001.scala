import org.apache.spark.sql.functions._

// Read the CSV file from DBFS
val df = spark.read
  .format("csv")
  .option("header", "true")
  .load("dbfs:/FileStore/raw_layer/transactions/")

// Filter rows where "amount" is not empty
val filteredDf = df.filter(col("amount") =!= "")

// Cast "amount" column to Double
val castedDf = filteredDf.withColumn("amount", col("amount").cast("double"))

// Aggregate by client_id and transaction_type
val groupedDf = castedDf
  .groupBy("client_id", "transaction_type")
  .agg(sum("amount").as("total_amount"))

// Write the result in Parquet format
groupedDf.write
  .format("parquet")
  .mode("overwrite")
  .save("dbfs:/FileStore/bronze_layer/transactions_by_client")
