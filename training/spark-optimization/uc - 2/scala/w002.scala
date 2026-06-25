import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
object NonOptimizedFraudJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // -----------------------------------------------------
    // Load CSV with schema inference
    // -----------------------------------------------------
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/FileStore/tables/transactions-10.csv")
      .withColumn("timestamp", $"timestamp".cast("timestamp"))

    // -----------------------------------------------------
    // RULE 1 : Withdrawals over 7 days exceeding €10,000
    // (Your dataset has no “withdrawal” type, but rule stays here)
    // -----------------------------------------------------
    val withdrawalDf = df.filter($"transaction_type" === "withdrawal")

    val withdrawalWithWindow =
      withdrawalDf.withColumn("window_7d", window($"timestamp", "7 days"))

    val rule1 = withdrawalWithWindow
      .groupBy("client_id", "window_7d")
      .agg(sum("amount").as("total_withdrawal"))
      .filter($"total_withdrawal" > 10000)

    // -----------------------------------------------------
    // RULE 2 : More than 5 foreign transactions in 3 days
    // -----------------------------------------------------
    val foreignDf = df.filter($"country" =!= "FR")

    val foreignWithWindow =
      foreignDf.withColumn("window_3d", window($"timestamp", "3 days"))

    val rule2 = foreignWithWindow
      .groupBy("client_id", "window_3d")
      .agg(count("*").as("nb_tx"))
      .filter($"nb_tx" > 5)

    // -----------------------------------------------------
    // RULE 3 : Movement greater than 1000 km in < 2 hours
    // -----------------------------------------------------
    val rule3 = df.as("a")
      .join(df.as("b"), $"a.client_id" === $"b.client_id")
      .filter($"a.timestamp" < $"b.timestamp")
      .filter(unix_timestamp($"b.timestamp") - unix_timestamp($"a.timestamp") < 7200)
      .withColumn("distance",
        haversine(
          $"a.latitude", $"a.longitude",
          $"b.latitude", $"b.longitude"
        )
      )
      .filter($"distance" > 1000)
      .select($"a.client_id".alias("client_id"))

    // -----------------------------------------------------
    // MERGE ALL SUSPICIOUS CLIENTS
    // -----------------------------------------------------
    val suspects = rule1.select("client_id")
      .union(rule2.select("client_id"))
      .union(rule3.select("client_id"))
      .distinct()

    // -----------------------------------------------------
    // SAVE RESULTS
    // -----------------------------------------------------
    suspects.write
      .mode("overwrite")
      .csv("output/suspects")
  }

  // -----------------------------------------------------
  // Haversine formula to compute distance (km)
  // -----------------------------------------------------
  def haversine(lat1: Column, lon1: Column, lat2: Column, lon2: Column): Column = {
    val R = 6371.0
    val dLat = radians(lat2 - lat1)
    val dLon = radians(lon2 - lon1)

    val a =
      pow(sin(dLat / 2), 2) +
        cos(radians(lat1)) * cos(radians(lat2)) *
          pow(sin(dLon / 2), 2)

    val c = atan2(sqrt(a), sqrt(lit(1) - a)) * 2

    lit(R) * c
  }
}
