// Package declaration which organizes the code modules, similar to a folder structure.
package com.wts.kayan.sessionmanager

// Importing SparkSession from Spark SQL library.
import org.apache.spark.sql.SparkSession

/**
 * Singleton object to manage Spark Session creation.
 *
 * @author Mehdi TAJMOUATI
 * @note Big Data/Cloud Trainer at WyTaSoft.
 *       For queries, training or further information, contact mehdi.tajmouati@wytasoft.com
 *       This design ensures that only one instance of SparkSession is used throughout the application.
 *       The SparkSession is configured with enhanced settings for performance and efficiency.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on courses and training sessions.</a>
 */
object SparkSessionManager {

  /**
   * Fetches or creates a SparkSession with specified application name and configuration settings.
   *
   * @param appName The name of the application. This name will be displayed in the Spark UI.
   * @return An instance of SparkSession tailored for the application, ensuring optimal performance.
   */
  def fetchSparkSession(appName: String): SparkSession = {

    // Builder pattern to construct a SparkSession with specific configurations.
    SparkSession
      .builder()
      .appName(appName)                                         // Application name in Spark UI
      .master("local[*]")                                       // Run locally on all cores
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.broadcastTimeout", 1200)
      .config("spark.sql.warehouse.dir",  "file:///tmp/spark-warehouse")
      .config("hive.metastore.warehouse.dir", "file:///tmp/spark-warehouse")
      .config("spark.driver.bindAddress", "0.0.0.0")  // listen on all interfaces
      .config("spark.driver.host", "127.0.0.1")      // advertise localhost to the UI
      .config("spark.ui.enabled", "true")           // (default) ensure UI is on
      .config("spark.ui.port", "4040")              // pick a fixed port
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir","file:///tmp/spark-events")
      .enableHiveSupport()                                      // Hive support with local warehouse
      .getOrCreate()
    // The getOrCreate method is critical for managing resource efficiency and ensuring application stability.
  }

}