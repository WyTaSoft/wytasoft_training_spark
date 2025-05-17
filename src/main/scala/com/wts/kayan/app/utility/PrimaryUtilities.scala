package com.wts.kayan.app.utility

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

import java.io.{BufferedReader, InputStreamReader, Reader}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Utility object providing methods for interacting with HDFS, reading and writing DataFrames,
 * and handling date conversions, tailored for Spark applications. These utilities facilitate
 * common tasks such as reading data, finding the most recent data partitions, and writing data,
 * thereby simplifying data management tasks.
 *
 * @note Use these utilities to enhance code reusability and maintain clean, efficient operations within Spark jobs.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Spark applications and data processing.</a>
 * @see <a href="https://www.linkedin.com/in/mtajmouati">Mehdi TAJMOUATI's LinkedIn profile</a>
 */
object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Fetches the most recent partition based on a date column from a specified HDFS path.
   * This method is useful for incremental data loading scenarios.
   *
   * @param path The HDFS directory to scan.
   * @param columnPartitioned The partition column, defaulted to 'date'.
   * @param spark The Spark session.
   * @return The most recent partition date as a string, or a far-future date if no partitions exist.
   * @throws ArrayIndexOutOfBoundsException if there is an error reading the partition data.
   */
  def getMaxPartition(path: String, columnPartitioned: String = "date")(
    spark: SparkSession): String = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    try {
      val listOfInsertDates: Array[String] = fs
        .listStatus(new Path(s"$path"))
        .filter(_.isDirectory)
        .map(_.getPath)
        .map(_.toString)
      val filterPartitionFolders =
        listOfInsertDates.filter(_.contains(s"$columnPartitioned="))
      val insertDateStr = filterPartitionFolders.map(_.split(s"$columnPartitioned=")(1))

      if (insertDateStr.length > 1) {
        val dateFormatted =
          insertDateStr.map(d => convertStringToDate(d, "yyyy-MM-dd"))
        val maxDate = dateToStrNdReformat(dateFormatted.max, "yyyy-MM-dd")
        log.info(s"\n**** max $columnPartitioned $maxDate ****\n")
        maxDate
      } else if (insertDateStr.length == 0) {
        log.info(s"\n**** there are no partitions by $columnPartitioned in $path ****\n")
        "2999-01-01"
      } else {
        log.info(s"\n**** max $columnPartitioned ${insertDateStr(0)} ****\n")
        insertDateStr(0)
      }
    } catch {
      case _: Throwable =>
        log.error("Fatal Exception: Check Src-View Data")
        throw new ArrayIndexOutOfBoundsException
    }
  }

  /**
   * Reads a DataFrame from a specified source path using a predefined schema, supporting data partitioning.
   *
   * @param sourceName The identifier for the data source to load.
   * @param schema The schema to apply to the DataFrame.
   * @param isCondition Boolean indicating if a condition should be applied.
   * @param condition The conditional filter to apply, if any.
   * @param sparkSession Implicit SparkSession to handle DataFrame operations.
   * @param env Implicit environment used for building the data path.
   * @param config Implicit Config object for additional settings.
   * @return DataFrame loaded from the specified path.
   */
  def readDataFrame(sourceName: String,
                    schema: StructType,
                    isCondition: Boolean = false,
                    condition: Column = null)
                   (implicit sparkSession: SparkSession, env: String): DataFrame = {

    log.info(s"\n**** Reading file to create DataFrame ****\n")

    // Generate the effective condition
    val effectiveCondition: Column = if (isCondition) condition else lit(true)

    var inputPath: String = "/prd/project/datalake/"
    var tableName = ""

    sourceName match {
      case PrimaryConstants.CLIENTS =>
        tableName = "clients"
      case PrimaryConstants.ORDERS =>
        tableName = "orders"
        //val PartitionedValue = getMaxPartition(s"$inputPath${tableName.toLowerCase}/")(sparkSession)
        //tableName = s"orders/date=$PartitionedValue"
    }

    log.info(s"\n Loading $sourceName from $inputPath${tableName.toLowerCase} ***\n")

    val dataFrame: DataFrame = sparkSession.read
      .schema(schema)
      .csv(s"$inputPath${tableName.toLowerCase}/${tableName.toLowerCase}.csv")
      .selectExpr(ColumnSelector.getColumnSequence(sourceName): _*)
      .where(effectiveCondition)
    dataFrame
  }

  /**
   * Opens a file in HDFS and returns a BufferedReader to read the file's contents.
   *
   * @param filePath The full path to the file in HDFS.
   * @param sc The SparkContext to access Hadoop configurations.
   * @return A BufferedReader that can be used to read the file.
   */
  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  /**
   * Converts a string to a Date object using the specified date format.
   *
   * @param s The date string to convert.
   * @param formatType The format of the date string.
   * @return A Date object representing the parsed date string.
   */
  def convertStringToDate(s: String, formatType: String): Date = {
    val format = new SimpleDateFormat(formatType)
    format.parse(s)
  }

  /**
   * Reformats a Date object to a string using the specified date format.
   *
   * @param date The Date object to reformat.
   * @param format The desired format of the date string.
   * @return A string representing the formatted date.
   */
  def dateToStrNdReformat(date: Date, format: String): String = {
    val df = new SimpleDateFormat(format)
    df.format(date)
  }

  /**
   * Writes a DataFrame to a specified path with a given mode and number of partitions.
   *
   * @param dataFrame The DataFrame to write.
   * @param mode The save mode (e.g., "overwrite", "append").
   * @param numPartition The number of partitions to use when writing the DataFrame.
   * @param env Implicit environment string for the write operation.
   */
  def writeDataFrame(dataFrame: DataFrame,
                     mode: String,
                     numPartition: Int)(implicit env: String): Unit = {

    log.info(s"\n *** Write started (mode: $mode, numPartition: $numPartition) ... ***\n")

    dataFrame
      .coalesce(numPartition)
      .write
      .format("parquet")
      .partitionBy("location")
      .mode(mode)
      .save(s"/$env/project/datalake/clients_orders")

    log.info(s"\n *** Write Completed ... *** \n")
  }
}
