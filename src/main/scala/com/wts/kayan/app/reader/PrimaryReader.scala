package com.wts.kayan.app.reader

import com.typesafe.config.Config
import com.wts.kayan.app.utility.{SchemaSelector, PrimaryConstants}
import com.wts.kayan.app.utility.PrimaryUtilities.readDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A class responsible for fetching DataFrames for different entities such as clients and orders
 * within a Spark application. This reader simplifies data access by pre-loading DataFrames based
 * on specified schemas and ensures efficient data handling by leveraging Spark's lazy val features.
 *
 * This class extends `SchemaSelector` to utilize predefined schemas, facilitating consistent
 * and error-free data structure usage throughout the application.
 *
 * @param sparkSession Implicit SparkSession that provides the context for DataFrame operations.
 * @param env Implicit environment string that helps determine the run-time environment for data path resolutions.
 * @param config Implicit Config object to fetch application-specific settings.
 * @author Mehdi TAJMOUATI
 * @note This class uses lazy initialization to ensure that DataFrames are only loaded when needed,
 *       optimizing resource usage and performance.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more on Mehdi's courses and training sessions.</a>
 */
class PrimaryReader()(implicit sparkSession: SparkSession, env: String) extends SchemaSelector {

  // Lazy initialization of DataFrame for clients using predefined schema from SchemaSelector.
  private lazy val clients: DataFrame =
    readDataFrame(PrimaryConstants.CLIENTS, clientsSchema)

  clients.show()

  // Lazy initialization of DataFrame for orders using predefined schema from SchemaSelector.
  private lazy val orders: DataFrame =
    readDataFrame(PrimaryConstants.ORDERS, ordersSchema, isLastPartition = true)

  orders.show()

  /**
   * Retrieves the DataFrame based on a specified input string that identifies the dataset.
   * This method simplifies fetching DataFrames for different datasets by using an identifier string.
   *
   * @param input A string identifier for the dataset, expected values are "CLIENTS" or "ORDERS".
   * @return DataFrame corresponding to the input identifier.
   * @throws IllegalArgumentException if the input does not match expected dataset identifiers.
   */
  def getDataframe(input: String): DataFrame = {
    input.toUpperCase match {
      case "CLIENTS" => clients
      case "ORDERS" => orders
      case _ => throw new IllegalArgumentException(s"Invalid input $input. Expected 'CLIENTS' or 'ORDERS'.")
    }
  }
}