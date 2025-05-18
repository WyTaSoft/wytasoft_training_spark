package com.wts.kayan.app.utility

/**
 * Object to manage the selection of columns for various tables within the Spark application.
 * This allows for centralized control over which columns are retrieved for specific datasets,
 * ensuring consistency across different parts of the application when accessing data.
 *
 * The object provides a method to retrieve an array of column names based on the table name,
 * simplifying the process of querying data with Spark SQL or DataFrame operations.
 *
 * @author Mehdi TAJMOUATI
 * @note For further inquiries or detailed explanations of column selection strategy,
 *       contact mehdi.tajmouati@wytasoft.com
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information
 *      on Mehdi's Spark courses and training sessions.</a>
 */
object ColumnSelector {

  /**
   * Retrieves a sequence of column names for specified tables.
   * The columns selected are essential for respective tables to ensure that only relevant data is processed.
   *
   * @param tableName The name of the table for which columns need to be fetched.
   *                  The table name should match one of the constants defined in `PrimaryConstants`.
   * @return Array[String] - An array of column names corresponding to the table's requirements.
   */
  def getColumnSequence(tableName: String): Array[String] = {

    tableName.toLowerCase match {

      case PrimaryConstants.ORDERS =>
        Array(
          "orderid",  // Unique identifier for the order.
          "clientid", // Client identifier linking to the client's details.
          "amount",   // Monetary value of the order.
          "date"      // Date the order was placed.

        )

      case PrimaryConstants.CLIENTS =>
        Array(
          "clientid",  // Unique identifier for the client.
          "name",      // Name of the client.
          "location"   // Geographical location of the client.
        )
    }
  }
}