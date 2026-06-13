package com.wts.kayan.app.utility
/**
 * Object to store primary constants used throughout the Spark application.
 * This centralized management of constants helps ensure consistency and easier maintenance.
 *
 * Constants:
 * - '''APPLICATION_NAME''': Used as the name of the Spark application in the UI and logs.
 * - '''CLIENTS''': Refers to the dataset or table name for clients.
 * - '''ORDERS''': Refers to the dataset or table name for orders.
 *
 * @author Mehdi TAJMOUATI
 * @note For queries or more detailed information on the use of these constants in Spark projects, contact mehdi.tajmouati@wytasoft.com
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Mehdi's Spark courses and training sessions.</a>
 */
object PrimaryConstants {

  /** Application name for Spark UI and log identification. */
  val APPLICATION_NAME = "training_spark"

  /** Dataset or table name for clients. Used for referring to the client data storage in queries. */
  val CLIENTS = "clients"
  /** Dataset or table name for orders. Used for referring to the order data storage in queries. */
  val ORDERS = "orders"
  /** Dataset name for the enriched clients-orders output. Used to resolve the output path from config. */
  val CLIENTS_ORDERS = "clients_orders"

  val MODE_APPEND = "Append"
  val MODE_OVERWRITE = "Overwrite"


}
