package com.wts.kayan.app.rdd

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.wts.kayan.app.utility.{PrimaryConstants, PrimaryUtilities}
import com.wts.kayan.sessionmanager.SparkSessionManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/** A single client record parsed from clients.csv (clientId,name,location). */
case class Client(clientId: Int, name: String, location: String)

/** A single order record parsed from orders.csv (orderId,clientId,amount); date comes from the partition. */
case class Order(orderId: Int, clientId: Int, amount: Double, date: String)

/** An order enriched with its client's details. Client fields are optional (left join may not match). */
case class EnrichedOrder(clientId: Int,
                         name: Option[String],
                         location: Option[String],
                         orderId: Int,
                         amount: Double,
                         date: String)

/**
 * RDD-based counterpart of the DataFrame/SQL pipeline (see `PrimaryMapper` / `PrimaryView`).
 *
 * It reads the same clients and orders CSV data, parses it into typed RDDs, and joins orders with
 * clients on `clientId` (left outer, keeping every order). Two join strategies are demonstrated:
 *
 *   1. [[broadcastJoin]] - a map-side join that broadcasts the small clients dataset to every
 *      executor. This mirrors the `/*+ BROADCAST(c) */` hint used in the SQL version and avoids a
 *      shuffle.
 *   2. [[shuffleJoin]] - the classic `RDD.leftOuterJoin`, which co-partitions both sides by key
 *      (incurs a shuffle). Shown as a reference / alternative.
 *
 * Run with the same arguments as `MainDriver`: `<env> [configPath]`,
 * e.g. `dev localRun/application.conf`.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more on Mehdi's Spark courses.</a>
 */
object RddClientOrderExample {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty || args(0).trim.isEmpty) {
      throw new IllegalArgumentException(
        "Missing required argument <env>. Usage: RddClientOrderExample <env> [configPath] " +
          "(e.g. dev localRun/application.conf)")
    }

    val env = args(0).trim

    val config: Config =
      if (args.length > 1 && args(1).trim.nonEmpty) {
        val configFile = new File(args(1).trim)
        if (!configFile.exists()) {
          throw new IllegalArgumentException(
            s"Config file not found: ${configFile.getAbsolutePath} " +
              s"(JVM working directory: ${new File(".").getAbsolutePath}).")
        }
        ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load()).resolve()
      } else {
        ConfigFactory.load()
      }

    implicit val spark: SparkSession =
      SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    logger.info("\n\n**** RDD clients/orders example started ... ****\n\n")

    // Resolve input directories from config (same keys as the DataFrame pipeline).
    val clientsDir = config.getString(s"training_spark.inputs.${PrimaryConstants.CLIENTS}.$env")
    val ordersDir = config.getString(s"training_spark.inputs.${PrimaryConstants.ORDERS}.$env")

    // Orders are partitioned by date=; read only the most recent partition (as the DataFrame path does).
    val latestDate = PrimaryUtilities.getMaxPartition(ordersDir)(spark)
    val ordersPartitionDir = s"${ordersDir}date=$latestDate/"

    val clients: RDD[Client] = readClients(clientsDir)
    val orders: RDD[Order] = readOrders(ordersPartitionDir, latestDate)

    val enriched = broadcastJoin(orders, clients)

    val results = enriched.collect()
    logger.info(s"\n**** Enriched ${results.length} orders (broadcast join) ****\n")
    results.foreach(r => logger.info(formatRow(r)))

    spark.stop()
  }

  /**
   * Reads and parses clients.csv into an RDD of [[Client]], skipping the header row.
   *
   * @param path Directory (or file) containing the clients CSV.
   */
  def readClients(path: String)(implicit spark: SparkSession): RDD[Client] =
    spark.sparkContext
      .textFile(path)
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(!_.startsWith("clientId")) // drop header
      .map { line =>
        val cols = line.split(",", -1)
        Client(cols(0).toInt, cols(1), cols(2))
      }

  /**
   * Reads and parses orders.csv into an RDD of [[Order]], skipping the header row.
   * The order date is taken from the partition value rather than the file contents.
   *
   * @param path Directory (or file) of the orders partition to read.
   * @param date The partition date attached to every order.
   */
  def readOrders(path: String, date: String)(implicit spark: SparkSession): RDD[Order] =
    spark.sparkContext
      .textFile(path)
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(!_.startsWith("orderId")) // drop header
      .map { line =>
        val cols = line.split(",", -1)
        Order(cols(0).toInt, cols(1).toInt, cols(2).toDouble, date)
      }

  /**
   * Map-side (broadcast) left join: broadcasts the small clients dataset and looks up each order's
   * client locally on the executor. No shuffle. Mirrors the SQL `/*+ BROADCAST(c) */` hint.
   *
   * @return one [[EnrichedOrder]] per order; client fields are `None` when there is no matching client.
   */
  def broadcastJoin(orders: RDD[Order], clients: RDD[Client])(implicit spark: SparkSession): RDD[EnrichedOrder] = {
    val clientsById: Map[Int, Client] = clients.map(c => c.clientId -> c).collectAsMap().toMap
    val broadcastClients: Broadcast[Map[Int, Client]] = spark.sparkContext.broadcast(clientsById)

    orders.map { o =>
      val client = broadcastClients.value.get(o.clientId)
      EnrichedOrder(
        clientId = o.clientId,
        name = client.map(_.name),
        location = client.map(_.location),
        orderId = o.orderId,
        amount = o.amount,
        date = o.date
      )
    }
  }

  /**
   * Shuffle-based left join using `RDD.leftOuterJoin`. Co-partitions both sides by `clientId`.
   * Functionally equivalent to [[broadcastJoin]]; kept as a reference for when the right side is large.
   */
  def shuffleJoin(orders: RDD[Order], clients: RDD[Client]): RDD[EnrichedOrder] = {
    val ordersByClient = orders.keyBy(_.clientId)
    val clientsByClient = clients.keyBy(_.clientId)

    ordersByClient
      .leftOuterJoin(clientsByClient) // RDD[(clientId, (Order, Option[Client]))]
      .map { case (clientId, (o, client)) =>
        EnrichedOrder(
          clientId = clientId,
          name = client.map(_.name),
          location = client.map(_.location),
          orderId = o.orderId,
          amount = o.amount,
          date = o.date
        )
      }
  }

  /** Formats an enriched order for logging, rendering missing client fields as "N/A". */
  private def formatRow(r: EnrichedOrder): String =
    f"clientId=${r.clientId} name=${r.name.getOrElse("N/A")} location=${r.location.getOrElse("N/A")} " +
      f"orderId=${r.orderId} amount=${r.amount}%.2f date=${r.date}"
}
