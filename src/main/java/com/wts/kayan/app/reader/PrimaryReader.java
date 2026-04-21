package com.wts.kayan.app.reader;

import com.wts.kayan.app.utility.PrimaryConstants;
import com.wts.kayan.app.utility.PrimaryUtilities;
import com.wts.kayan.app.utility.SchemaSelector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for loading source data into Spark {@link Dataset}s.
 *
 * <p><strong>ud02 changes vs ud01:</strong>
 * <ul>
 *   <li>Extends {@link SchemaSelector} — schemas are no longer defined inline; they come
 *       from the shared abstract base class just like the Scala version extended the
 *       {@code SchemaSelector} trait.</li>
 *   <li>Reads <em>both</em> {@code clients} and {@code orders} — orders are loaded from
 *       the most recent date partition detected at runtime.</li>
 *   <li>Delegates all I/O logic to {@link PrimaryUtilities#readDataFrame} — the reader
 *       is now concerned only with <em>which</em> datasets to load, not <em>how</em>.</li>
 *   <li>Exposes {@link #getDataframe(String)} so orchestrators (e.g. a future
 *       {@code PrimaryRunner}) can retrieve datasets by name without knowing internals.</li>
 * </ul>
 *
 * <p>Datasets are lazy-initialised: Spark only reads from disk when a downstream
 * action ({@code show()}, {@code count()}, a join …) triggers evaluation.
 * In Java we achieve this with {@code null}-guarded fields initialised on first access
 * (the Java equivalent of Scala's {@code lazy val}).
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class PrimaryReader extends SchemaSelector {

    private static final Logger log = LoggerFactory.getLogger(PrimaryReader.class);

    private final SparkSession sparkSession;
    private final String env;

    // Lazy backing fields — null until first access.
    // In Scala these would be: private lazy val clients: DataFrame = ...
    private Dataset<Row> clients;
    private Dataset<Row> orders;

    /**
     * Constructs a PrimaryReader.
     *
     * @param sparkSession Active Spark session — used by PrimaryUtilities for path resolution.
     * @param env          Runtime environment identifier (e.g. {@code "dev"}).
     */
    public PrimaryReader(SparkSession sparkSession, String env) {
        this.sparkSession = sparkSession;
        this.env = env;
    }

    // -------------------------------------------------------------------------
    // Lazy-initialised dataset accessors
    // -------------------------------------------------------------------------

    /**
     * Returns the clients {@link Dataset}, loading it from CSV on first call.
     *
     * <p>The schema comes from {@link SchemaSelector#clientsSchema()} — inherited from
     * the parent abstract class. The actual I/O is delegated to
     * {@link PrimaryUtilities#readDataFrame}.
     */
    private Dataset<Row> getClients() {
        if (clients == null) {
            // clientsSchema() is inherited from SchemaSelector.
            clients = PrimaryUtilities.readDataFrame(
                    PrimaryConstants.CLIENTS,
                    clientsSchema(),   // Inherited schema definition.
                    sparkSession,
                    env
            );
            clients.show(); // Display for training — shows the loaded data immediately.
        }
        return clients;
    }

    /**
     * Returns the orders {@link Dataset} from the most recent date partition, loading it
     * on first call.
     *
     * <p>{@code isLastPartition = true} tells {@link PrimaryUtilities#readDataFrame} to
     * call {@link com.wts.kayan.app.utility.PrimaryUtilities#getMaxPartition} to find the
     * latest {@code date=} folder and attach the date value as a literal column.
     */
    private Dataset<Row> getOrders() {
        if (orders == null) {
            // ordersSchema() is inherited from SchemaSelector.
            orders = PrimaryUtilities.readDataFrame(
                    PrimaryConstants.ORDERS,
                    ordersSchema(),    // Inherited schema definition.
                    true,              // isLastPartition — load only the most recent date folder.
                    sparkSession,
                    env
            );
            orders.show(); // Display for training — shows the loaded data immediately.
        }
        return orders;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Retrieves a dataset by its logical name.
     *
     * <p>This method is the entry point for orchestrators such as {@code PrimaryRunner}
     * that need datasets without caring about how they are loaded.
     *
     * @param input Dataset identifier — {@link PrimaryConstants#CLIENTS} or
     *              {@link PrimaryConstants#ORDERS} (case-insensitive).
     * @return The requested {@link Dataset}{@literal <}{@link Row}{@literal >}.
     * @throws IllegalArgumentException if {@code input} is not a recognised identifier.
     */
    public Dataset<Row> getDataframe(String input) {
        switch (input.toUpperCase()) {
            case "CLIENTS": return getClients();
            case "ORDERS":  return getOrders();
            default:
                throw new IllegalArgumentException(
                        "Invalid input '" + input + "'. Expected 'CLIENTS' or 'ORDERS'."
                );
        }
    }
}
