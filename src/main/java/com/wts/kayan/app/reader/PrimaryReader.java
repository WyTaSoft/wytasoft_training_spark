package com.wts.kayan.app.reader;

import com.wts.kayan.app.utility.AppConfig;
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
 * <p><strong>Enhancement vs. previous units:</strong> the constructor now accepts
 * an {@link AppConfig} instead of a raw {@code env} string. All path resolution is
 * delegated to {@code AppConfig} and {@link PrimaryUtilities}, so no path strings
 * appear anywhere in this class.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class PrimaryReader extends SchemaSelector {

    private static final Logger log = LoggerFactory.getLogger(PrimaryReader.class);

    private final SparkSession sparkSession;
    private final AppConfig    appConfig;

    // Lazy backing fields — null until first access via getDataframe().
    private Dataset<Row> clients;
    private Dataset<Row> orders;

    /**
     * Constructs a PrimaryReader backed by the given configuration.
     *
     * @param sparkSession Active Spark session.
     * @param appConfig    Resolved application configuration — provides all paths.
     */
    public PrimaryReader(SparkSession sparkSession, AppConfig appConfig) {
        this.sparkSession = sparkSession;
        this.appConfig    = appConfig;
    }

    // -------------------------------------------------------------------------
    // Lazy-initialised dataset accessors
    // -------------------------------------------------------------------------

    private Dataset<Row> getClients() {
        if (clients == null) {
            clients = PrimaryUtilities.readDataFrame(
                    PrimaryConstants.CLIENTS,
                    clientsSchema(),
                    sparkSession,
                    appConfig          // path resolved from application.conf
            );
            clients.show();
        }
        return clients;
    }

    private Dataset<Row> getOrders() {
        if (orders == null) {
            orders = PrimaryUtilities.readDataFrame(
                    PrimaryConstants.ORDERS,
                    ordersSchema(),
                    true,              // isLastPartition
                    sparkSession,
                    appConfig          // path + partition column resolved from application.conf
            );
            orders.show();
        }
        return orders;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Retrieves a dataset by its logical name.
     *
     * @param input {@link PrimaryConstants#CLIENTS} or {@link PrimaryConstants#ORDERS}
     *              (case-insensitive).
     * @return The requested {@link Dataset}{@literal <}{@link Row}{@literal >}.
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
