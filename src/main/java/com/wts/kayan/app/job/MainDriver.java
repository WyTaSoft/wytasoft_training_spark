package com.wts.kayan.app.job;

import com.wts.kayan.app.reader.PrimaryReader;
import com.wts.kayan.app.utility.PrimaryConstants;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Java Spark training application — Unit 02.
 *
 * <p><strong>Learning objectives for ud02:</strong>
 * <ol>
 *   <li>Understand how to define schemas as a reusable abstract class ({@code SchemaSelector})
 *       instead of inline — mirroring the Scala {@code trait} pattern.</li>
 *   <li>Introduce utility methods: {@code readDataFrame} and {@code getMaxPartition} in
 *       {@code PrimaryUtilities}.</li>
 *   <li>Separate column selection into a dedicated {@code ColumnSelector} class.</li>
 *   <li>Load <em>both</em> datasets — clients (full table) and orders (latest partition only).</li>
 *   <li>Expose datasets through a named accessor {@code getDataframe(String)} so the reader
 *       can be shared with future orchestrators ({@code PrimaryRunner} in ud03/ud04).</li>
 * </ol>
 *
 * <p><strong>New program argument:</strong>
 * <ul>
 *   <li>{@code args[0]} — environment identifier (e.g. {@code "dev"}, {@code "test"},
 *       {@code "prod"}).</li>
 * </ul>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class MainDriver {

    private static final Logger logger = LoggerFactory.getLogger(MainDriver.class);

    /**
     * Application entry point.
     *
     * @param args Command-line arguments. {@code args[0]} must be the environment string.
     */
    public static void main(String[] args) {

        // -------------------------------------------------------------------
        // Step 1 — Parse program arguments
        // -------------------------------------------------------------------
        String env = args[0];

        // -------------------------------------------------------------------
        // Step 2 — Create the SparkSession
        // -------------------------------------------------------------------
        SparkSession sparkSession = SparkSessionManager.fetchSparkSession(
                PrimaryConstants.APPLICATION_NAME
        );

        logger.info("\n\n****  training job has started ... ****\n\n");

        // -------------------------------------------------------------------
        // Step 3 — Read source data (clients + orders latest partition)
        // -------------------------------------------------------------------
        // PrimaryReader now extends SchemaSelector (ud02).
        // Calling getDataframe() triggers lazy loading the first time.
        PrimaryReader primaryReader = new PrimaryReader(sparkSession, env);

        // Retrieve clients — full table.
        Dataset<Row> clients = primaryReader.getDataframe(PrimaryConstants.CLIENTS);
        logger.info("\n---- Clients schema ----");
        clients.printSchema();

        // Retrieve orders — most recent date partition only.
        // PrimaryUtilities.getMaxPartition() scans the directory and picks the latest folder.
        Dataset<Row> orders = primaryReader.getDataframe(PrimaryConstants.ORDERS);
        logger.info("\n---- Orders schema ----");
        orders.printSchema();

        // -------------------------------------------------------------------
        // Step 4 — Stop the SparkSession
        // -------------------------------------------------------------------
        sparkSession.stop();
    }
}
