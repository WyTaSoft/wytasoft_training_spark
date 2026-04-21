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
 * Entry point of the Java Spark training application — Unit 01.
 *
 * <p><strong>Learning objectives for ud01:</strong>
 * <ol>
 *   <li>Understand how to bootstrap a {@link SparkSession} in Java.</li>
 *   <li>Read a CSV file with an explicit schema.</li>
 *   <li>Display the resulting {@link Dataset} using {@code show()}.</li>
 * </ol>
 *
 * <p>This is the Java equivalent of the Scala {@code MainDriver} object.
 * In Java there are no singleton objects: the entry point is a plain class
 * with a {@code public static void main(String[] args)} method.
 *
 * <p><strong>Expected program arguments:</strong>
 * <ul>
 *   <li>{@code args[0]} — environment identifier (e.g. {@code "dev"}, {@code "test"}, {@code "prod"}).</li>
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
        // In Java there is no implicit parameter passing; we resolve the
        // environment here and forward it explicitly to every collaborator.
        String env = args[0];

        // -------------------------------------------------------------------
        // Step 2 — Create the SparkSession
        // -------------------------------------------------------------------
        // SparkSessionManager centralises all Spark configuration so that
        // MainDriver stays focused on orchestration.
        SparkSession sparkSession = SparkSessionManager.fetchSparkSession(
                PrimaryConstants.APPLICATION_NAME
        );

        logger.info("\n\n****  training job has started ... ****\n\n");

        // -------------------------------------------------------------------
        // Step 3 — Read source data
        // -------------------------------------------------------------------
        // PrimaryReader encapsulates the reading logic and the schema definition,
        // keeping MainDriver free of low-level Spark API calls.
        PrimaryReader primaryReader = new PrimaryReader(sparkSession, env);

        Dataset<Row> clients = primaryReader.readClients();

        // show() prints the first 20 rows to stdout — useful for local development.
        clients.show();

        // -------------------------------------------------------------------
        // Step 4 — Stop the SparkSession
        // -------------------------------------------------------------------
        // Always stop the session explicitly to release resources and flush logs.
        sparkSession.stop();
    }
}
