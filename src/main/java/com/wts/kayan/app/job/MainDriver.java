package com.wts.kayan.app.job;

import com.wts.kayan.app.common.PrimaryRunner;
import com.wts.kayan.app.reader.PrimaryReader;
import com.wts.kayan.app.utility.PrimaryConstants;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Java Spark training application — Unit 03.
 *
 * <p><strong>Learning objectives for ud03:</strong>
 * <ol>
 *   <li>Introduce the <strong>orchestrator pattern</strong>: {@code PrimaryRunner} coordinates
 *       the reader and the mapper so {@code MainDriver} stays focused on wiring and startup.</li>
 *   <li>Understand <strong>Spark SQL temporary views</strong>: DataFrames are registered as
 *       views ({@code createOrReplaceTempView}) so plain SQL can be run against them.</li>
 *   <li>Use a <strong>BROADCAST join hint</strong> to optimise the client-order join when
 *       the clients table is small enough to fit in executor memory.</li>
 *   <li>Use a <strong>window function</strong> ({@code SUM OVER PARTITION BY clientId}) to
 *       compute per-client totals without collapsing individual order rows.</li>
 * </ol>
 *
 * <p><strong>Pipeline added in ud03:</strong>
 * <pre>
 *   PrimaryReader  ──►  PrimaryRunner  ──►  PrimaryMapper  ──►  enriched Dataset
 *     (ud02)                (new)              (new)               shown here
 * </pre>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class MainDriver {

    private static final Logger logger = LoggerFactory.getLogger(MainDriver.class);

    /**
     * Application entry point.
     *
     * @param args {@code args[0]} — environment identifier (e.g. {@code "dev"}).
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
        // Step 3 — Build the pipeline and run it
        // -------------------------------------------------------------------
        // PrimaryReader (ud02) — loads clients and orders with schema + partition detection.
        PrimaryReader primaryReader = new PrimaryReader(sparkSession, env);

        // PrimaryRunner (ud03) — orchestrates Reader → Mapper and returns the enriched Dataset.
        // Internally it:
        //   1. Calls primaryReader.getDataframe() to retrieve both source DataFrames.
        //   2. Passes them to PrimaryMapper, which registers temp views and runs the SQL.
        PrimaryRunner primaryRunner = new PrimaryRunner(primaryReader, sparkSession);
        Dataset<Row> enriched = primaryRunner.runPrimaryRunner();

        // -------------------------------------------------------------------
        // Step 4 — Display results
        // -------------------------------------------------------------------
        logger.info("\n---- Enriched schema (clients LEFT JOIN orders + window total) ----");
        enriched.printSchema();

        // show() prints the first 20 rows — enough to verify the join and window column.
        enriched.show();

        // -------------------------------------------------------------------
        // Step 5 — Stop the SparkSession
        // -------------------------------------------------------------------
        sparkSession.stop();
    }
}
