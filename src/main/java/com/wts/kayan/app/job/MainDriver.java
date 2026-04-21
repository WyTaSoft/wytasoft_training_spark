package com.wts.kayan.app.job;

import com.wts.kayan.app.common.PrimaryRunner;
import com.wts.kayan.app.reader.PrimaryReader;
import com.wts.kayan.app.utility.PrimaryConstants;
import com.wts.kayan.app.writer.PrimaryWriter;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Java Spark training application — Unit 04.
 *
 * <p><strong>Learning objectives for ud04:</strong>
 * <ol>
 *   <li>Complete the <strong>full ETL pipeline</strong>: Extract → Transform → Load.</li>
 *   <li>Introduce {@link PrimaryWriter} — the Load step that persists the enriched
 *       Dataset to Parquet, partitioned by {@code location}.</li>
 *   <li>Understand Spark write options: save mode ({@code overwrite} / {@code append}),
 *       {@code coalesce} for controlling output file count, and {@code partitionBy} for
 *       organising data on disk by a business key.</li>
 * </ol>
 *
 * <p><strong>Complete pipeline — ud04:</strong>
 * <pre>
 *   PrimaryReader  ──►  PrimaryRunner  ──►  PrimaryMapper  ──►  PrimaryWriter
 *     (Extract)         (Orchestrate)        (Transform)            (Load)
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
        // Step 3 — EXTRACT: load clients and orders from CSV
        // -------------------------------------------------------------------
        // PrimaryReader extends SchemaSelector (ud02):
        //   - clientsSchema() / ordersSchema() define the column types.
        //   - getDataframe() triggers lazy loading on first access.
        //   - Orders are loaded from the most recent date= partition only.
        PrimaryReader primaryReader = new PrimaryReader(sparkSession, env);

        // -------------------------------------------------------------------
        // Step 4 — TRANSFORM: join + window function via SQL
        // -------------------------------------------------------------------
        // PrimaryRunner (ud03) wires the reader to PrimaryMapper:
        //   1. Fetches clients and orders DataFrames from PrimaryReader.
        //   2. Registers them as Spark SQL temp views.
        //   3. Executes the SQL in PrimaryView (BROADCAST join + SUM OVER PARTITION BY).
        PrimaryRunner primaryRunner  = new PrimaryRunner(primaryReader, sparkSession);
        Dataset<Row>  enriched       = primaryRunner.runPrimaryRunner();

        // Show the enriched result — useful for verifying the transformation in training.
        enriched.show();

        // -------------------------------------------------------------------
        // Step 5 — LOAD: persist the enriched Dataset to Parquet (ud04)
        // -------------------------------------------------------------------
        // PrimaryWriter delegates to PrimaryUtilities.writeDataFrame():
        //   - coalesce(2)       — two output files (appropriate for small training data).
        //   - format("parquet") — columnar storage, compressed by default.
        //   - partitionBy("location") — organises output folders by client location.
        //   - mode(overwrite)   — replaces any existing data at the output path.
        PrimaryWriter primaryWriter = new PrimaryWriter();
        primaryWriter.write(enriched, PrimaryConstants.MODE_OVERWRITE, 2, env);

        // -------------------------------------------------------------------
        // Step 6 — Stop the SparkSession
        // -------------------------------------------------------------------
        sparkSession.stop();
    }
}
