package com.wts.kayan.app.common;

import com.wts.kayan.app.mapping.PrimaryMapper;
import com.wts.kayan.app.reader.PrimaryReader;
import com.wts.kayan.app.utility.PrimaryConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the data processing pipeline by connecting the reader to the mapper.
 *
 * <p>{@code PrimaryRunner} is the <em>coordinator</em> layer: it knows nothing about how
 * data is read from disk (that is {@link PrimaryReader}'s responsibility) or how SQL is
 * written (that is {@link com.wts.kayan.app.mapping.PrimaryView}'s responsibility).
 * It simply wires the two together and triggers execution.
 *
 * <p>This separation of concerns mirrors the classic ETL split:
 * <pre>
 *   PrimaryReader  →  PrimaryMapper  →  result Dataset
 *       (Extract)        (Transform)
 * </pre>
 *
 * <p>This is the Java equivalent of the Scala {@code PrimaryRunner} class. In Scala the
 * SparkSession was an implicit parameter; in Java it is passed explicitly to the constructor.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class PrimaryRunner {

    private static final Logger log = LoggerFactory.getLogger(PrimaryRunner.class);

    // The reader provides the raw source datasets.
    private final PrimaryReader fetchSource;

    // SparkSession is forwarded to PrimaryMapper so it can call sparkSession.sql(...).
    private final SparkSession sparkSession;

    /**
     * Constructs a PrimaryRunner.
     *
     * @param fetchSource  An initialised {@link PrimaryReader} — the source of raw datasets.
     * @param sparkSession Active SparkSession forwarded to the mapper.
     */
    public PrimaryRunner(PrimaryReader fetchSource, SparkSession sparkSession) {
        this.fetchSource  = fetchSource;
        this.sparkSession = sparkSession;
    }

    /**
     * Runs the full Extract → Transform pipeline and returns the enriched Dataset.
     *
     * <p>Steps:
     * <ol>
     *   <li>Fetch the clients and orders DataFrames from {@link PrimaryReader} via
     *       {@code getDataframe(String)}.</li>
     *   <li>Pass them to {@link PrimaryMapper}, which registers temporary views and
     *       executes the SQL enrichment query.</li>
     *   <li>Return the resulting enriched DataFrame to the caller ({@code MainDriver})
     *       for display or writing.</li>
     * </ol>
     *
     * @return Enriched {@link Dataset}{@literal <}{@link Row}{@literal >} containing
     *         joined client-order data with per-client totals.
     */
    public Dataset<Row> runPrimaryRunner() {
        log.info("\n**** PrimaryRunner — starting data processing pipeline ****\n");

        // Step 1 — Extract: retrieve both source datasets from the reader.
        // getDataframe() triggers lazy loading the first time it is called.
        Dataset<Row> clients = fetchSource.getDataframe(PrimaryConstants.CLIENTS);
        Dataset<Row> orders  = fetchSource.getDataframe(PrimaryConstants.ORDERS);

        // Step 2 — Transform: pass both datasets to the mapper.
        // PrimaryMapper registers temp views and runs the SQL enrichment query.
        PrimaryMapper primaryMapper = new PrimaryMapper(clients, orders, sparkSession);
        Dataset<Row> enriched = primaryMapper.enrichDataFrame();

        log.info("\n**** PrimaryRunner — pipeline completed successfully ****\n");
        return enriched;
    }
}
