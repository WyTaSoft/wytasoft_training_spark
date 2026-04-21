package com.wts.kayan.app.mapping;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies SQL transformations to the source DataFrames to produce an enriched result.
 *
 * <p>The mapping strategy uses <strong>Spark SQL temporary views</strong>:
 * <ol>
 *   <li>Each input {@link Dataset} is registered as a temporary view in the Spark SQL catalog
 *       via {@code createOrReplaceTempView}.</li>
 *   <li>A SQL query (defined in {@link PrimaryView}) is executed against those views using
 *       {@code sparkSession.sql(...)}, which returns a new {@link Dataset}.</li>
 * </ol>
 *
 * <p>This approach keeps the transformation logic in plain SQL — easy to read, optimised
 * by Spark's Catalyst engine, and independent of the DataFrame API version.
 *
 * <p>This is the Java equivalent of the Scala {@code PrimaryMapper} class. In Scala the
 * SparkSession was passed as an implicit parameter; in Java it is an explicit constructor
 * argument.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class PrimaryMapper {

    private static final Logger log = LoggerFactory.getLogger(PrimaryMapper.class);

    private final Dataset<Row> clients;
    private final Dataset<Row> orders;
    private final SparkSession sparkSession;

    /**
     * Constructs a PrimaryMapper with the two source datasets and the active session.
     *
     * @param clients      Dataset containing client rows.
     * @param orders       Dataset containing order rows (latest partition).
     * @param sparkSession Active SparkSession — needed to call {@code sparkSession.sql(...)}.
     */
    public PrimaryMapper(Dataset<Row> clients, Dataset<Row> orders, SparkSession sparkSession) {
        this.clients      = clients;
        this.orders       = orders;
        this.sparkSession = sparkSession;
    }

    /**
     * Registers both datasets as temporary views and executes the enrichment SQL query.
     *
     * <p>The temporary views ({@code clients_view}, {@code orders_view}) are session-scoped:
     * they live only for the duration of the SparkSession and are never persisted to storage.
     *
     * <p>The SQL in {@link PrimaryView#GET_CLIENT_ORDER_SQL} performs:
     * <ul>
     *   <li>A broadcast LEFT JOIN of orders onto clients on {@code clientId}.</li>
     *   <li>A window function ({@code SUM OVER PARTITION BY clientId}) that adds
     *       the per-client total as a new column without collapsing rows.</li>
     * </ul>
     *
     * @return Enriched {@link Dataset}{@literal <}{@link Row}{@literal >} ready for display or writing.
     */
    public Dataset<Row> enrichDataFrame() {
        log.info("\n**** Registering temporary views and executing SQL enrichment ****\n");

        // Register clients as a temporary view named "clients_view".
        // createOrReplaceTempView replaces any previous view with the same name.
        clients.createOrReplaceTempView("clients_view");

        // Register orders as a temporary view named "orders_view".
        orders.createOrReplaceTempView("orders_view");

        // Execute the SQL query — Catalyst optimises the plan before running it.
        Dataset<Row> enriched = sparkSession.sql(PrimaryView.GET_CLIENT_ORDER_SQL);

        log.info("\n**** SQL enrichment completed ****\n");
        return enriched;
    }
}
