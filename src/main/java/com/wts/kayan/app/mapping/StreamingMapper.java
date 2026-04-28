package com.wts.kayan.app.mapping;

import com.wts.kayan.app.utility.StreamingConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies transformations to the raw streaming log Dataset to produce structured, queryable rows.
 *
 * <p>Mirrors {@link PrimaryMapper} for the batch ETL project. The same two-step strategy is used:
 * <ol>
 *   <li>Transform the input Dataset using the DataFrame API ({@code regexp_extract}).</li>
 *   <li>Register the result as a temporary Spark SQL view, then execute a SQL query against it.</li>
 * </ol>
 *
 * <p><strong>Parsing strategy</strong>: each raw log line ({@code value} column) is split
 * into five fields using {@link functions#regexp_extract} with the pattern defined in
 * {@link StreamingConstants#LOG_PATTERN}. Lines that do not match produce empty strings —
 * these are filtered out by the SQL query in {@link LogView}.
 *
 * <p><strong>Streaming compatibility</strong>: {@code createOrReplaceTempView} is valid on
 * streaming DataFrames. Spark propagates the streaming flag through the SQL query, so the
 * result is still a streaming Dataset that must be consumed via {@code writeStream().start()}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class StreamingMapper {

    private static final Logger log = LoggerFactory.getLogger(StreamingMapper.class);

    private final SparkSession sparkSession;

    /**
     * Constructs a {@code StreamingMapper}.
     *
     * @param sparkSession Active SparkSession — needed to call {@code sparkSession.sql(...)}.
     */
    public StreamingMapper(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * Parses raw log lines into structured fields using {@code regexp_extract}.
     *
     * <p>Each row of the input Dataset must have a {@code value} column ({@code StringType})
     * containing one raw log line. Five new columns are extracted:
     * {@code log_timestamp}, {@code level}, {@code thread}, {@code logger}, {@code message}.
     *
     * <p>This method works identically on both streaming and batch Datasets, which makes it
     * straightforward to unit-test in batch mode.
     *
     * @param rawLogs Streaming (or batch) Dataset with a single {@code value} column.
     * @return Dataset with the five parsed log columns — still streaming if input was streaming.
     */
    public Dataset<Row> parseLogs(Dataset<Row> rawLogs) {
        log.info("\n**** StreamingMapper — parsing raw log lines with regexp_extract ****\n");

        String pattern = StreamingConstants.LOG_PATTERN;

        return rawLogs.select(
                functions.regexp_extract(rawLogs.col("value"), pattern, 1).as("log_timestamp"),
                functions.regexp_extract(rawLogs.col("value"), pattern, 2).as("level"),
                functions.regexp_extract(rawLogs.col("value"), pattern, 3).as("thread"),
                functions.regexp_extract(rawLogs.col("value"), pattern, 4).as("logger"),
                functions.regexp_extract(rawLogs.col("value"), pattern, 5).as("message")
        );
    }

    /**
     * Registers the parsed log Dataset as a temporary view and executes the enrichment SQL query.
     *
     * <p>The temporary view ({@code parsed_logs_view}) is session-scoped: it lives only for
     * the duration of the SparkSession and is never persisted to storage.
     *
     * <p>The SQL in {@link LogView#ENRICH_LOGS_SQL} filters out unmatched lines (empty
     * {@code level}) and returns all five structured columns.
     *
     * @param parsedLogs Dataset produced by {@link #parseLogs(Dataset)}.
     * @return Enriched (streaming) Dataset ready for display or writing.
     */
    public Dataset<Row> enrichLogs(Dataset<Row> parsedLogs) {
        log.info("\n**** StreamingMapper — registering parsed_logs_view and executing SQL ****\n");

        // Register the parsed DataFrame as a temporary view — valid for streaming DataFrames.
        parsedLogs.createOrReplaceTempView("parsed_logs_view");

        // Catalyst optimises this query plan just like any other SQL execution.
        Dataset<Row> enriched = sparkSession.sql(LogView.ENRICH_LOGS_SQL);

        log.info("\n**** StreamingMapper — SQL enrichment plan created ****\n");
        return enriched;
    }
}
