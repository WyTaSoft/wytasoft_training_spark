package com.wts.kayan.app.common;

import com.wts.kayan.app.mapping.StreamingMapper;
import com.wts.kayan.app.reader.StreamingReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the streaming data pipeline by connecting the reader to the mapper.
 *
 * <p>Mirrors {@link PrimaryRunner} for the batch ETL project. The same coordinator pattern
 * applies: {@code StreamingRunner} knows nothing about where logs come from or how SQL is
 * written — it only wires the two layers together and triggers plan construction.
 *
 * <p>ETL split for the streaming project:
 * <pre>
 *   StreamingReader → StreamingMapper → enriched streaming Dataset
 *       (Extract)        (Transform)
 * </pre>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class StreamingRunner {

    private static final Logger log = LoggerFactory.getLogger(StreamingRunner.class);

    private final StreamingReader streamingReader;
    private final SparkSession    sparkSession;

    /**
     * Constructs a {@code StreamingRunner}.
     *
     * @param streamingReader An initialised {@link StreamingReader} — the source of raw log lines.
     * @param sparkSession    Active SparkSession forwarded to the mapper.
     */
    public StreamingRunner(StreamingReader streamingReader, SparkSession sparkSession) {
        this.streamingReader = streamingReader;
        this.sparkSession    = sparkSession;
    }

    /**
     * Runs the full Extract → Transform pipeline and returns the enriched streaming Dataset.
     *
     * <p>Steps:
     * <ol>
     *   <li>Open the streaming text source via {@link StreamingReader#readLogs()}.</li>
     *   <li>Parse each raw log line using {@link StreamingMapper#parseLogs(Dataset)}.</li>
     *   <li>Register the parsed result as a temp view and apply the enrichment SQL via
     *       {@link StreamingMapper#enrichLogs(Dataset)}.</li>
     * </ol>
     *
     * <p>No data is read yet — Spark builds a logical plan that is only executed
     * when the caller invokes {@code writeStream().start()}.
     *
     * @return Enriched streaming {@link Dataset}{@literal <}{@link Row}{@literal >} ready
     *         for consumption by {@link com.wts.kayan.app.writer.StreamingWriter}.
     */
    public Dataset<Row> runStreamingRunner() {
        log.info("\n**** StreamingRunner — building streaming pipeline ****\n");

        // Step 1 — Extract: open the streaming file source.
        Dataset<Row> rawLogs = streamingReader.readLogs();

        // Step 2 — Transform: parse raw lines and enrich via SQL.
        StreamingMapper mapper  = new StreamingMapper(sparkSession);
        Dataset<Row> parsedLogs = mapper.parseLogs(rawLogs);
        Dataset<Row> enriched   = mapper.enrichLogs(parsedLogs);

        log.info("\n**** StreamingRunner — pipeline plan built successfully ****\n");
        return enriched;
    }
}
