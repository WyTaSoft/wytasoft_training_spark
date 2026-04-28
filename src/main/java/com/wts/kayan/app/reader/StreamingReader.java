package com.wts.kayan.app.reader;

import com.wts.kayan.app.utility.LogSchemaSelector;
import com.wts.kayan.app.utility.StreamingAppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for opening a Spark Structured Streaming source over a log file directory.
 *
 * <p>Mirrors {@link PrimaryReader} for the batch ETL project. The key difference is the use of
 * {@code sparkSession.readStream()} instead of {@code sparkSession.read()}, which returns a
 * <em>streaming</em> {@link Dataset} that is not materialised until a
 * {@code writeStream().start()} call triggers micro-batch execution.
 *
 * <p><strong>Source format — {@code text}</strong>: Spark reads each line of every file in the
 * monitored directory as a single row. The resulting Dataset has one column: {@code value}
 * ({@code StringType}). The raw log lines are parsed downstream by
 * {@link com.wts.kayan.app.mapping.StreamingMapper}.
 *
 * <p>{@code maxFilesPerTrigger=1} limits Spark to processing one new file per micro-batch,
 * which keeps latency predictable during development.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class StreamingReader extends LogSchemaSelector {

    private static final Logger log = LoggerFactory.getLogger(StreamingReader.class);

    private final SparkSession       sparkSession;
    private final StreamingAppConfig appConfig;

    /**
     * Constructs a {@code StreamingReader} backed by the given configuration.
     *
     * @param sparkSession Active Spark session.
     * @param appConfig    Resolved streaming configuration — provides the logs input path.
     */
    public StreamingReader(SparkSession sparkSession, StreamingAppConfig appConfig) {
        this.sparkSession = sparkSession;
        this.appConfig    = appConfig;
    }

    /**
     * Opens a streaming text source over the configured logs directory.
     *
     * <p>Spark watches the directory and picks up every new {@code .log} file as it arrives.
     * Each line becomes one row in the returned Dataset (column: {@code value}).
     *
     * @return Streaming {@link Dataset}{@literal <}{@link Row}{@literal >} with a single
     *         {@code value} ({@code StringType}) column — one row per log line.
     */
    public Dataset<Row> readLogs() {
        String logsPath = appConfig.getLogsInputPath();
        log.info("\n**** StreamingReader — opening log stream from: {} ****\n", logsPath);

        return sparkSession
                .readStream()
                .format("text")                      // each line → one row in "value" column
                .option("maxFilesPerTrigger", 1)     // process one file per micro-batch
                .load(logsPath);
    }
}
