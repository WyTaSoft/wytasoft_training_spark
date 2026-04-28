package com.wts.kayan.app.writer;

import com.wts.kayan.app.utility.StreamingAppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Responsible for starting the Spark Structured Streaming output query.
 *
 * <p>Mirrors {@link PrimaryWriter} for the batch ETL project. The key difference is the use of
 * {@code writeStream()} instead of {@code write()}, which returns a
 * {@link StreamingQuery} — a live handle to the running streaming job.
 *
 * <p><strong>Enhancement vs. a hardcoded writer</strong>: all sink settings (format, output
 * mode, checkpoint path, trigger interval) come from {@link StreamingAppConfig}, so the same
 * JAR can write to the console in dev and to Parquet in prod without any code change.
 *
 * <p><strong>Trigger</strong>: {@link Trigger#ProcessingTime} wakes up Spark every
 * {@code trigger.interval_ms} milliseconds to check for new data. A value of {@code 0}
 * runs as fast as possible (continuous micro-batching).
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class StreamingWriter {

    private static final Logger log = LoggerFactory.getLogger(StreamingWriter.class);

    /**
     * Starts the streaming output query.
     *
     * <p>All write settings (format, output mode, checkpoint, trigger) come from
     * {@link StreamingAppConfig}. The method returns immediately — the query runs
     * asynchronously. Callers must invoke {@link StreamingQuery#awaitTermination()} to
     * keep the driver alive.
     *
     * @param dataFrame  Enriched streaming Dataset produced by the Transform step.
     * @param appConfig  Resolved streaming configuration.
     * @return A running {@link StreamingQuery} handle.
     * @throws Exception if the streaming query fails to start.
     */
    public StreamingQuery write(Dataset<Row> dataFrame, StreamingAppConfig appConfig) throws Exception {
        log.info("\n**** StreamingWriter — starting streaming query (sink={}, mode={}) ****\n",
                appConfig.getSinkFormat(), appConfig.getOutputMode());

        StreamingQuery query = dataFrame
                .writeStream()
                .outputMode(appConfig.getOutputMode())
                .format(appConfig.getSinkFormat())
                .option("checkpointLocation", appConfig.getCheckpointPath())
                .trigger(Trigger.ProcessingTime(appConfig.getTriggerIntervalMs(), TimeUnit.MILLISECONDS))
                .start();

        log.info("\n**** StreamingWriter — query started (id={}) ****\n", query.id());
        return query;
    }
}
