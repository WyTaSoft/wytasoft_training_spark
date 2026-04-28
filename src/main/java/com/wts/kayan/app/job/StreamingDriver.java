package com.wts.kayan.app.job;

import com.wts.kayan.app.common.StreamingRunner;
import com.wts.kayan.app.reader.StreamingReader;
import com.wts.kayan.app.utility.StreamingAppConfig;
import com.wts.kayan.app.utility.StreamingConstants;
import com.wts.kayan.app.writer.StreamingWriter;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Spark Structured Streaming log consumption job.
 *
 * <p>Mirrors {@link MainDriver} for the batch ETL project — the same five-step pattern:
 * <ol>
 *   <li><strong>Config</strong>  — load {@code application.conf} via {@link StreamingAppConfig}.</li>
 *   <li><strong>Session</strong> — create the {@link SparkSession} via {@link SparkSessionManager}.</li>
 *   <li><strong>Extract</strong> — open the streaming log source via {@link StreamingReader}.</li>
 *   <li><strong>Transform</strong> — parse and enrich log lines via {@link StreamingRunner}.</li>
 *   <li><strong>Load</strong>    — start the streaming output query via {@link StreamingWriter}.</li>
 * </ol>
 *
 * <p>The driver calls {@link StreamingQuery#awaitTermination()} to keep the JVM alive while
 * micro-batches are processed. To stop the job, send a SIGTERM or interrupt the process.
 *
 * <p><strong>Usage:</strong>
 * <pre>
 *   spark-submit \
 *     --class com.wts.kayan.app.job.StreamingDriver \
 *     wts-training-spark-java.jar dev
 * </pre>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class StreamingDriver {

    // Runs before LoggerFactory is first touched — forces our log4j2.xml to load
    // instead of Spark's bundled log4j2-defaults.properties from spark-core.jar,
    // and pre-silences Jetty before SparkUI starts the server.
    static {
        System.setProperty("log4j2.configurationFile", "log4j2.xml");
        org.apache.logging.log4j.core.config.Configurator
                .setLevel("org.sparkproject.jetty", org.apache.logging.log4j.Level.ERROR);
    }

    private static final Logger logger = LoggerFactory.getLogger(StreamingDriver.class);

    public static void main(String[] args) throws Exception {

        // -------------------------------------------------------------------
        // Step 1 — Load streaming configuration from application.conf
        // -------------------------------------------------------------------
        String             env       = (args.length > 0) ? args[0] : "dev";
        StreamingAppConfig appConfig = new StreamingAppConfig(env);

        // -------------------------------------------------------------------
        // Step 2 — Create the SparkSession
        // -------------------------------------------------------------------
        SparkSession sparkSession = SparkSessionManager.fetchSparkSession(
                StreamingConstants.APPLICATION_NAME
        );

        logger.info("\n\n****  streaming job has started (env={}) ****\n\n", env);

        // -------------------------------------------------------------------
        // Step 3 — EXTRACT
        // -------------------------------------------------------------------
        StreamingReader streamingReader = new StreamingReader(sparkSession, appConfig);

        // -------------------------------------------------------------------
        // Step 4 — TRANSFORM
        // -------------------------------------------------------------------
        StreamingRunner runner   = new StreamingRunner(streamingReader, sparkSession);
        Dataset<Row>    enriched = runner.runStreamingRunner();

        // -------------------------------------------------------------------
        // Step 5 — LOAD (starts async streaming query)
        // -------------------------------------------------------------------
        StreamingWriter writer = new StreamingWriter();
        StreamingQuery  query  = writer.write(enriched, appConfig);

        // Block the driver until the query is stopped externally (SIGTERM / exception).
        query.awaitTermination();

        // -------------------------------------------------------------------
        // Step 6 — Stop
        // -------------------------------------------------------------------
        sparkSession.stop();
    }
}
