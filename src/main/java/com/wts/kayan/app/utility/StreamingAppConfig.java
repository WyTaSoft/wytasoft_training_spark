package com.wts.kayan.app.utility;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and exposes the Spark Structured Streaming configuration from {@code application.conf}.
 *
 * <p>Mirrors {@link AppConfig} for the batch ETL project. The same Typesafe Config mechanism
 * applies: {@link ConfigFactory#load()} reads {@code application.conf} from the classpath
 * and selects the right path block using the runtime {@code env} argument.
 *
 * <p><strong>HOCON block used by this class</strong> (see {@code application.conf}):
 * <pre>
 *   training_spark_streaming {
 *     data.logs.dev   = "src/main/resources/data/logs/"
 *     data.output.dev = "src/main/resources/data/datalake/logs_analysis/"
 *     writer.format       = "console"
 *     writer.output_mode  = "append"
 *     checkpoint.dev      = "target/checkpoint/logs/"
 *     trigger.interval_ms = 5000
 *   }
 * </pre>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class StreamingAppConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamingAppConfig.class);

    private static final String ROOT = "training_spark_streaming";

    private final Config config;
    private final String env;

    /**
     * Constructs a {@code StreamingAppConfig} for the given environment.
     *
     * @param env Runtime environment identifier — must match one of the keys declared
     *            under each path block in {@code application.conf}
     *            (e.g. {@code "dev"}, {@code "test"}, {@code "prod"}).
     */
    public StreamingAppConfig(String env) {
        this.config = ConfigFactory.load();
        this.env    = env;
        log.info("\n**** StreamingAppConfig loaded for environment: {} ****\n", env);
    }

    // -------------------------------------------------------------------------
    // Source data paths
    // -------------------------------------------------------------------------

    /**
     * Returns the directory monitored for incoming log files.
     *
     * @return Path string (local or HDFS) to the logs folder.
     */
    public String getLogsInputPath() {
        return config.getString(ROOT + ".data.logs." + env);
    }

    /**
     * Returns the output directory where the processed log results are written.
     *
     * @return Path string (local or HDFS) to the output datalake folder.
     */
    public String getOutputPath() {
        return config.getString(ROOT + ".data.output." + env);
    }

    // -------------------------------------------------------------------------
    // Writer / sink settings
    // -------------------------------------------------------------------------

    /**
     * Returns the streaming sink format ({@code "console"} or {@code "parquet"}).
     *
     * @return Sink format string.
     */
    public String getSinkFormat() {
        return config.getString(ROOT + ".writer.format");
    }

    /**
     * Returns the streaming output mode ({@code "append"} or {@code "complete"}).
     *
     * @return Output mode string.
     */
    public String getOutputMode() {
        return config.getString(ROOT + ".writer.output_mode");
    }

    // -------------------------------------------------------------------------
    // Checkpoint
    // -------------------------------------------------------------------------

    /**
     * Returns the checkpoint directory for exactly-once streaming guarantees.
     *
     * @return Path string (local or HDFS) to the checkpoint folder.
     */
    public String getCheckpointPath() {
        return config.getString(ROOT + ".checkpoint." + env);
    }

    // -------------------------------------------------------------------------
    // Trigger
    // -------------------------------------------------------------------------

    /**
     * Returns the processing-time trigger interval in milliseconds.
     *
     * <p>Spark checks for new micro-batches at this interval. A value of {@code 0}
     * runs as fast as possible; {@code -1} uses the one-time trigger.
     *
     * @return Trigger interval in milliseconds.
     */
    public long getTriggerIntervalMs() {
        return config.getLong(ROOT + ".trigger.interval_ms");
    }
}
