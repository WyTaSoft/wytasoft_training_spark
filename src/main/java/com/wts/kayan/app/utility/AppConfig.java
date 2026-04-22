package com.wts.kayan.app.utility;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and exposes the application configuration from {@code application.conf}.
 *
 * <p>{@link ConfigFactory#load()} reads {@code application.conf} automatically from
 * the classpath — no file path needs to be passed at runtime. All paths and writer
 * settings are looked up by combining the HOCON key with the runtime {@code env}
 * argument, so the same JAR runs identically in dev, test, and prod.
 *
 * <p>This replaces the hardcoded path strings that were embedded directly inside
 * {@link PrimaryUtilities} in the previous units.
 *
 * <p><strong>HOCON substitution example</strong> — given {@code env = "dev"} the
 * call {@code config.getString("training_spark.data.clients.dev")} resolves to
 * {@code "src/main/resources/data/clients/"} as declared in {@code application.conf}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    private static final String ROOT = "training_spark";

    private final Config config;
    private final String env;

    /**
     * Constructs an {@code AppConfig} for the given environment.
     *
     * <p>{@link ConfigFactory#load()} reads {@code application.conf} from the classpath,
     * merges it with any system-property overrides (e.g. {@code -Dtraining_spark.writer.mode=append}),
     * and makes the resolved values available via the typed getter methods below.
     *
     * @param env Runtime environment identifier — must match one of the keys declared
     *            under each path block in {@code application.conf} (e.g. {@code "dev"},
     *            {@code "test"}, {@code "prod"}).
     */
    public AppConfig(String env) {
        this.config = ConfigFactory.load();
        this.env    = env;
        log.info("\n**** AppConfig loaded for environment: {} ****\n", env);
    }

    // -------------------------------------------------------------------------
    // Source data paths
    // -------------------------------------------------------------------------

    /**
     * Returns the base directory of the clients CSV files for the current environment.
     *
     * @return Path string (local or HDFS) to the clients folder.
     */
    public String getClientsPath() {
        return config.getString(ROOT + ".data.clients." + env);
    }

    /**
     * Returns the base directory of the orders CSV files for the current environment.
     * The actual partition sub-folder ({@code date=yyyy-MM-dd}) is resolved at runtime
     * by {@link PrimaryUtilities#getMaxPartition}.
     *
     * @return Path string (local or HDFS) to the orders folder.
     */
    public String getOrdersPath() {
        return config.getString(ROOT + ".data.orders." + env);
    }

    /**
     * Returns the output directory where the enriched Parquet dataset is written.
     *
     * @return Path string (local or HDFS) to the output datalake folder.
     */
    public String getOutputPath() {
        return config.getString(ROOT + ".data.output." + env);
    }

    // -------------------------------------------------------------------------
    // Writer settings
    // -------------------------------------------------------------------------

    /** @return Parquet save mode ({@code "overwrite"} or {@code "append"}). */
    public String getWriterMode() {
        return config.getString(ROOT + ".writer.mode");
    }

    /** @return Target number of output Parquet files after coalesce. */
    public int getNumPartitions() {
        return config.getInt(ROOT + ".writer.num_partitions");
    }

    /** @return Column used to physically partition the Parquet output. */
    public String getPartitionBy() {
        return config.getString(ROOT + ".writer.partition_by");
    }

    // -------------------------------------------------------------------------
    // Partition detection
    // -------------------------------------------------------------------------

    /**
     * Returns the name of the column used to detect the most recent data partition
     * (default: {@code "date"}, producing folders like {@code date=2023-09-20}).
     *
     * @return Partition column name.
     */
    public String getPartitionColumn() {
        return config.getString(ROOT + ".partition.column");
    }
}
