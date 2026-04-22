package com.wts.kayan.app.job;

import com.wts.kayan.app.common.PrimaryRunner;
import com.wts.kayan.app.reader.PrimaryReader;
import com.wts.kayan.app.utility.AppConfig;
import com.wts.kayan.app.utility.PrimaryConstants;
import com.wts.kayan.app.writer.PrimaryWriter;
import com.wts.kayan.sessionmanager.SparkSessionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point — Enhancement branch: HOCON config, Log4j2, unit tests.
 *
 * <p><strong>What changed vs. ud04:</strong>
 * <ol>
 *   <li>{@link AppConfig} replaces the raw {@code env} string as the configuration
 *       carrier — all paths and writer settings come from {@code application.conf}.</li>
 *   <li>Log4j2 is now the SLF4J backend (configured via {@code log4j2.xml}):
 *       Spark/Hadoop logs are silenced to WARN, application logs stay at INFO.</li>
 *   <li>{@link PrimaryWriter#write(Dataset, AppConfig)} takes a single {@code AppConfig}
 *       argument — no more scattered magic numbers.</li>
 * </ol>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class MainDriver {

    private static final Logger logger = LoggerFactory.getLogger(MainDriver.class);

    public static void main(String[] args) {

        // -------------------------------------------------------------------
        // Step 1 — Load configuration from application.conf (classpath)
        // -------------------------------------------------------------------
        // AppConfig wraps Typesafe ConfigFactory.load() and exposes typed
        // accessors. The env string selects the right path block in the HOCON file.
        String    env       = args[0];
        AppConfig appConfig = new AppConfig(env);

        // -------------------------------------------------------------------
        // Step 2 — Create the SparkSession
        // -------------------------------------------------------------------
        SparkSession sparkSession = SparkSessionManager.fetchSparkSession(
                PrimaryConstants.APPLICATION_NAME
        );

        logger.info("\n\n****  training job has started (env={}) ****\n\n", env);

        // -------------------------------------------------------------------
        // Step 3 — EXTRACT
        // -------------------------------------------------------------------
        PrimaryReader primaryReader = new PrimaryReader(sparkSession, appConfig);

        // -------------------------------------------------------------------
        // Step 4 — TRANSFORM
        // -------------------------------------------------------------------
        PrimaryRunner primaryRunner = new PrimaryRunner(primaryReader, sparkSession);
        Dataset<Row>  enriched      = primaryRunner.runPrimaryRunner();
        enriched.show();

        // -------------------------------------------------------------------
        // Step 5 — LOAD
        // -------------------------------------------------------------------
        // All write settings (path, mode, partitions) come from AppConfig.
        new PrimaryWriter().write(enriched, appConfig);

        // -------------------------------------------------------------------
        // Step 6 — Stop
        // -------------------------------------------------------------------
        sparkSession.stop();
    }
}
