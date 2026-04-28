// Package declaration — organizes the code into logical namespaces, similar to a folder structure.
package com.wts.kayan.sessionmanager;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.SparkSession;

/**
 * Utility class to manage the creation of a SparkSession.
 *
 * <p>In Java, we cannot use Scala singleton objects, so we use a class with a
 * static factory method to achieve the same effect. The SparkSession itself
 * is a singleton under the hood: {@code getOrCreate()} always returns the same
 * instance if one is already live in the JVM.
 *
 * <p>The session is pre-configured with a set of performance-oriented options
 * (Kryo serialization, Tungsten, Snappy compression) that are suitable for both
 * local development and cluster execution.
 *
 * @author Mehdi TAJMOUATI
 * @note  Big Data / Cloud Trainer at WyTaSoft.
 *        Contact: mehdi.tajmouati@wytasoft.com
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class SparkSessionManager {

    // Private constructor prevents instantiation — all access is via the static method.
    private SparkSessionManager() {}

    /**
     * Fetches or creates a SparkSession with the given application name.
     *
     * <p>Key configurations applied:
     * <ul>
     *   <li>{@code master("local[*]")} — runs on all available cores when no cluster is configured.</li>
     *   <li>KryoSerializer — faster and more compact than Java's default serialization.</li>
     *   <li>Tungsten — Spark's in-memory binary processing engine.</li>
     *   <li>Snappy compression — reduces IO and memory overhead for RDD operations.</li>
     *   <li>UI enabled on port 4040 — open {@code http://localhost:4040} to inspect your jobs.</li>
     * </ul>
     *
     * @param appName Name displayed in the Spark UI and application logs.
     * @return A fully configured {@link SparkSession} instance.
     */
    public static SparkSession fetchSparkSession(String appName) {

        // Silence Jetty and Hadoop BEFORE getOrCreate() — both start logging during UI init.
        Configurator.setLevel("org.sparkproject.jetty", Level.ERROR);
        Configurator.setLevel("org.apache.hadoop",       Level.ERROR);

        // Builder pattern: each .config() call sets one Spark property.
        SparkSession session = SparkSession
                .builder()
                .appName(appName)          // Shown in the Spark UI header.
                .master("local[*]")        // Use all local CPU cores; replace with "yarn" or "spark://..." on a cluster.
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // Faster than default Java serialization.
                .config("spark.sql.tungsten.enabled", "true")     // Binary in-memory processing for better throughput.
                .config("spark.rdd.compress", "true")             // Compress cached RDDs to save memory.
                .config("spark.io.compression.codec", "snappy")   // Snappy offers a good speed/compression ratio.
                .config("spark.sql.broadcastTimeout", "1200")     // Extra time for broadcast joins on large datasets.
                .config("spark.driver.bindAddress", "0.0.0.0")    // Listen on all interfaces.
                .config("spark.driver.host", "127.0.0.1")         // Advertise localhost so the UI is reachable.
                .config("spark.ui.enabled", "true")               // Enable the Spark Web UI.
                .config("spark.ui.port", "4040")                  // Fixed port — open http://localhost:4040 to explore.
                // Use the FileSystem-based checkpoint manager — avoids FileContext failures on Windows.
                .config("spark.sql.streaming.checkpointFileManagerClass",
                        "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
                .getOrCreate();

        // Suppress Spark internal DEBUG/INFO noise — only ERROR from the framework.
        // setLogLevel covers Spark's own root logger; Configurator.setLevel covers any
        // sub-package loggers that Spark's bundled log4j2-defaults may have overridden.
        session.sparkContext().setLogLevel("ERROR");
        Configurator.setLevel("org.apache.spark",        Level.ERROR);
        Configurator.setLevel("org.apache.hadoop",       Level.ERROR);
        Configurator.setLevel("org.sparkproject.jetty",  Level.ERROR);
        Configurator.setLevel("org.apache.zookeeper",    Level.ERROR);
        return session;
    }
}
