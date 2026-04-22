package com.wts.kayan;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract base class that manages a shared {@link SparkSession} for all Spark unit tests.
 *
 * <p><strong>Design choices:</strong>
 * <ul>
 *   <li>{@code @TestInstance(PER_CLASS)} — JUnit 5 creates one instance of each test
 *       subclass per test run (not one per method). This allows {@code @BeforeAll} and
 *       {@code @AfterAll} to be non-static while still running once, which is essential
 *       here because {@code SparkSession} is stored as an instance field.</li>
 *   <li>The SparkSession runs with {@code local[2]} — two local threads simulate a small
 *       cluster without needing a Hadoop installation.</li>
 *   <li>{@code spark.sql.shuffle.partitions=2} overrides Spark's default of 200, which
 *       would cause 200 empty shuffle files for tiny test datasets.</li>
 *   <li>The UI and event log are disabled so tests run without opening network ports or
 *       writing log files to disk.</li>
 * </ul>
 *
 * <p>Subclasses simply extend this class and access {@link #spark} directly in their tests.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseSparkTest {

    /** Shared SparkSession — created once before all tests, stopped after all tests. */
    protected SparkSession spark;

    /**
     * Starts a local SparkSession optimised for unit tests.
     * Called once before any test method in the subclass runs.
     */
    @BeforeAll
    public void setUpSpark() {
        spark = SparkSession.builder()
                .appName("unit-test")
                .master("local[2]")                                    // 2 local threads
                .config("spark.sql.shuffle.partitions", "2")           // avoid 200 empty files
                .config("spark.ui.enabled", "false")                   // no web UI in tests
                .config("spark.eventLog.enabled", "false")             // no event log files
                .config("spark.driver.bindAddress", "127.0.0.1")       // stable local address
                .getOrCreate();

        // Silence Spark's own verbose logging so test output is readable.
        spark.sparkContext().setLogLevel("WARN");
    }

    /**
     * Stops the SparkSession after all tests in the subclass have finished.
     */
    @AfterAll
    public void tearDownSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }
}
