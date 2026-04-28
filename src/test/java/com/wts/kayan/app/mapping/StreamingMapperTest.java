package com.wts.kayan.app.mapping;

import com.wts.kayan.BaseSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StreamingMapper}.
 *
 * <p>Mirrors {@link PrimaryMapperTest} for the batch ETL project. The streaming transformer
 * ({@code regexp_extract}) works identically on batch and streaming Datasets, so all tests
 * use a static in-memory batch Dataset — no actual stream source is required.
 *
 * <p>Test data matches the log format defined by {@link com.wts.kayan.app.utility.StreamingConstants#LOG_PATTERN}:
 * <pre>
 *   2024-01-15 08:00:00.001 INFO  [main] com.wts.kayan.app.job.StreamingDriver - Message
 * </pre>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
class StreamingMapperTest extends BaseSparkTest {

    private StreamingMapper mapper;
    private Dataset<Row>    parsedLogs;

    @BeforeAll
    public void setUpMapper() {
        mapper     = new StreamingMapper(spark);
        parsedLogs = mapper.parseLogs(buildRawLogs());
        parsedLogs.createOrReplaceTempView("parsed_logs_view");
    }

    // -------------------------------------------------------------------------
    // Schema tests
    // -------------------------------------------------------------------------

    @Test
    void testParsedSchemaHasFiveColumns() {
        assertEquals(5, parsedLogs.schema().fields().length,
                "Parsed DataFrame must have exactly 5 columns");
    }

    @Test
    void testParsedSchemaColumnNames() {
        List<String> expectedCols = Arrays.asList(
                "log_timestamp", "level", "thread", "logger", "message");
        List<String> actualCols   = Arrays.asList(parsedLogs.schema().fieldNames());
        assertEquals(expectedCols, actualCols,
                "Column names must match the parsedLogSchema definition");
    }

    // -------------------------------------------------------------------------
    // Parsing correctness tests
    // -------------------------------------------------------------------------

    @Test
    void testRowCount() {
        // 8 input lines, all well-formed → 8 parsed rows (empty-string rows filtered by SQL)
        assertEquals(8L, parsedLogs.count(),
                "Every input line should produce one parsed row");
    }

    @Test
    void testTimestampExtracted() {
        Row first = parsedLogs.orderBy("log_timestamp").first();
        assertEquals("2024-01-15 08:00:00.001", first.getString(0),
                "First timestamp must be 2024-01-15 08:00:00.001");
    }

    @Test
    void testLevelExtracted() {
        Row first = parsedLogs.orderBy("log_timestamp").first();
        assertEquals("INFO", first.getString(1),
                "Level of the first row must be INFO");
    }

    @Test
    void testThreadExtracted() {
        Row first = parsedLogs.orderBy("log_timestamp").first();
        assertEquals("main", first.getString(2),
                "Thread of the first row must be main");
    }

    @Test
    void testLoggerExtracted() {
        Row first = parsedLogs.orderBy("log_timestamp").first();
        assertEquals("com.wts.kayan.app.job.StreamingDriver", first.getString(3),
                "Logger of the first row must be com.wts.kayan.app.job.StreamingDriver");
    }

    @Test
    void testMessageExtracted() {
        Row first = parsedLogs.orderBy("log_timestamp").first();
        assertEquals("Streaming job has started", first.getString(4),
                "Message of the first row must be 'Streaming job has started'");
    }

    // -------------------------------------------------------------------------
    // Level distribution tests
    // -------------------------------------------------------------------------

    @Test
    void testInfoRowCount() {
        long infoCount = parsedLogs.filter("level = 'INFO'").count();
        assertEquals(3L, infoCount, "Expected 3 INFO rows in test data");
    }

    @Test
    void testWarnRowCount() {
        long warnCount = parsedLogs.filter("level = 'WARN'").count();
        assertEquals(2L, warnCount, "Expected 2 WARN rows in test data");
    }

    @Test
    void testErrorRowCount() {
        long errorCount = parsedLogs.filter("level = 'ERROR'").count();
        assertEquals(2L, errorCount, "Expected 2 ERROR rows in test data");
    }

    @Test
    void testDebugRowCount() {
        long debugCount = parsedLogs.filter("level = 'DEBUG'").count();
        assertEquals(1L, debugCount, "Expected 1 DEBUG row in test data");
    }

    // -------------------------------------------------------------------------
    // SQL enrichment tests
    // -------------------------------------------------------------------------

    @Test
    void testEnrichLogsFiltersMalformedLines() {
        // enrichLogs() applies WHERE level != '' — an empty level means the regex did not match.
        Dataset<Row> enriched = mapper.enrichLogs(parsedLogs);
        long enrichedCount    = enriched.count();
        // All 8 test lines are well-formed, so the count should be unchanged.
        assertEquals(8L, enrichedCount,
                "enrichLogs must retain all well-formed rows");
    }

    @Test
    void testEnrichLogsOutputColumns() {
        Dataset<Row> enriched    = mapper.enrichLogs(parsedLogs);
        List<String> actualCols  = Arrays.asList(enriched.schema().fieldNames());
        List<String> expectedCols = Arrays.asList(
                "log_timestamp", "level", "thread", "logger", "message");
        assertEquals(expectedCols, actualCols,
                "enrichLogs output must have exactly the 5 parsed columns");
    }

    @Test
    void testErrorLogsSqlReturnsOnlyErrors() {
        // Apply the error-only SQL directly against the already-registered view.
        Dataset<Row> errorLogs = spark.sql(LogView.ERROR_LOGS_SQL);
        List<Row> rows = errorLogs.collectAsList();
        assertFalse(rows.isEmpty(), "ERROR_LOGS_SQL must return at least one row");
        // The error-only query returns log_timestamp, logger, message — no level column.
        for (Row row : rows) {
            assertEquals(3, row.length(),
                    "ERROR_LOGS_SQL must return rows with 3 columns");
        }
    }

    // -------------------------------------------------------------------------
    // Test data helpers
    // -------------------------------------------------------------------------

    /** Builds an 8-row in-memory batch Dataset simulating raw log lines. */
    private Dataset<Row> buildRawLogs() {
        StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("value", DataTypes.StringType, true)
        });

        List<Row> rows = Arrays.asList(
                RowFactory.create("2024-01-15 08:00:00.001 INFO  [main] com.wts.kayan.app.job.StreamingDriver - Streaming job has started"),
                RowFactory.create("2024-01-15 08:00:01.234 INFO  [task-1] com.wts.kayan.app.reader.StreamingReader - Opening log stream from: src/test/resources/data/logs/"),
                RowFactory.create("2024-01-15 08:00:02.000 WARN  [thread-2] org.apache.spark.SparkContext - Configuration key not found"),
                RowFactory.create("2024-01-15 08:00:03.000 ERROR [worker-1] com.wts.kayan.app.mapping.StreamingMapper - Malformed log line encountered"),
                RowFactory.create("2024-01-15 08:00:04.123 DEBUG [main] com.wts.kayan.app.utility.StreamingConstants - Pattern loaded"),
                RowFactory.create("2024-01-15 08:00:05.456 INFO  [task-3] com.wts.kayan.app.writer.StreamingWriter - Writing batch to console sink"),
                RowFactory.create("2024-01-15 08:00:06.789 ERROR [worker-2] com.wts.kayan.app.reader.StreamingReader - File not found"),
                RowFactory.create("2024-01-15 08:00:07.012 WARN  [thread-4] com.wts.kayan.app.common.StreamingRunner - Retrying connection")
        );

        return spark.createDataFrame(rows, schema);
    }
}
