package com.wts.kayan.app.utility;

import com.wts.kayan.BaseSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LogSchemaSelector} and {@link StreamingConstants}.
 *
 * <p>Mirrors {@link SchemaTest} for the batch ETL project. Tests verify that the schema
 * definitions are correct and that the log-pattern constant produces the expected capture
 * groups when used with {@code regexp_extract}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
class LogSchemaTest extends BaseSparkTest {

    // Concrete subclass used to access the protected parsedLogSchema() method.
    private final LogSchemaSelector schemaHolder = new LogSchemaSelector() {};

    // -------------------------------------------------------------------------
    // parsedLogSchema tests
    // -------------------------------------------------------------------------

    @Test
    void testParsedLogSchemaFieldCount() {
        assertEquals(5, schemaHolder.parsedLogSchema().fields().length,
                "parsedLogSchema must have exactly 5 fields");
    }

    @Test
    void testParsedLogSchemaFieldNames() {
        String[] names = schemaHolder.parsedLogSchema().fieldNames();
        assertArrayEquals(
                new String[]{"log_timestamp", "level", "thread", "logger", "message"},
                names,
                "parsedLogSchema field names must match the declared order");
    }

    @Test
    void testParsedLogSchemaAllStringType() {
        StructField[] fields = schemaHolder.parsedLogSchema().fields();
        for (StructField field : fields) {
            assertEquals(DataTypes.StringType, field.dataType(),
                    "All fields in parsedLogSchema must be StringType — field: " + field.name());
        }
    }

    @Test
    void testParsedLogSchemaCreatesDataset() {
        // Verify that a Dataset can be created with this schema without throwing.
        Dataset<Row> ds = spark.createDataFrame(
                Collections.singletonList(
                        RowFactory.create("2024-01-15 08:00:00.001", "INFO", "main",
                                "com.wts.kayan.app.job.StreamingDriver", "Job started")),
                schemaHolder.parsedLogSchema()
        );
        assertEquals(1L, ds.count(), "Dataset created from parsedLogSchema must have 1 row");
    }

    // -------------------------------------------------------------------------
    // StreamingConstants tests
    // -------------------------------------------------------------------------

    @Test
    void testApplicationNameConstant() {
        assertEquals("training_spark_streaming_java", StreamingConstants.APPLICATION_NAME);
    }

    @Test
    void testLogsConstant() {
        assertEquals("logs", StreamingConstants.LOGS);
    }

    @Test
    void testSinkConsoleConstant() {
        assertEquals("console", StreamingConstants.SINK_CONSOLE);
    }

    @Test
    void testSinkParquetConstant() {
        assertEquals("parquet", StreamingConstants.SINK_PARQUET);
    }

    @Test
    void testOutputModeAppendConstant() {
        assertEquals("append", StreamingConstants.OUTPUT_MODE_APPEND);
    }

    // -------------------------------------------------------------------------
    // LOG_PATTERN regex tests (via regexp_extract in batch mode)
    // -------------------------------------------------------------------------

    @Test
    void testLogPatternExtractsTimestamp() {
        String result = extractGroup(
                "2024-01-15 08:00:00.001 INFO  [main] com.wts.kayan - Message", 1);
        assertEquals("2024-01-15 08:00:00.001", result, "Group 1 must be the timestamp");
    }

    @Test
    void testLogPatternExtractsLevel() {
        String result = extractGroup(
                "2024-01-15 08:00:00.001 ERROR [worker] com.wts.kayan - Message", 2);
        assertEquals("ERROR", result, "Group 2 must be the log level");
    }

    @Test
    void testLogPatternExtractsThread() {
        String result = extractGroup(
                "2024-01-15 08:00:00.001 WARN  [thread-99] com.wts.kayan - Message", 3);
        assertEquals("thread-99", result, "Group 3 must be the thread name");
    }

    @Test
    void testLogPatternExtractsLogger() {
        String result = extractGroup(
                "2024-01-15 08:00:00.001 DEBUG [main] com.wts.kayan.app.utility.StreamingConstants - Msg", 4);
        assertEquals("com.wts.kayan.app.utility.StreamingConstants", result,
                "Group 4 must be the logger class name");
    }

    @Test
    void testLogPatternExtractsMessage() {
        String result = extractGroup(
                "2024-01-15 08:00:00.001 INFO  [main] com.wts.kayan - Hello world", 5);
        assertEquals("Hello world", result, "Group 5 must be the message text");
    }

    @Test
    void testLogPatternReturnsEmptyForMalformedLine() {
        // A line that does not match the pattern must produce empty string for all groups.
        String result = extractGroup("this is not a valid log line", 2);
        assertEquals("", result,
                "Malformed lines must yield empty string — they are filtered by SQL");
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    /**
     * Applies {@code regexp_extract} with {@link StreamingConstants#LOG_PATTERN} to a single
     * string and returns the value of the specified capture group.
     */
    private String extractGroup(String logLine, int group) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("value", DataTypes.StringType, true)
        });

        Dataset<Row> ds = spark.createDataFrame(
                Collections.singletonList(RowFactory.create(logLine)), schema);

        return ds.select(
                org.apache.spark.sql.functions.regexp_extract(
                        ds.col("value"), StreamingConstants.LOG_PATTERN, group)
        ).first().getString(0);
    }
}
