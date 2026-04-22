package com.wts.kayan.app.mapping;

import com.wts.kayan.BaseSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PrimaryMapper}.
 *
 * <p><strong>Testing strategy:</strong> DataFrames are built in memory using
 * {@link org.apache.spark.sql.SparkSession#createDataFrame} with {@link RowFactory}
 * rows — no CSV files are read. This keeps tests fast, deterministic, and
 * independent of the filesystem layout.
 *
 * <p>The pattern to compare two DataFrames for equality in Spark is:
 * <pre>
 *   assertTrue(actual.except(expected).isEmpty());
 *   assertTrue(expected.except(actual).isEmpty());
 * </pre>
 * {@code except()} returns rows in the left side that are not in the right side.
 * Both directions must be empty for full equality (handles duplicates correctly).
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
@DisplayName("PrimaryMapper — SQL enrichment tests")
class PrimaryMapperTest extends BaseSparkTest {

    // -------------------------------------------------------------------------
    // Test data helpers
    // -------------------------------------------------------------------------

    private Dataset<Row> buildClients() {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("name",     DataTypes.StringType,  true),
                DataTypes.createStructField("location", DataTypes.StringType,  true)
        });
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "John Doe",   "New York"),
                RowFactory.create(2, "Jane Smith", "California")
        );
        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> buildOrders() {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("orderId",  DataTypes.IntegerType, true),
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("amount",   DataTypes.DoubleType,  true),
                DataTypes.createStructField("date",     DataTypes.StringType,  true)
        });
        List<Row> rows = Arrays.asList(
                RowFactory.create(101, 1, 150.50, "2023-09-20"),  // John: two orders
                RowFactory.create(102, 1, 200.00, "2023-09-20"),
                RowFactory.create(103, 2, 320.75, "2023-09-20")   // Jane: one order
        );
        return spark.createDataFrame(rows, schema);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("enrichDataFrame() returns one row per order (LEFT JOIN)")
    void testRowCount() {
        Dataset<Row> result = new PrimaryMapper(buildClients(), buildOrders(), spark)
                .enrichDataFrame();

        // 2 orders for client 1 + 1 order for client 2 = 3 rows
        assertEquals(3L, result.count(),
                "Expected one output row per order row after the LEFT JOIN");
    }

    @Test
    @DisplayName("enrichDataFrame() output contains expected columns")
    void testOutputColumns() {
        Dataset<Row> result = new PrimaryMapper(buildClients(), buildOrders(), spark)
                .enrichDataFrame();

        List<String> columns = Arrays.asList(result.columns());
        assertTrue(columns.contains("clientId"),            "Missing clientId");
        assertTrue(columns.contains("name"),                "Missing name");
        assertTrue(columns.contains("location"),            "Missing location");
        assertTrue(columns.contains("orderId"),             "Missing orderId");
        assertTrue(columns.contains("amount"),              "Missing amount");
        assertTrue(columns.contains("date"),                "Missing date");
        assertTrue(columns.contains("totalAmountByClient"), "Missing totalAmountByClient");
    }

    @Test
    @DisplayName("window function computes correct per-client total")
    void testWindowTotalAmountByClient() {
        Dataset<Row> result = new PrimaryMapper(buildClients(), buildOrders(), spark)
                .enrichDataFrame();

        // For client 1 both rows should carry total = 150.50 + 200.00 = 350.50
        Dataset<Row> client1Rows = result.filter("clientId = 1");
        assertEquals(2L, client1Rows.count());

        client1Rows.collectAsList().forEach(row -> {
            double total = row.getDouble(row.fieldIndex("totalAmountByClient"));
            assertEquals(350.50, total, 0.001,
                    "Client 1 totalAmountByClient should be 350.50");
        });

        // For client 2 the single row should carry total = 320.75
        Row client2Row = result.filter("clientId = 2").first();
        double total2 = client2Row.getDouble(client2Row.fieldIndex("totalAmountByClient"));
        assertEquals(320.75, total2, 0.001,
                "Client 2 totalAmountByClient should be 320.75");
    }

    @Test
    @DisplayName("LEFT JOIN retains clients that have no orders")
    void testLeftJoinKeepsClientsWithNoOrders() {
        // Add a third client with no matching orders
        StructType clientSchema = new StructType(new StructField[]{
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("name",     DataTypes.StringType,  true),
                DataTypes.createStructField("location", DataTypes.StringType,  true)
        });
        List<Row> clientRows = Arrays.asList(
                RowFactory.create(1, "John Doe",        "New York"),
                RowFactory.create(99, "Ghost Client",   "Unknown")  // no orders
        );
        Dataset<Row> clients = spark.createDataFrame(clientRows, clientSchema);

        Dataset<Row> result = new PrimaryMapper(clients, buildOrders(), spark)
                .enrichDataFrame();

        // Client 99 should appear with null orderId (LEFT JOIN)
        long ghostRows = result.filter("clientId = 99").count();
        assertEquals(1L, ghostRows,
                "LEFT JOIN must retain client 99 even though they have no orders");

        Row ghostRow = result.filter("clientId = 99").first();
        assertTrue(ghostRow.isNullAt(ghostRow.fieldIndex("orderId")),
                "orderId should be null for a client with no orders");
    }
}
