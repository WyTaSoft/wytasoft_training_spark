package com.wts.kayan.app.utility;

import com.wts.kayan.BaseSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify the schema definitions in {@link SchemaSelector} are correct
 * and that Spark can read CSVs using those schemas without type errors.
 *
 * <p>These tests also demonstrate how to create a minimal in-memory Dataset with a
 * given schema and assert on its structure — a fundamental skill for DataFrame testing.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
@DisplayName("SchemaSelector — schema definition tests")
class SchemaTest extends BaseSparkTest {

    // Concrete subclass just to instantiate the abstract SchemaSelector.
    private static final SchemaSelector SELECTOR = new SchemaSelector() {};

    // -------------------------------------------------------------------------
    // clientsSchema
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("clientsSchema has exactly 3 fields")
    void testClientsSchemaFieldCount() {
        assertEquals(3, SELECTOR.clientsSchema().fields().length);
    }

    @Test
    @DisplayName("clientsSchema field names and types are correct")
    void testClientsSchemaFieldTypes() {
        StructType schema = SELECTOR.clientsSchema();

        assertFieldType(schema, "clientId", DataTypes.IntegerType);
        assertFieldType(schema, "name",     DataTypes.StringType);
        assertFieldType(schema, "location", DataTypes.StringType);
    }

    @Test
    @DisplayName("clientsSchema can be used to create a valid Dataset")
    void testClientsSchemaCreatesDataset() {
        Dataset<Row> ds = spark.createDataFrame(
                Collections.singletonList(RowFactory.create(1, "John Doe", "New York")),
                SELECTOR.clientsSchema()
        );
        assertEquals(1L, ds.count());
        assertEquals(1, ds.first().getInt(ds.first().fieldIndex("clientId")));
    }

    // -------------------------------------------------------------------------
    // ordersSchema
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("ordersSchema has exactly 4 fields")
    void testOrdersSchemaFieldCount() {
        assertEquals(4, SELECTOR.ordersSchema().fields().length);
    }

    @Test
    @DisplayName("ordersSchema field names and types are correct")
    void testOrdersSchemaFieldTypes() {
        StructType schema = SELECTOR.ordersSchema();

        assertFieldType(schema, "orderId",  DataTypes.IntegerType);
        assertFieldType(schema, "clientId", DataTypes.IntegerType);
        assertFieldType(schema, "amount",   DataTypes.DoubleType);
        assertFieldType(schema, "date",     DataTypes.DateType);
    }

    @Test
    @DisplayName("ordersSchema can be used to create a valid Dataset")
    void testOrdersSchemaCreatesDataset() {
        // Use null for the date field — DateType rows require a java.sql.Date.
        Dataset<Row> ds = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(101, 1, 150.50, null),
                        RowFactory.create(102, 2, 200.00, null)
                ),
                SELECTOR.ordersSchema()
        );
        assertEquals(2L, ds.count());
    }

    // -------------------------------------------------------------------------
    // ColumnSelector
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("ColumnSelector returns correct columns for CLIENTS")
    void testColumnSelectorClients() {
        String[] cols = ColumnSelector.getColumnSequence(PrimaryConstants.CLIENTS);
        assertArrayEquals(new String[]{"clientid", "name", "location"}, cols);
    }

    @Test
    @DisplayName("ColumnSelector returns correct columns for ORDERS")
    void testColumnSelectorOrders() {
        String[] cols = ColumnSelector.getColumnSequence(PrimaryConstants.ORDERS);
        assertArrayEquals(new String[]{"orderid", "clientid", "amount", "date"}, cols);
    }

    @Test
    @DisplayName("ColumnSelector throws for unknown table")
    void testColumnSelectorUnknownTable() {
        assertThrows(IllegalArgumentException.class,
                () -> ColumnSelector.getColumnSequence("unknown_table"));
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private void assertFieldType(StructType schema, String fieldName, DataType expectedType) {
        StructField field = schema.apply(fieldName);
        assertNotNull(field, "Field '" + fieldName + "' not found in schema");
        assertEquals(expectedType, field.dataType(),
                "Field '" + fieldName + "' has wrong type");
    }
}
