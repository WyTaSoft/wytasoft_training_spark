package com.wts.kayan.app.utility;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Abstract base class that centralises the schema definitions for all source datasets.
 *
 * <p>In the Scala training this was a <em>trait</em>; in Java the closest equivalent is an
 * abstract class that subclasses extend to inherit the schema definitions.
 *
 * <p>Defining schemas explicitly (rather than relying on {@code inferSchema}) is a production
 * best-practice for two reasons:
 * <ol>
 *   <li>It avoids the extra scan Spark would perform to infer types, saving time and cost.</li>
 *   <li>It documents the contract of the data source directly in the code.</li>
 * </ol>
 *
 * <p>Subclasses (e.g. {@code PrimaryReader}) call {@link #clientsSchema()} and
 * {@link #ordersSchema()} to obtain a typed {@link StructType} ready to pass to
 * {@code sparkSession.read().schema(...)}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public abstract class SchemaSelector {

    /**
     * Schema for the <strong>clients</strong> dataset.
     *
     * <ul>
     *   <li>{@code clientId}  — integer primary key.</li>
     *   <li>{@code name}      — full name of the client.</li>
     *   <li>{@code location}  — city or region of the client.</li>
     * </ul>
     *
     * @return {@link StructType} describing the clients table.
     */
    protected StructType clientsSchema() {
        // StructType wraps an array of StructFields — one per column.
        // DataTypes.createStructField(name, dataType, nullable)
        return new StructType(new StructField[]{
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("name",     DataTypes.StringType,  true),
                DataTypes.createStructField("location", DataTypes.StringType,  true)
        });
    }

    /**
     * Schema for the <strong>orders</strong> dataset.
     *
     * <ul>
     *   <li>{@code orderId}   — integer primary key for the order.</li>
     *   <li>{@code clientId}  — foreign key linking back to the client.</li>
     *   <li>{@code amount}    — total monetary value of the order.</li>
     *   <li>{@code date}      — date the order was placed (partition column).</li>
     * </ul>
     *
     * @return {@link StructType} describing the orders table.
     */
    protected StructType ordersSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("orderId",  DataTypes.IntegerType, true),
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("amount",   DataTypes.DoubleType,  true),
                DataTypes.createStructField("date",     DataTypes.DateType,    true)
        });
    }
}
