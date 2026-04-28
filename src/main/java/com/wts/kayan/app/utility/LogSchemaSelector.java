package com.wts.kayan.app.utility;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Abstract base class that centralises the schema definition for parsed log entries.
 *
 * <p>Mirrors {@link SchemaSelector} for the batch ETL project — the same rationale applies:
 * explicit schemas avoid the extra scan Spark performs with {@code inferSchema}, and they
 * document the data contract directly in the code.
 *
 * <p>Subclasses (e.g. {@link com.wts.kayan.app.reader.StreamingReader}) inherit
 * {@link #parsedLogSchema()} to verify or annotate the parsed streaming DataFrame.
 *
 * <p><strong>Schema columns</strong> (all {@code StringType} — timestamps are kept as strings
 * because Spark Structured Streaming's text source delivers raw lines; type-casting is deferred
 * to downstream transformations if needed):
 * <ul>
 *   <li>{@code log_timestamp} — timestamp string, e.g. {@code "2024-01-15 08:00:00.001"}.</li>
 *   <li>{@code level}         — log level: {@code INFO}, {@code WARN}, {@code ERROR}, {@code DEBUG}.</li>
 *   <li>{@code thread}        — thread name extracted from {@code [...]}.</li>
 *   <li>{@code logger}        — fully-qualified class name of the emitting logger.</li>
 *   <li>{@code message}       — everything after the {@code " - "} separator.</li>
 * </ul>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public abstract class LogSchemaSelector {

    /**
     * Schema of a single parsed log entry.
     *
     * <p>This schema reflects the output of {@link com.wts.kayan.app.mapping.StreamingMapper}
     * after applying {@code regexp_extract} to each raw log line.
     *
     * @return {@link StructType} with five {@code StringType} fields.
     */
    protected StructType parsedLogSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("log_timestamp", DataTypes.StringType, true),
                DataTypes.createStructField("level",         DataTypes.StringType, true),
                DataTypes.createStructField("thread",        DataTypes.StringType, true),
                DataTypes.createStructField("logger",        DataTypes.StringType, true),
                DataTypes.createStructField("message",       DataTypes.StringType, true)
        });
    }
}
