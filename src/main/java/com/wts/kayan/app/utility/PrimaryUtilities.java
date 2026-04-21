package com.wts.kayan.app.utility;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * Static utility methods shared across the Spark application.
 *
 * <p>This is the Java equivalent of the Scala {@code PrimaryUtilities} object. The key methods are:
 * <ul>
 *   <li>{@link #getMaxPartition} — scans an HDFS/local directory and returns the most recent
 *       date-partition folder (e.g. {@code date=2023-09-20}).</li>
 *   <li>{@link #readDataFrame} — reads a CSV dataset with an explicit schema, optional column
 *       selection, and optional last-partition loading.</li>
 *   <li>{@link #writeDataFrame} — writes a DataFrame to Parquet with partition-by.</li>
 * </ul>
 *
 * <p>Java does not have default parameter values, so overloaded versions of {@code readDataFrame}
 * are provided for the common cases.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public final class PrimaryUtilities {

    private static final Logger log = LoggerFactory.getLogger(PrimaryUtilities.class);

    private PrimaryUtilities() {}

    // -------------------------------------------------------------------------
    // Partition detection
    // -------------------------------------------------------------------------

    /**
     * Scans a directory and returns the value of the most recent date partition.
     *
     * <p>The method looks for sub-directories whose name starts with
     * {@code <columnPartitioned>=} (e.g. {@code date=2023-09-20}), extracts the date
     * portion, and returns the maximum date as a {@code yyyy-MM-dd} string.
     *
     * <p>This is a direct Java translation of the Scala {@code getMaxPartition} method.
     * In Scala, implicit parameters carry the SparkSession; in Java we pass it explicitly.
     *
     * @param path              Directory to scan (local path or HDFS URI).
     * @param columnPartitioned Partition column name — defaults to {@code "date"} in the
     *                          overload below.
     * @param spark             Active SparkSession (used to retrieve the Hadoop configuration).
     * @return Most recent partition value as {@code yyyy-MM-dd}, or {@code "2999-01-01"} if
     *         no partitions are found.
     */
    public static String getMaxPartition(String path, String columnPartitioned, SparkSession spark) {
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

            // List all entries under <path> and keep only directories whose name
            // contains the partition column — e.g. "date=2023-09-20".
            FileStatus[] statuses = fs.listStatus(new Path(path));

            String prefix = columnPartitioned + "=";
            String[] partitionValues = Arrays.stream(statuses)
                    .filter(FileStatus::isDirectory)
                    .map(s -> s.getPath().getName())    // e.g. "date=2023-09-20"
                    .filter(name -> name.startsWith(prefix))
                    .map(name -> name.substring(prefix.length())) // e.g. "2023-09-20"
                    .toArray(String[]::new);

            if (partitionValues.length == 0) {
                log.info("\n**** No partitions by {} found in {} ****\n", columnPartitioned, path);
                return "2999-01-01";
            }

            // Parse each string to a Date so we can find the maximum correctly,
            // then format the winner back to a string.
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date maxDate = Arrays.stream(partitionValues)
                    .map(d -> {
                        try { return sdf.parse(d); }
                        catch (ParseException e) { throw new RuntimeException(e); }
                    })
                    .max(Date::compareTo)
                    .orElseThrow(() -> new RuntimeException("Unexpected empty partition list"));

            String maxPartition = sdf.format(maxDate);
            log.info("\n**** max {} = {} ****\n", columnPartitioned, maxPartition);
            return maxPartition;

        } catch (IOException e) {
            log.error("Fatal Exception: could not read partition data from {}", path);
            throw new RuntimeException(e);
        }
    }

    /**
     * Convenience overload that defaults to {@code "date"} as the partition column.
     *
     * @param path  Directory to scan.
     * @param spark Active SparkSession.
     * @return Most recent partition value as {@code yyyy-MM-dd}.
     */
    public static String getMaxPartition(String path, SparkSession spark) {
        return getMaxPartition(path, "date", spark);
    }

    // -------------------------------------------------------------------------
    // DataFrame reading
    // -------------------------------------------------------------------------

    /**
     * Reads a CSV dataset into a {@link Dataset}{@literal <}{@link Row}{@literal >}.
     *
     * <p>The method resolves the file path from {@code sourceName}, applies the given
     * {@code schema}, projects only the columns declared in {@link ColumnSelector}, and
     * optionally appends the partition date as a literal column.
     *
     * @param sourceName      Dataset identifier ({@link PrimaryConstants#CLIENTS} or
     *                        {@link PrimaryConstants#ORDERS}).
     * @param schema          Explicit schema to apply — avoids Spark's inferSchema scan.
     * @param isLastPartition When {@code true}, only the most recent partition of orders
     *                        is loaded and a {@code date} column is added with its value.
     * @param spark           Active SparkSession.
     * @param env             Runtime environment identifier (e.g. {@code "dev"}).
     * @return Typed {@link Dataset} loaded from the resolved path.
     */
    public static Dataset<Row> readDataFrame(
            String sourceName,
            StructType schema,
            boolean isLastPartition,
            SparkSession spark,
            String env) {

        log.info("\n**** Reading DataFrame for {} ****\n", sourceName);

        String inputPath;
        String tableName;
        String partitionValue = "";

        // Resolve the physical path from the logical source name.
        switch (sourceName.toLowerCase()) {

            case PrimaryConstants.CLIENTS:
                inputPath = "src/main/resources/data/";
                tableName = "clients";
                break;

            case PrimaryConstants.ORDERS:
                inputPath = "src/main/resources/data/";
                tableName = "orders";
                // Detect the most recent partition before building the full path.
                partitionValue = getMaxPartition(inputPath + tableName + "/", spark);
                tableName = "orders/date=" + partitionValue;
                break;

            default:
                throw new IllegalArgumentException("Unknown source: " + sourceName);
        }

        log.info("\n Loading {} from {}{} \n", sourceName, inputPath, tableName.toLowerCase());

        // Build the column-expression array for selectExpr — comes from ColumnSelector
        // so that column picking stays in one place.
        String[] columns = ColumnSelector.getColumnSequence(sourceName);

        Dataset<Row> dataFrame = spark.read()
                .schema(schema)                    // Use explicit schema — no inferSchema scan.
                .option("header", "true")          // First row is the header.
                .option("delimiter", ",")          // Comma-separated values.
                .csv(inputPath + tableName.toLowerCase() + "/")
                .selectExpr(columns);              // Project only the declared columns.

        // For orders loaded from the last partition, attach the partition date as a column
        // so downstream transformations can reference it without re-reading the path.
        if (isLastPartition) {
            log.info("\n Adding partition column 'date' = {} \n", partitionValue);
            return dataFrame.withColumn("date", functions.lit(partitionValue));
        }

        return dataFrame;
    }

    /**
     * Convenience overload — reads the full dataset without partition filtering.
     *
     * @param sourceName Dataset identifier.
     * @param schema     Explicit schema.
     * @param spark      Active SparkSession.
     * @param env        Runtime environment identifier.
     * @return Typed {@link Dataset}.
     */
    public static Dataset<Row> readDataFrame(
            String sourceName,
            StructType schema,
            SparkSession spark,
            String env) {
        return readDataFrame(sourceName, schema, false, spark, env);
    }

    // -------------------------------------------------------------------------
    // DataFrame writing
    // -------------------------------------------------------------------------

    /**
     * Writes a {@link Dataset} to Parquet format, partitioned by {@code location}.
     *
     * @param dataFrame    Dataset to persist.
     * @param mode         Save mode — use {@link PrimaryConstants#MODE_OVERWRITE} or
     *                     {@link PrimaryConstants#MODE_APPEND}.
     * @param numPartition Number of output file partitions (controls parallelism and file size).
     * @param env          Runtime environment identifier.
     */
    public static void writeDataFrame(
            Dataset<Row> dataFrame,
            String mode,
            int numPartition,
            String env) {

        log.info("\n *** Write started (mode: {}, numPartition: {}) ... ***\n", mode, numPartition);

        dataFrame
                .coalesce(numPartition)   // Reduce to the desired number of output files.
                .write()
                .format("parquet")
                .partitionBy("location")  // Partition the output by client location.
                .mode(mode)
                .save("src/main/resources/data/datalake/clients_orders/");

        log.info("\n *** Write completed *** \n");
    }
}
