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

/**
 * Static utility methods shared across the Spark application.
 *
 * <p><strong>Enhancement vs. previous units:</strong> paths are no longer hardcoded.
 * Every method that previously computed a path from string literals now accepts an
 * {@link AppConfig} instance and reads the path from {@code application.conf}.
 * This makes the same code work in dev, test, and prod without recompilation.
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
     * <p>Looks for sub-directories whose name starts with {@code <columnPartitioned>=}
     * (e.g. {@code date=2023-09-20}), extracts the date portion, and returns the
     * maximum as a {@code yyyy-MM-dd} string.
     *
     * @param path              Directory to scan (local path or HDFS URI).
     * @param columnPartitioned Partition column name (e.g. {@code "date"}).
     * @param spark             Active SparkSession (used to access Hadoop configuration).
     * @return Most recent partition value as {@code yyyy-MM-dd}, or {@code "2999-01-01"}
     *         if no partitions are found.
     */
    public static String getMaxPartition(String path, String columnPartitioned, SparkSession spark) {
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            FileStatus[] statuses = fs.listStatus(new Path(path));

            String prefix = columnPartitioned + "=";
            String[] partitionValues = Arrays.stream(statuses)
                    .filter(FileStatus::isDirectory)
                    .map(s -> s.getPath().getName())
                    .filter(name -> name.startsWith(prefix))
                    .map(name -> name.substring(prefix.length()))
                    .toArray(String[]::new);

            if (partitionValues.length == 0) {
                log.info("\n**** No partitions by {} found in {} ****\n", columnPartitioned, path);
                return "2999-01-01";
            }

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

    // -------------------------------------------------------------------------
    // DataFrame reading  (paths now come from AppConfig)
    // -------------------------------------------------------------------------

    /**
     * Reads a CSV dataset into a typed {@link Dataset}.
     *
     * <p>Paths are resolved from {@link AppConfig} instead of being hardcoded,
     * so the same method works across dev / test / prod environments.
     *
     * @param sourceName      Dataset identifier ({@link PrimaryConstants#CLIENTS} or
     *                        {@link PrimaryConstants#ORDERS}).
     * @param schema          Explicit schema — avoids Spark's inferSchema scan.
     * @param isLastPartition When {@code true} only the most recent partition of orders
     *                        is loaded and a {@code date} literal column is appended.
     * @param spark           Active SparkSession.
     * @param appConfig       Resolved application configuration.
     * @return Typed {@link Dataset} loaded from the resolved path.
     */
    public static Dataset<Row> readDataFrame(
            String sourceName,
            StructType schema,
            boolean isLastPartition,
            SparkSession spark,
            AppConfig appConfig) {

        log.info("\n**** Reading DataFrame for {} ****\n", sourceName);

        String inputPath;
        String tableName;
        String partitionValue = "";

        // Paths come from AppConfig — no hardcoded strings.
        switch (sourceName.toLowerCase()) {

            case PrimaryConstants.CLIENTS:
                inputPath = appConfig.getClientsPath();
                tableName = "";
                break;

            case PrimaryConstants.ORDERS:
                inputPath = appConfig.getOrdersPath();
                // Detect the most recent partition folder at runtime.
                partitionValue = getMaxPartition(inputPath, appConfig.getPartitionColumn(), spark);
                tableName      = "date=" + partitionValue + "/";
                break;

            default:
                throw new IllegalArgumentException("Unknown source: " + sourceName);
        }

        String fullPath = inputPath + tableName;
        log.info("\n Loading {} from {} \n", sourceName, fullPath);

        String[] columns = ColumnSelector.getColumnSequence(sourceName);

        Dataset<Row> dataFrame = spark.read()
                .schema(schema)
                .option("header", "true")
                .option("delimiter", ",")
                .csv(fullPath)
                .selectExpr(columns);

        if (isLastPartition && !partitionValue.isEmpty()) {
            log.info("\n Adding partition column '{}' = {} \n",
                    appConfig.getPartitionColumn(), partitionValue);
            return dataFrame.withColumn(appConfig.getPartitionColumn(),
                    functions.lit(partitionValue));
        }

        return dataFrame;
    }

    /**
     * Convenience overload — reads the full dataset without partition filtering.
     */
    public static Dataset<Row> readDataFrame(
            String sourceName,
            StructType schema,
            SparkSession spark,
            AppConfig appConfig) {
        return readDataFrame(sourceName, schema, false, spark, appConfig);
    }

    // -------------------------------------------------------------------------
    // DataFrame writing  (output path now comes from AppConfig)
    // -------------------------------------------------------------------------

    /**
     * Writes a {@link Dataset} to Parquet, partitioned by the column declared in
     * {@code AppConfig} ({@code writer.partition_by}).
     *
     * @param dataFrame  Dataset to persist.
     * @param appConfig  Resolved application configuration.
     */
    public static void writeDataFrame(Dataset<Row> dataFrame, AppConfig appConfig) {

        String mode         = appConfig.getWriterMode();
        int    numPartition = appConfig.getNumPartitions();
        String outputPath   = appConfig.getOutputPath();
        String partitionBy  = appConfig.getPartitionBy();

        log.info("\n *** Write started (mode={}, partitions={}, path={}) ***\n",
                mode, numPartition, outputPath);

        dataFrame
                .coalesce(numPartition)
                .write()
                .format("parquet")
                .partitionBy(partitionBy)
                .mode(mode)
                .save(outputPath);

        log.info("\n *** Write completed *** \n");
    }
}
