package com.wts.kayan.app.writer;

import com.wts.kayan.app.utility.PrimaryUtilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for persisting the enriched {@link Dataset} to storage.
 *
 * <p>{@code PrimaryWriter} is the <strong>Load</strong> step of the ETL pipeline,
 * completing the Extract → Transform → <strong>Load</strong> cycle:
 * <pre>
 *   PrimaryReader  →  PrimaryRunner  →  PrimaryMapper  →  PrimaryWriter
 *     (Extract)       (Orchestrate)      (Transform)         (Load)
 * </pre>
 *
 * <p>All low-level write logic (coalesce, format, partitionBy, mode) lives in
 * {@link PrimaryUtilities#writeDataFrame} so this class stays focused on the
 * write contract: <em>what</em> to write, <em>how many</em> partitions, and
 * <em>which</em> save mode to use.
 *
 * <p>This is the Java equivalent of the Scala {@code PrimaryWriter} class.
 * In Scala the environment was an implicit parameter; in Java it is passed
 * explicitly to {@link #write}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class PrimaryWriter {

    private static final Logger log = LoggerFactory.getLogger(PrimaryWriter.class);

    /**
     * Persists the given {@link Dataset} to Parquet storage.
     *
     * <p>Delegates to {@link PrimaryUtilities#writeDataFrame}, which:
     * <ul>
     *   <li>Coalesces the Dataset to {@code numPartition} output files.</li>
     *   <li>Writes in Parquet format, partitioned by {@code location}.</li>
     *   <li>Applies the given save {@code mode} (overwrite or append).</li>
     * </ul>
     *
     * @param dataFrame    Enriched Dataset produced by the Transform step.
     * @param mode         Save mode — use {@link com.wts.kayan.app.utility.PrimaryConstants#MODE_OVERWRITE}
     *                     or {@link com.wts.kayan.app.utility.PrimaryConstants#MODE_APPEND}.
     * @param numPartition Number of output Parquet files. Use {@code 1} or {@code 2} for
     *                     small training datasets; increase for production workloads.
     * @param env          Runtime environment identifier forwarded to the utility method.
     */
    public void write(Dataset<Row> dataFrame, String mode, int numPartition, String env) {
        log.info("\n**** PrimaryWriter — starting write (mode={}, partitions={}) ****\n",
                mode, numPartition);

        // Delegate all I/O details to PrimaryUtilities to keep this class thin.
        PrimaryUtilities.writeDataFrame(dataFrame, mode, numPartition, env);

        log.info("\n**** PrimaryWriter — write completed ****\n");
    }
}
