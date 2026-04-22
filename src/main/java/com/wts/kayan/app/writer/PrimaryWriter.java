package com.wts.kayan.app.writer;

import com.wts.kayan.app.utility.AppConfig;
import com.wts.kayan.app.utility.PrimaryUtilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for persisting the enriched {@link Dataset} to storage.
 *
 * <p><strong>Enhancement vs. previous units:</strong> write parameters (output path,
 * save mode, partition column, file count) are read from {@link AppConfig} instead of
 * being passed as individual arguments or hardcoded. A single {@code write()} call is
 * now enough — all settings come from {@code application.conf}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class PrimaryWriter {

    private static final Logger log = LoggerFactory.getLogger(PrimaryWriter.class);

    /**
     * Persists the given {@link Dataset} using settings from {@link AppConfig}.
     *
     * @param dataFrame Enriched Dataset produced by the Transform step.
     * @param appConfig Resolved application configuration — provides output path,
     *                  save mode, partition column, and file count.
     */
    public void write(Dataset<Row> dataFrame, AppConfig appConfig) {
        log.info("\n**** PrimaryWriter — starting write ****\n");
        PrimaryUtilities.writeDataFrame(dataFrame, appConfig);
        log.info("\n**** PrimaryWriter — write completed ****\n");
    }
}
