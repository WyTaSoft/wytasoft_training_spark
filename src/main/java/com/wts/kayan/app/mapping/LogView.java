package com.wts.kayan.app.mapping;

/**
 * Holds the SQL query strings used by {@link StreamingMapper} to analyse parsed log entries.
 *
 * <p>Mirrors {@link PrimaryView} for the batch ETL project — SQL kept in a dedicated class
 * so it is easy to find, test, and evolve independently of the execution logic.
 *
 * <p><strong>Streaming SQL constraints</strong>: not all SQL constructs are valid in
 * streaming queries. Notably, {@code ORDER BY} requires {@code complete} output mode and
 * a bounded result, which conflicts with the {@code append} mode used here. The queries
 * below are written to be compatible with {@code append} output mode.
 *
 * <p>The queries read from the temporary view {@code parsed_logs_view} registered by
 * {@link StreamingMapper#enrichLogs(Dataset)}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public final class LogView {

    private LogView() {}

    /**
     * SQL query that selects all well-formed log entries from the parsed view.
     *
     * <p>Rows where {@code level} is an empty string are the result of lines that did not
     * match the regex pattern (e.g. stack traces, continuation lines). Filtering them out
     * keeps the output clean.
     *
     * <p>Output columns:
     * <ul>
     *   <li>{@code log_timestamp} — original timestamp string.</li>
     *   <li>{@code level}         — log level ({@code INFO}, {@code WARN}, {@code ERROR}, {@code DEBUG}).</li>
     *   <li>{@code thread}        — name of the thread that emitted the log.</li>
     *   <li>{@code logger}        — fully-qualified class name of the logger.</li>
     *   <li>{@code message}       — log message text.</li>
     * </ul>
     */
    public static final String ENRICH_LOGS_SQL =
            "SELECT"                        + "\n" +
            "  log_timestamp,"              + "\n" +
            "  level,"                      + "\n" +
            "  thread,"                     + "\n" +
            "  logger,"                     + "\n" +
            "  message"                     + "\n" +
            "FROM parsed_logs_view"         + "\n" +
            "WHERE level != ''";            // discard lines that did not match the pattern

    /**
     * SQL query that filters only {@code ERROR}-level log entries.
     *
     * <p>Use this view when the consumer only needs to act on error conditions
     * (e.g. alerting, dead-letter routing).
     *
     * <p>Output columns: {@code log_timestamp}, {@code logger}, {@code message}.
     */
    public static final String ERROR_LOGS_SQL =
            "SELECT"                        + "\n" +
            "  log_timestamp,"              + "\n" +
            "  logger,"                     + "\n" +
            "  message"                     + "\n" +
            "FROM parsed_logs_view"         + "\n" +
            "WHERE level = 'ERROR'";
}
