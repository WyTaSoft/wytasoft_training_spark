package com.wts.kayan.app.utility;

/**
 * Centralises the constant values used throughout the Spark Structured Streaming application.
 *
 * <p>Mirrors {@link PrimaryConstants} for the batch ETL project — one class, one place to
 * update if identifiers or the log-line pattern ever change.
 *
 * <p><strong>Log line format expected by {@link #LOG_PATTERN}:</strong>
 * <pre>
 *   2024-01-15 08:00:00.001 INFO  [main] com.wts.kayan.app.job.StreamingDriver - Message text
 * </pre>
 * Capture groups (used with {@code regexp_extract}):
 * <ol>
 *   <li>Timestamp  — {@code yyyy-MM-dd HH:mm:ss.SSS}</li>
 *   <li>Level      — {@code INFO | WARN | ERROR | DEBUG}</li>
 *   <li>Thread     — text inside {@code [...]}</li>
 *   <li>Logger     — fully-qualified class name</li>
 *   <li>Message    — everything after {@code " - "}</li>
 * </ol>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public final class StreamingConstants {

    private StreamingConstants() {}

    /** Name shown in the Spark UI and application logs. */
    public static final String APPLICATION_NAME = "training_spark_streaming_java";

    /** Logical identifier for the logs dataset. */
    public static final String LOGS = "logs";

    /**
     * Regex that parses a single log line into five capture groups.
     *
     * <p>The pattern is written with double-escaped backslashes so that Java's
     * string literal produces a single backslash for the regex engine, which in
     * turn treats them as regex meta-characters (e.g. {@code \\d} → {@code \d}).
     */
    public static final String LOG_PATTERN =
            "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})" // group 1 — timestamp
            + "\\s+(INFO|WARN|ERROR|DEBUG)"                            // group 2 — level
            + "\\s+\\[([^\\]]+)\\]"                                   // group 3 — thread
            + "\\s+(\\S+)"                                             // group 4 — logger
            + "\\s+-\\s+(.+)$";                                        // group 5 — message

    /** Streaming sink: write to the driver console (dev / debugging). */
    public static final String SINK_CONSOLE = "console";

    /** Streaming sink: write to Parquet files (prod). */
    public static final String SINK_PARQUET = "parquet";

    /** Streaming output mode: emit only new rows (event-by-event). */
    public static final String OUTPUT_MODE_APPEND = "append";
}
