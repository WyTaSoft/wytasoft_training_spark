package com.wts.kayan.app.utility;

/**
 * Centralizes the constant values used throughout the Spark application.
 *
 * <p>Keeping constants in one place avoids magic strings scattered across the codebase
 * and makes renaming or reconfiguring datasets a one-line change.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public final class PrimaryConstants {

    // Prevent instantiation — this class is a constants holder only.
    private PrimaryConstants() {}

    /** Name shown in the Spark UI and application logs. */
    public static final String APPLICATION_NAME = "training_spark_java";

    /** Identifier for the clients dataset. */
    public static final String CLIENTS = "clients";

    /** Identifier for the orders dataset. */
    public static final String ORDERS = "orders";

    /** Write mode: overwrite existing data. */
    public static final String MODE_OVERWRITE = "overwrite";

    /** Write mode: append to existing data. */
    public static final String MODE_APPEND = "append";
}
