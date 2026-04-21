package com.wts.kayan.app.utility;

/**
 * Centralises the column selection logic for each source dataset.
 *
 * <p>Having a single place to declare which columns are selected per table makes it easy
 * to add, remove, or rename columns without hunting through multiple reader classes.
 * It is the Java equivalent of the Scala {@code ColumnSelector} object.
 *
 * <p>The returned arrays are consumed by {@code Dataset.selectExpr(...)} inside
 * {@link PrimaryUtilities#readDataFrame}.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public final class ColumnSelector {

    // Prevent instantiation — all access is via static methods.
    private ColumnSelector() {}

    /**
     * Returns the ordered list of columns to select for the given dataset.
     *
     * <p>Column names are lower-cased to match the CSV headers after Spark normalises them.
     * Adding a column alias here (e.g. {@code "clientid as client_id"}) is the right place
     * to introduce naming conventions without touching reader or mapping code.
     *
     * @param tableName Dataset identifier — expected values are {@link PrimaryConstants#CLIENTS}
     *                  or {@link PrimaryConstants#ORDERS}.
     * @return Array of column-expression strings ready for {@code selectExpr}.
     * @throws IllegalArgumentException if {@code tableName} is not recognised.
     */
    public static String[] getColumnSequence(String tableName) {

        switch (tableName.toLowerCase()) {

            case PrimaryConstants.CLIENTS:
                return new String[]{
                        "clientid",   // Unique identifier for the client.
                        "name",       // Full name of the client.
                        "location"    // Geographical location of the client.
                };

            case PrimaryConstants.ORDERS:
                return new String[]{
                        "orderid",    // Unique identifier for the order.
                        "clientid",   // Client identifier — links an order to a client.
                        "amount",     // Monetary value of the order.
                        "date"        // Date the order was placed (partition column).
                };

            default:
                throw new IllegalArgumentException(
                        "Unknown table: " + tableName + ". Expected 'clients' or 'orders'."
                );
        }
    }
}
