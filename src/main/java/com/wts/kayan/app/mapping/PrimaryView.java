package com.wts.kayan.app.mapping;

/**
 * Holds the SQL query strings used by {@link PrimaryMapper} to transform data.
 *
 * <p>Keeping SQL strings in a dedicated class (rather than inlining them in the mapper)
 * makes them easy to find, test, and evolve independently of the execution logic.
 * This is the Java equivalent of the Scala {@code PrimaryView} object.
 *
 * <p><strong>Query explanation — {@link #GET_CLIENT_ORDER_SQL}:</strong>
 * <ol>
 *   <li><strong>BROADCAST hint</strong> — tells Spark to broadcast the smaller {@code clients_view}
 *       to every executor instead of shuffling both sides. This eliminates the most expensive
 *       shuffle in a join when one side is small enough to fit in memory.</li>
 *   <li><strong>LEFT JOIN</strong> — keeps every client row even if they have no orders yet.</li>
 *   <li><strong>SUM OVER PARTITION BY</strong> — a window function that computes the running
 *       total of orders per client without collapsing rows (unlike GROUP BY). Each output row
 *       keeps its individual order details <em>plus</em> the client-level total.</li>
 * </ol>
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public final class PrimaryView {

    // Prevent instantiation — this class is a SQL constant holder only.
    private PrimaryView() {}

    /**
     * SQL query that joins clients with orders, and adds a window-function column
     * with the total order amount per client.
     *
     * <p>The query reads from two temporary views registered by {@link PrimaryMapper}:
     * <ul>
     *   <li>{@code clients_view} — all clients (small dataset → broadcast candidate).</li>
     *   <li>{@code orders_view}  — orders from the latest date partition.</li>
     * </ul>
     *
     * <p>Output columns:
     * <ul>
     *   <li>{@code clientId}, {@code name}, {@code location} — from clients.</li>
     *   <li>{@code orderId}, {@code amount}, {@code date}    — from orders.</li>
     *   <li>{@code totalAmountByClient}                      — window aggregate.</li>
     * </ul>
     */
    public static final String GET_CLIENT_ORDER_SQL =
            "SELECT /*+ BROADCAST(c) */"                                         + "\n" +
            "  c.clientId,"                                                       + "\n" +
            "  c.name,"                                                           + "\n" +
            "  c.location,"                                                       + "\n" +
            "  o.orderId,"                                                        + "\n" +
            "  o.amount,"                                                         + "\n" +
            "  o.date,"                                                           + "\n" +
            // Window function: SUM partitioned by clientId — keeps all order rows.
            "  SUM(o.amount) OVER(PARTITION BY c.clientId) AS totalAmountByClient" + "\n" +
            "FROM"                                                                + "\n" +
            "  clients_view c"                                                    + "\n" +
            "LEFT JOIN"                                                           + "\n" +
            "  orders_view o"                                                     + "\n" +
            "ON"                                                                  + "\n" +
            "  c.clientId = o.clientId";
}
