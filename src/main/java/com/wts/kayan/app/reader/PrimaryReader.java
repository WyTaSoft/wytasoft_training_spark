package com.wts.kayan.app.reader;

import com.wts.kayan.app.utility.PrimaryConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for loading source data into Spark {@link Dataset}s.
 *
 * <p>In this first unit (ud01) the reader handles the <strong>clients</strong> table only.
 * The schema is defined explicitly so Spark does not have to infer it by scanning the file,
 * which is both faster and safer in production pipelines.
 *
 * <p>Java equivalent of the Scala {@code PrimaryReader} class. Because Java lacks implicit
 * parameters, the SparkSession and the environment string are passed via the constructor.
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">WyTaSoft — courses and training sessions</a>
 */
public class PrimaryReader {

    private static final Logger log = LoggerFactory.getLogger(PrimaryReader.class);

    // SparkSession is injected at construction time — no static access.
    private final SparkSession sparkSession;

    // The environment tag (e.g. "dev", "test", "prod") used to resolve data paths.
    private final String env;

    /**
     * Constructs a PrimaryReader.
     *
     * @param sparkSession Active Spark session.
     * @param env          Runtime environment identifier.
     */
    public PrimaryReader(SparkSession sparkSession, String env) {
        this.sparkSession = sparkSession;
        this.env = env;
    }

    // -------------------------------------------------------------------------
    // Schema definitions
    // -------------------------------------------------------------------------

    /**
     * Schema for the clients CSV file.
     *
     * <p>Defining the schema explicitly is a best practice: it avoids the extra scan
     * that Spark would otherwise do to infer types, and it documents the contract of
     * the data source clearly inside the code.
     *
     * <ul>
     *   <li>clientId  — integer primary key.</li>
     *   <li>name      — full name of the client.</li>
     *   <li>location  — city or region of the client.</li>
     * </ul>
     *
     * @return {@link StructType} describing the clients table.
     */
    private StructType clientsSchema() {
        // StructType is the Java/Spark equivalent of Scala's StructType in SchemaSelector.
        return new StructType(new StructField[]{
                // DataTypes.createStructField(name, dataType, nullable)
                DataTypes.createStructField("clientId", DataTypes.IntegerType, true),
                DataTypes.createStructField("name",     DataTypes.StringType,  true),
                DataTypes.createStructField("location", DataTypes.StringType,  true)
        });
    }

    // -------------------------------------------------------------------------
    // Public reader methods
    // -------------------------------------------------------------------------

    /**
     * Reads the clients CSV file and returns it as a typed {@link Dataset}.
     *
     * <p>Options used:
     * <ul>
     *   <li>{@code header=true}  — the first row of the CSV is treated as column names.</li>
     *   <li>{@code delimiter=,} — fields are separated by commas (standard CSV).</li>
     * </ul>
     *
     * @return {@link Dataset}{@literal <}{@link Row}{@literal >} representing all clients.
     */
    public Dataset<Row> readClients() {
        log.info("\n**** Reading clients DataFrame ****\n");

        // Relative path — works when the application is launched from the project root.
        String inputPath = "src/main/resources/data/clients/";

        return sparkSession.read()
                .schema(clientsSchema())       // Apply the explicit schema — avoids inferSchema scan.
                .option("header", "true")      // First line is the header row.
                .option("delimiter", ",")      // Comma-separated values.
                .csv(inputPath);               // Load all CSV files in the directory.
    }
}
