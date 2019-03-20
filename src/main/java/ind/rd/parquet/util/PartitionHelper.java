package ind.rd.parquet.util;

import com.google.common.base.Stopwatch;
import org.apache.spark.sql.*;

import java.util.logging.Logger;

/**
 * This class uses spark to create partitions from the temporary file
 */
public class PartitionHelper {

    private static final Logger LOGGER = Logger.getLogger(PartitionHelper.class.getName());

    private final int threadCount;
    private final String memory;
    private final int numHours;
    private final String outFileName;

    public PartitionHelper(int threadCount,String memory, int numHours, String outFileName) {
        this.threadCount = threadCount;
        this.memory = memory;
        this.numHours = numHours;
        this.outFileName = outFileName;
    }

    public void partitionParquet(){
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOGGER.info("SparkSession creation started!");
        //create spark session with thread and memory parameters. other configs are for small data related fixes
        final SparkSession sparkSession = SparkSession
                .builder()
                .master("local[" + threadCount + "]")
                .appName("Parquet Generator")
                .config("spark.executor.memory", memory)
                .config("spark.network.timeout", "600s")
                .config("spark.rpc.askTimeout", "600s")
                .config("spark.sql.parquet.writeLegacyFormat", true)
                .getOrCreate();
        LOGGER.info("SparkSession creation completed in " + stopwatch.stop());

        SQLContext sqlContext = new SQLContext(sparkSession);

        stopwatch = Stopwatch.createStarted();
        LOGGER.info("Partitioning started!");
        //read from temporary parquet file, add interval column according to user input and write partitioned data to the file system with interval renaming
        sqlContext
                .read()
                .parquet(Defaults.temporaryFileName)
                .withColumn("interval",
                        functions
                                .lit(
                                        functions.from_unixtime(functions
                                                .col("timestamp")
                                                .minus(functions.col("timestamp").mod(Defaults.splitIntervalForHour * numHours))
                                                .cast("long").divide(1000), "yyyy-MM-dd'T'HH")

                                )
                )
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("interval")
                .parquet("file://" + outFileName);
        LOGGER.info("Partitioning completed in " + stopwatch.stop());

        //this part is for validation of the written records. it may be opened to see the output or closed to save resources
        stopwatch = Stopwatch.createStarted();
        LOGGER.info("Data read started!");
        Dataset<Row> tp = sqlContext.read().parquet("file://" + outFileName);
        LOGGER.info("Total number of records : " + tp.count());
        tp.show();
        LOGGER.info("Data read completed in " + stopwatch.stop());

        sparkSession.stop();
    }
}
