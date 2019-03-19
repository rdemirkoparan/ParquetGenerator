package ind.rd.parquet.writer.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import ind.rd.parquet.util.Defaults;
import ind.rd.parquet.util.PartitionedParquetWriter;
import ind.rd.parquet.writer.PartitionWriter;

import java.io.IOException;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;


//TODO add microbenchmark
//TODO add logging
public class PartitionWriterImpl<T>  implements PartitionWriter {

    private final ParquetWriter<Group> writer;

    public PartitionWriterImpl(Configuration conf) throws IOException {
        this.writer = new PartitionedParquetWriter(new Path(Defaults.temporaryFileName))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(UNCOMPRESSED)
                .withPageSize(1024)
                .withConf(conf)
                .withDictionaryPageSize(512)
                .withMaxPaddingSize(8388608)
                .withRowGroupSize(1048576)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .enableDictionaryEncoding()
                .build();
    }

    public void write(Object rec) throws IOException {
        this.writer.write((Group) rec);
    }

    public void close() throws IOException {
        this.writer.close();
    }
}





/*

package ind.rd.parquet.writer.impl;

import com.google.common.base.Stopwatch;
import org.apache.spark.sql.*;
import ind.rd.parquet.util.Defaults;
import ind.rd.parquet.writer.PartitionWriter;

import java.io.IOException;
import java.util.logging.Logger;


//TODO add microbenchmark
//TODO add logging
public class PartitionWriterImpl implements PartitionWriter {

    private static final Logger LOGGER = Logger.getLogger(PartitionWriterImpl.class.getName());

    private static int threadCount;
    private static String memory;
    static int numHours;
    static String outFileName;

    public PartitionWriterImpl(int threadCount,String memory, int numHours, String outFileName) {
        this.threadCount = threadCount;
        this.memory = memory;
        this.numHours = numHours;
        this.outFileName = outFileName;
    }

    //TODO make parametric all of the below

    //parameter
    public void write(Object rec) throws IOException {

        Stopwatch stopwatch = Stopwatch.createStarted();
        LOGGER.info("SparkSession creation started!");
        final SparkSession sparkSession = SparkSession
                .builder()
                .master("local[" + threadCount + "]")
                .appName("Parquet Generator")
                .config("spark.executor.memory", memory)
                .config("spark.network.timeout", "600s")
                .config("spark.rpc.askTimeout", "600s")
                .getOrCreate();
        LOGGER.info("SparkSession creation completed in " + stopwatch.stop());

        SQLContext sqlContext = new SQLContext(sparkSession);

        stopwatch = Stopwatch.createStarted();
        LOGGER.info("Partitioning started!");
        sqlContext
                .read()
                .ind.rd.parquet((String) rec)
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
                .ind.rd.parquet("file://" + outFileName);
        LOGGER.info("Partitioning completed in " + stopwatch.stop());

        //TODO remove this validation
        stopwatch = Stopwatch.createStarted();
        LOGGER.info("Data read started!");
        Dataset<Row> tp = sqlContext.read().ind.rd.parquet("file://" + outFileName);
        LOGGER.info("Total number of records : " + tp.count());
        tp.show();
        LOGGER.info("Data read completed in " + stopwatch.stop());

        sparkSession.stop();
    }
}


*/
