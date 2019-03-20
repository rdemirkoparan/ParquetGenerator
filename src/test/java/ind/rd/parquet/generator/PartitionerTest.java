package ind.rd.parquet.generator;

import ind.rd.parquet.util.Defaults;
import ind.rd.parquet.writer.PartitionedParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;


@RunWith(Parameterized.class)
public class PartitionerTest {

    int numberOfRow;
    long timestamp;
    long expected;

    ParquetWriter<Group> writer;
    SimpleGroupFactory fact;
    static SparkSession sparkSession;
    SQLContext sqlContext;

    static final String outFileName = "/tmp/par.out";
    byte[] b;

    public PartitionerTest(int numberOfRow, long timestamp, long expected) {
        this.numberOfRow = numberOfRow;
        this.timestamp = timestamp;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        long t1 = new Date().getTime() * new Random().nextInt(200);
        long t2 = new Date().getTime() * new Random().nextInt(200);
        long t3 = new Date().getTime() * new Random().nextInt(200);
        Object[][] data = {{10,t1,t1},{20,t2,t2},{40,t3,t3}};
        return Arrays.asList(data);
    }

    @BeforeClass
    public static void setUpSpark() {
        sparkSession = SparkSession
                .builder()
                .master("local[" + Runtime.getRuntime().availableProcessors() + "]")
                .appName("Parquet Generator")
                .config("spark.executor.memory", "1g")
                .config("spark.network.timeout", "600s")
                .config("spark.rpc.askTimeout", "600s")
                .config("spark.sql.parquet.writeLegacyFormat", true)
                .getOrCreate();
    }

    @Before
    public void setUp() throws Exception {
        b = new byte[12];
        new Random().nextBytes(b);

        Configuration conf = new Configuration();
        MessageType schema = parseMessageType(
                "message test { "
                        + "required int64 timestamp;"
                        + "required binary binary_field; "
                        + "required int32 int32_field; "
                        + "} ");
        GroupWriteSupport.setSchema(schema, conf);
        this.fact = new SimpleGroupFactory(schema);

        writer = new PartitionedParquetWriter(new Path(Defaults.temporaryFileName))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withPageSize(1024)
                .withConf(conf)
                .withDictionaryPageSize(512)
                .withMaxPaddingSize(8388608)
                .withRowGroupSize(1048576)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .enableDictionaryEncoding()
                .build();

        for (int i = 0; i < numberOfRow; i++) {
            writer.write(
                    fact.newGroup()
                            .append("timestamp", timestamp)
                            .append("binary_field", Binary.fromConstantByteArray(b))
                            .append("int32_field", 32));
        }
        writer.close();

        sqlContext = new SQLContext(sparkSession);
    }

    @AfterClass
    public static void tearDownSpark() throws IOException {

        sparkSession.stop();

        Files.walk(java.nio.file.Paths.get(outFileName))
                .sorted(Comparator.reverseOrder())
                .map(java.nio.file.Path::toFile)
                .forEach(File::delete);
    }

    @Test
    public void testPartition() {
        sqlContext
                .read()
                .parquet(Defaults.temporaryFileName)
                .withColumn("interval",
                        functions
                                .lit(
                                        functions.from_unixtime(functions
                                                .col("timestamp")
                                                .minus(functions.col("timestamp").mod(Defaults.splitIntervalForHour))
                                                .cast("long").divide(1000), "yyyy-MM-dd'T'HH")

                                )
                )
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("interval")
                .parquet("file://" + outFileName);


        Dataset<Row> tp = sqlContext.read().parquet("file://" + outFileName);

        Assert.assertEquals(expected, tp.toJavaRDD().first().getLong(0));
        Assert.assertEquals(numberOfRow, tp.count());

    }
}