package ind.rd.parquet;

import com.google.common.base.Stopwatch;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import ind.rd.parquet.exception.SchemaValidationException;
import ind.rd.parquet.generator.DataGenerator;
import ind.rd.parquet.util.Defaults;
import ind.rd.parquet.util.PartitionHelper;
import ind.rd.parquet.writer.DataWriter;
import ind.rd.parquet.writer.PartitionWriter;
import ind.rd.parquet.writer.impl.PartitionWriterImpl;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

//TODO mvn exec:java -Dexec.mainClass="ind.rd.parquet.PartitionedParquetGenerator" -Dexec.args="-r 999999 -l 10000 -i /tmp/x1/sample -s 24"
public class PartitionedParquetGenerator {

    private static final Logger LOGGER = Logger.getLogger(PartitionedParquetGenerator.class.getName());

//    private static final long bufferMemory = 1099511627776l;

    //TODO schema should be argument and use default in case no schema provided
    //TODO make all of the parameters parametric
    //TODO ignore warmup etc
    public static void main(String[] args) throws Exception {

        //TODO split option parser part from execution
        Options options = new Options();

        Option input = new Option("i", "inputSchema", true, "Input message schema");
        input.setRequired(false);
        options.addOption(input);

        Option output = new Option("o", "targetFileName", true, "Output file name");
        output.setRequired(false);
        options.addOption(output);

        Option threads = new Option("t", "threadCount", true, "Number of threads");
        threads.setRequired(false);
        options.addOption(threads);

        Option records = new Option("r", "numberOfRecords", true, "Number of records to generate");
        records.setRequired(false);
        options.addOption(records);

        Option limit = new Option("l", "bufferLimit", true, "Number of records to buffer");
        limit.setRequired(false);
        options.addOption(limit);

        Option memory = new Option("m", "memoryLimit", true, "Maximum memory buffer while partitioning");
        memory.setRequired(false);
        options.addOption(memory);

        Option interval = new Option("s", "partitionInterval", true, "Partitioning time interval as hours");
        interval.setRequired(false);
        options.addOption(interval);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            String inputSchema = cmd.getOptionValue("inputSchema");
            String targetFile = cmd.getOptionValue("targetFileName", Defaults.targetFileName);
            String threadCount = cmd.getOptionValue("threadCount", Defaults.threadCount);
            String numberOfRecords = cmd.getOptionValue("numberOfRecords", Defaults.numberOfRecords);
            String bufferLimit = cmd.getOptionValue("bufferLimit", Defaults.bufferLimit);
            String memoryLimit = cmd.getOptionValue("memoryLimit", Defaults.memoryLimit);
            String partitionInterval = cmd.getOptionValue("partitionInterval", Defaults.partitionInterval);

            //TODO add new validation exception and validation for options

            formatter.printHelp("PartitionedParquetGenerator", options);

            execute(inputSchema, targetFile, Integer.parseInt(threadCount), Integer.parseInt(numberOfRecords), Integer.parseInt(bufferLimit), memoryLimit, Integer.parseInt(partitionInterval));

        } catch (ParseException e) {
            System.out.println(e.getMessage());

            System.exit(1);
        }
    }

    private static void execute(String inputSchema, String targetFile, int threadCount, int numberOfRecords, int bufferLimit, String memoryLimit, int partitionInterval) throws SchemaValidationException, IOException {
        BlockingQueue<Group> buffer = new LinkedBlockingQueue<>();

        ExecutorService producerService = Executors.newFixedThreadPool(threadCount);
        ScheduledExecutorService consumerService = Executors.newScheduledThreadPool(threadCount);

        Configuration conf = new Configuration();
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOGGER.info("Producer service started!");
        producerService.execute(new DataGenerator(buffer, conf, inputSchema, bufferLimit, numberOfRecords));

        PartitionWriter<Group> partitionWriter = new PartitionWriterImpl<Group>(conf);

        Stopwatch stopwatchc = Stopwatch.createStarted();
        LOGGER.info("Consumer service scheduled!");
        consumerService.schedule(new DataWriter(buffer, partitionWriter, numberOfRecords), bufferLimit, TimeUnit.MILLISECONDS);

        producerService.shutdown();
        consumerService.shutdown();

        while (!producerService.isTerminated()) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "error", e);
            }
        }

        LOGGER.info("Producer service completed in " + stopwatch.stop());

        while (!consumerService.isTerminated()) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "error", e);
            }
        }

        LOGGER.info("Consumer service completed in " + stopwatchc.stop());

        try {
            stopwatch = Stopwatch.createStarted();
            LOGGER.info("Temporary ind.rd.parquet file persistence started!");
            ((PartitionWriterImpl) partitionWriter).close();
            LOGGER.info("Temporary ind.rd.parquet file persistence completed in " + stopwatch.stop());

            new PartitionHelper(threadCount, memoryLimit, partitionInterval, targetFile).partitionParquet();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
