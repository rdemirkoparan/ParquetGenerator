package ind.rd.parquet;

import com.google.common.base.Stopwatch;
import ind.rd.parquet.exception.SchemaValidationException;
import ind.rd.parquet.generator.DataGenerator;
import ind.rd.parquet.util.OptionsParser;
import ind.rd.parquet.util.PartitionHelper;
import ind.rd.parquet.writer.DataWriter;
import ind.rd.parquet.writer.PartitionWriter;
import ind.rd.parquet.writer.impl.PartitionWriterImpl;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Entry class of the application
 */
public class PartitionedParquetGenerator {

    private static final Logger LOGGER = Logger.getLogger(PartitionedParquetGenerator.class.getName());
    private static final int WAIT_TIME = 200;

    public static void main(String[] args) throws Exception {

        try {
            OptionsParser optionsParser = new OptionsParser(args).invoke();

            String inputSchema = optionsParser.getInputSchema();
            String targetFile = optionsParser.getTargetFile();
            String threadCount = optionsParser.getThreadCount();
            String numberOfRecords = optionsParser.getNumberOfRecords();
            String bufferLimit = optionsParser.getBufferLimit();
            String memoryLimit = optionsParser.getMemoryLimit();
            String partitionInterval = optionsParser.getPartitionInterval();

            execute(inputSchema, targetFile, Integer.parseInt(threadCount), Integer.parseInt(numberOfRecords), Integer.parseInt(bufferLimit), memoryLimit, Integer.parseInt(partitionInterval));

        } catch (ParseException e) {
            LOGGER.severe(e.getMessage());

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

        PartitionWriter<Group> partitionWriter = new PartitionWriterImpl<>(conf);

        Stopwatch stopwatchc = Stopwatch.createStarted();
        LOGGER.info("Consumer service scheduled!");
        consumerService.schedule(new DataWriter(buffer, partitionWriter, numberOfRecords), bufferLimit, TimeUnit.MILLISECONDS);

        producerService.shutdown();
        consumerService.shutdown();

        while (!producerService.isTerminated()) {
            try {
                Thread.sleep(WAIT_TIME);
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "error", e);
            }
        }

        LOGGER.info("Producer service completed in " + stopwatch.stop());

        while (!consumerService.isTerminated()) {
            try {
                Thread.sleep(WAIT_TIME);
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
