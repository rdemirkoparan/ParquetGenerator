package ind.rd.parquet.generator;

import ind.rd.parquet.writer.DataWriter;
import ind.rd.parquet.writer.PartitionWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ind.rd.parquet.exception.SchemaValidationException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.*;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DataGeneratorAndWriterTest {

    private BlockingQueue<Group> buffer = new LinkedBlockingQueue<>();

    int bufferLimit;
    int numberOfRecords;
    int expected;

    PartitionWriter mockPartitionWriter;

    ExecutorService producerService;
    ScheduledExecutorService consumerService;

    public DataGeneratorAndWriterTest(int bufferLimit, int numberOfRecords, int expected) {
        this.bufferLimit = bufferLimit;
        this.numberOfRecords = numberOfRecords;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] data = {{1,1,0},{2,3,0},{4,9,0}};
        return Arrays.asList(data);
    }

    @Before
    public void setUp() {
        mockPartitionWriter = Mockito.mock(PartitionWriter.class);

        producerService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        consumerService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @After
    public void tearDown() throws Exception {
        producerService.awaitTermination(100, TimeUnit.MILLISECONDS);
        consumerService.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testCreateAndConsumeGroup() throws SchemaValidationException, InterruptedException {
        producerService.execute(new DataGenerator(this.buffer, new Configuration(), null, this.bufferLimit,this.numberOfRecords));
        consumerService.schedule(new DataWriter(buffer, mockPartitionWriter, numberOfRecords), bufferLimit, TimeUnit.MILLISECONDS);

        producerService.shutdown();
        consumerService.shutdown();

        while (!producerService.isTerminated()) { Thread.sleep(50); }
        while (!consumerService.isTerminated()) { Thread.sleep(50); }

        Assert.assertEquals(this.expected, this.buffer.size());
    }
}