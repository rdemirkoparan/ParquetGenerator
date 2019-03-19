package ind.rd.parquet.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ind.rd.parquet.exception.SchemaValidationException;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@RunWith(Parameterized.class)
public class DataGeneratorTest {

    private BlockingQueue<Group> buffer = new LinkedBlockingQueue<>();

    int bufferLimit;
    int numberOfRecords;
    int expected;

    public DataGeneratorTest(int bufferLimit, int numberOfRecords, int expected) {
        this.bufferLimit = bufferLimit;
        this.numberOfRecords = numberOfRecords;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] data = {{1,1,1},{3,3,3},{9,9,9}};
        return Arrays.asList(data);
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreateGroup() throws SchemaValidationException {
        new DataGenerator(this.buffer, new Configuration(), null, this.bufferLimit,this.numberOfRecords).run();
        Assert.assertEquals(this.expected, this.numberOfRecords);
    }
}