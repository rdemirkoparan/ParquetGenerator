package ind.rd.parquet.writer;

import com.google.common.base.Stopwatch;
import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;


public class DataWriter implements Runnable {
    private BlockingQueue<Group> queue;
    private int numberOfRecords;
    private PartitionWriter<Group> partitionWriter;

    private static final Logger LOGGER = Logger.getLogger(DataWriter.class.getName());

    public DataWriter(BlockingQueue<Group> queue, PartitionWriter<Group> partitionWriter, int numberOfRecords) {
        this.queue = queue;
        this.partitionWriter =  partitionWriter;
        this.numberOfRecords = numberOfRecords;
    }

    @Override
    public void run() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOGGER.info("DataWriter task started!");
        while (numberOfRecords > 0) {
            synchronized (queue) {
                while (queue.isEmpty()) {
                    try {
                        queue.wait();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }

                try {
                    synchronized (partitionWriter) {
                        partitionWriter.write(queue.take());
                    }
                    numberOfRecords--;
                    queue.notifyAll();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        LOGGER.info("DataWriter task completed in " + stopwatch.stop());
    }
}
