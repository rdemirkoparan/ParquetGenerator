package ind.rd.parquet.generator;

import com.google.common.base.Stopwatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import ind.rd.parquet.exception.SchemaValidationException;
import ind.rd.parquet.util.Defaults;
import ind.rd.parquet.util.SchemaHelper;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

//TODO thread, buffer, etc performance related stuffs
public class DataGenerator implements Runnable {
    private BlockingQueue<Group> queue;
    private int numberOfRecords;
    private int buffer;
    private SimpleGroupFactory fact;
    private SchemaHelper helper;
    private static final Date date = new Date();

    private static final Logger LOGGER = Logger.getLogger(DataGenerator.class.getName());
    private Random random;

    public DataGenerator(BlockingQueue<Group> queue, Configuration conf, String messageSchema, int buffer, int numberOfRecords) throws SchemaValidationException {
        this.queue = queue;
        this.numberOfRecords = numberOfRecords;
        this.buffer = buffer;

        MessageType schema = parseMessageType(Defaults.readInputSchema(messageSchema));
        GroupWriteSupport.setSchema(schema, conf);
        this.fact = new SimpleGroupFactory(schema);
        this.helper = new SchemaHelper(schema);
        this.random = new Random();
    }

    @Override
    public void run() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOGGER.info("DataGenerator task started!");
        while (numberOfRecords > 0) {
            synchronized (queue) {
                while (queue.size() >= buffer) {
                    try {
                        //DataGenerator paused until there is a free room at Buffer. Buffered objects are consuming by DataWriter
                        queue.wait();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
                Group r = createGroup();
                try {
                    queue.put(r);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                numberOfRecords--;
                queue.notifyAll();
            }
        }
        LOGGER.info("DataGenerator task completed in " + stopwatch.stop());
    }

    private Group createGroup() {
        Group r = fact.newGroup();
        helper.getFields().forEach(fieldDescriptor -> {
            switch (fieldDescriptor.getType()) {
                case FIXED_LEN_BYTE_ARRAY:
//                    byte[] b = new byte[fieldDescriptor.getTypeLength()];
//                    random.nextBytes(b);
//                    String s = Binary.fromConstantByteArray(b).toString();
//                    System.out.println(fieldDescriptor.getTypeLength() + "" + fieldDescriptor.getName() + " " + s);
//                    r.append(fieldDescriptor.getName(), Binary.fromString("test"));
//                    break;
                case BINARY:
                case INT96:
                    byte[] b = new byte[12];
                    random.nextBytes(b);
                    r.append(fieldDescriptor.getName(), Binary.fromConstantByteArray(b));
                    break;
                case INT32:
                    r.append(fieldDescriptor.getName(), fieldDescriptor.hashCode() + queue.size());
                    break;
                case INT64:
                    // special and DIRTY treatment for long. It may be timestamp. FIND a better way (getting TIMESTAMP_MILLIS info etc)
                    if (fieldDescriptor.getName().contains("timestamp")) {
                        r.append(fieldDescriptor.getName(), date.getTime() + (random.nextInt() * 117));
                    } else {
                        r.append(fieldDescriptor.getName(), random.nextLong() & Long.MAX_VALUE);
                    }
                    break;
                case FLOAT:
                    r.append(fieldDescriptor.getName(), random.nextFloat());
                    break;
                case DOUBLE:
                    r.append(fieldDescriptor.getName(), random.nextDouble());
                    break;
                case BOOLEAN:
                    r.append(fieldDescriptor.getName(), random.nextBoolean());
                    break;
            }
        });
        return r;
    }


}
