package ind.rd.parquet.writer;

import java.io.IOException;

/**
 * default interface for writer
 * @param <T>
 */
public interface PartitionWriter<T> {
    void write(T rec) throws IOException;
}
