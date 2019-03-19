package ind.rd.parquet.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

public class PartitionedParquetWriter extends ParquetWriter.Builder {

    public PartitionedParquetWriter(Path path) {
        super(path);
    }

    @Override
    protected ParquetWriter.Builder self() {
        return this;
    }

    @Override
    protected WriteSupport getWriteSupport(Configuration configuration) {
        return new GroupWriteSupport();
    }
}
