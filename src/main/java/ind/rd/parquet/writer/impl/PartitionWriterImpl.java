package ind.rd.parquet.writer.impl;

import ind.rd.parquet.util.Defaults;
import ind.rd.parquet.writer.PartitionedParquetWriter;
import ind.rd.parquet.writer.PartitionWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;


//Implementation of the writer
public class PartitionWriterImpl<Group>  implements PartitionWriter {

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

    @Override
    public void write(Object rec) throws IOException {
        this.writer.write((Group) rec);
    }

    public void close() throws IOException {
        this.writer.close();
    }
}
