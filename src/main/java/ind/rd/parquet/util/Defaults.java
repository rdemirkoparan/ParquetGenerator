package ind.rd.parquet.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Default values for application wide use
 */
public class Defaults {
    public static final int splitIntervalForHour = 3600000;
    public static final String targetFileName = "/tmp/par.out";
    public static final String threadCount = "4";
    public static final String numberOfRecords = "1000";
    public static final String bufferLimit = "200";
    public static final String memoryLimit = "1g";
    public static final String partitionInterval = "1";

    public static final String temporaryFileName;

    static {
        String tmp = "/tmp/tmp.par";
        try {
            File f = Files.createTempFile("tmp", "par").toFile();
            tmp = f.getAbsolutePath();
            f.deleteOnExit();
        } catch (IOException e) {
            //nothing to do..
        }
        temporaryFileName = tmp;
    }

    public static String readInputSchema(String filePath) {

        String defaultSchema = "message sample { "
                + "required int64 timestamp;"
                + "required binary binary_field; "
                + "required int32 int32_field; "
                + "required int64 int64_field; "
                + "required boolean boolean_field; "
                + "required float float_field; "
                + "required double double_field; "
                + "required int96 int96_field; "
//                + "optional fixed_len_byte_array(3) flba_field; "
                + "} ";
        try {
            if(null != filePath && !filePath.isEmpty()){
                defaultSchema = new String(Files.readAllBytes(Paths.get(filePath)));
            }
        } catch (IOException e) {
            //do nothing
        }
        return defaultSchema;
    }
}
