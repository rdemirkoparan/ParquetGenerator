package ind.rd.parquet.util;

import ind.rd.parquet.exception.OptionsValidationException;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

/**
 * Parses the user inputs
 */
public class OptionsParser {
    private String[] args;
    private String inputSchema;
    private String targetFile;
    private String threadCount;
    private String numberOfRecords;
    private String bufferLimit;
    private String memoryLimit;
    private String partitionInterval;

    public OptionsParser(String... args) {
        this.args = args;
    }

    public String getInputSchema() {
        return inputSchema;
    }

    public String getTargetFile() {
        return targetFile;
    }

    public String getThreadCount() {
        return threadCount;
    }

    public String getNumberOfRecords() {
        return numberOfRecords;
    }

    public String getBufferLimit() {
        return bufferLimit;
    }

    public String getMemoryLimit() {
        return memoryLimit;
    }

    public String getPartitionInterval() {
        return partitionInterval;
    }

    public OptionsParser invoke() throws ParseException, OptionsValidationException {
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
        CommandLine cmd = parser.parse(options, args);

        inputSchema = cmd.getOptionValue("inputSchema");
        targetFile = cmd.getOptionValue("targetFileName", Defaults.targetFileName);
        threadCount = cmd.getOptionValue("threadCount", Defaults.threadCount);
        numberOfRecords = cmd.getOptionValue("numberOfRecords", Defaults.numberOfRecords);
        bufferLimit = cmd.getOptionValue("bufferLimit", Defaults.bufferLimit);
        memoryLimit = cmd.getOptionValue("memoryLimit", Defaults.memoryLimit);
        partitionInterval = cmd.getOptionValue("partitionInterval", Defaults.partitionInterval);

        validateOptions();

        formatter.printHelp("PartitionedParquetGenerator", options);
        return this;
    }

    private void validateOptions() throws OptionsValidationException {
        if(!StringUtils.isEmpty(threadCount) && !StringUtils.isNumeric(threadCount)){
            throw new OptionsValidationException("Thread Count must be a valid integer!");
        }
        if(!StringUtils.isEmpty(numberOfRecords) && !StringUtils.isNumeric(numberOfRecords)){
            throw new OptionsValidationException("Number of Records must be a valid integer!");
        }
        if(!StringUtils.isEmpty(bufferLimit) && !StringUtils.isNumeric(bufferLimit)){
            throw new OptionsValidationException("Buffe Limit must be a valid integer!");
        }
        if(!StringUtils.isEmpty(partitionInterval) && !StringUtils.isNumeric(partitionInterval)){
            throw new OptionsValidationException("Partitioning must be a valid integer!");
        }
    }
}
