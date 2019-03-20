package ind.rd.parquet.exception;

/**
 * Validates if the schema contains timestamp column
 */
public class SchemaValidationException extends Exception {
    public SchemaValidationException(String message) {
        super(message);
    }
}
