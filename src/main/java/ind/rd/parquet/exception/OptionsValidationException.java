package ind.rd.parquet.exception;

/**
 * Validates if the numerical parameters are suitable
 */
public class OptionsValidationException extends Exception {
    public OptionsValidationException(String message) {
        super(message);
    }
}
