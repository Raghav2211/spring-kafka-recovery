package example.spring.kafka.recovery.consumer.exception;

public class RetryException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    public static final String RETRY_TEMPLATE = "Consume Retry message , Message -> %s";

    public RetryException(String retryRecord) {
        super(String.format(RETRY_TEMPLATE, retryRecord));
    }
}
