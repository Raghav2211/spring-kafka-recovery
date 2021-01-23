package example.spring.kafka.recovery.consumer.exception;

import lombok.Getter;

public class RetryException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    public static final String RETRY_TEMPLATE = "Consume Retry message , Message -> %s";

    @Getter
    private final int sourcePartition;
    @Getter
    private final int sourceOffset;

    public RetryException(String retryRecord, int sourcePartition, int sourceOffset) {
        super(String.format(RETRY_TEMPLATE, retryRecord));
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }
}
