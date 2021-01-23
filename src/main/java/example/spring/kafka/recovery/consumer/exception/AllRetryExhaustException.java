package example.spring.kafka.recovery.consumer.exception;

import lombok.Getter;

public class AllRetryExhaustException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static final String ALL_RETRY_EXHAUST_TEMPLATE = "All retries exhaust for parition %s offset %s, Message -> %s";

    @Getter
    private int sourcePartition;
    @Getter
    private int sourceOffset;

    public AllRetryExhaustException(int sourcePartition, int sourceOffset, String data) {
        super(String.format(ALL_RETRY_EXHAUST_TEMPLATE, sourcePartition, sourceOffset, data));
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }
}
