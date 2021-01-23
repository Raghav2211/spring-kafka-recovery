package example.spring.kafka.recovery.consumer.exception;

import lombok.Getter;

public class AllRetryExhaustException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static final String ALL_RETRY_EXHAUST_TEMPLATE = "All retries exhaust for topic %s parition %s, Message -> %s";

    @Getter
    private int partition;

    public AllRetryExhaustException(String topic, int partition, String data) {
        super(String.format(ALL_RETRY_EXHAUST_TEMPLATE, topic, partition, data));
        this.partition = partition;
    }
}
