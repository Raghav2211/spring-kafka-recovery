package example.spring.kafka.recovery.consumer.exception;

import lombok.Getter;

public class PoisonPillException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private static final String MESSAGE_TEMPLATE = "Occur poison pill message , Message -> %s";

    @Getter
    private final int sourcePartition;
    @Getter
    private final int sourceOffset;

    public PoisonPillException(String poisonPillRecord, int sourcePartition, int sourceOffset) {
        super(String.format(MESSAGE_TEMPLATE, poisonPillRecord));
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }
}
