package example.spring.kafka.recovery.consumer.exception;

public class PoisonPillException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private static final String MESSAGE_TEMPLATE = "Occur poison pill message , Message -> %s";

    public PoisonPillException(String poisonPillRecord) {
        super(String.format(MESSAGE_TEMPLATE, poisonPillRecord));
    }
}
