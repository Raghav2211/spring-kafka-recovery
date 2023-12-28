package example.spring.kafka.recovery.consumer.exception;

import lombok.Getter;
import reactor.kafka.receiver.ReceiverRecord;

public class PoisonPillException extends RuntimeException {

  /** */
  private static final long serialVersionUID = 1L;

  @Getter private final ReceiverRecord<?, ?> record;

  public PoisonPillException(ReceiverRecord<?, ?> record) {
    super("This is a poison pill");
    this.record = record;
  }

  public PoisonPillException(ReceiverRecord<?, ?> record, Throwable throwable) {
    super(throwable);
    this.record = record;
  }
}
