package example.spring.kafka.recovery.consumer.exception;

import example.spring.kafka.recovery.consumer.BootstrapConsumer.RecordType;
import lombok.Getter;

public class RetriableException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  public static final String RETRY_TEMPLATE = "Consume Retry message , Message -> %s";

  @Getter private final int sourcePartition;
  @Getter private final int sourceOffset;
  @Getter private RecordType recordType;

  public RetriableException(RecordType retryRecord, int sourcePartition, int sourceOffset) {
    super(String.format(RETRY_TEMPLATE, retryRecord.name()));
    this.recordType = retryRecord;
    this.sourcePartition = sourcePartition;
    this.sourceOffset = sourceOffset;
  }
}
