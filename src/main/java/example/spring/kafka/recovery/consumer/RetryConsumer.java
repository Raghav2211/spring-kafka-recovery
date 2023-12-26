package example.spring.kafka.recovery.consumer;

import example.spring.kafka.recovery.consumer.BootstrapConsumer.RecordType;
import example.spring.kafka.recovery.consumer.exception.RetriableException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RetryConsumer {

  public static enum FailRecordType {
    RETRY_SUCCESS,
    RETRY_FAIL
  }

  //  @KafkaListener(
  //      topics = "${app.kafka.inbound.retry.topic}",
  //      groupId = "${app.kafka.inbound.retry.groupId}",
  //      containerFactory = "kafkaRetryListenerContainerFactory")
  public void onReceiving(
      String data,
      @Header(KafkaHeaders.OFFSET) Integer offset,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
      Acknowledgment ack) {
    processRecord(data, topic, partition, offset);
  }

  private void processRecord(String data, String topic, int partition, int offset) {
    if (data.contains(RecordType.RETRY_SUCCESS.name())) {
      log.info(
          "Data successfully consumed in retry, topic = {}, partition = {}, offset = {}, data = {}",
          topic,
          partition,
          offset,
          data);
    } else if (data.contains(RecordType.RETRY_FAIL.name())) {
      log.info(
          "Retry fail data consumed in retry , topic = {}, partition = {}, offset = {}, success data = {}",
          topic,
          partition,
          offset,
          data);
      throw new RetriableException(RecordType.RETRY_FAIL, partition, offset);
    } else {
      log.info(
          "Unknown message retrieve , Message should be in {} ",
          Arrays.asList(FailRecordType.values()));
    }
  }
}
