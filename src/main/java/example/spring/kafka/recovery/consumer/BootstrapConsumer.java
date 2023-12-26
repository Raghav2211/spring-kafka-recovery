package example.spring.kafka.recovery.consumer;

import example.spring.kafka.recovery.consumer.exception.PoisonPillException;
import example.spring.kafka.recovery.consumer.exception.RetriableException;
import java.time.Duration;
import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

@Service
@Slf4j
@RequiredArgsConstructor
public class BootstrapConsumer {
  private final KafkaReceiver<String, String> bootStrapKafkaReceiver;

  public static enum RecordType {
    SUCCESS,
    POISON_PILL,
    RETRY_SUCCESS,
    RETRY_FAIL
  }

  @EventListener(ApplicationReadyEvent.class)
  Disposable bootstrapKafkaConsumer() {
    return bootStrapKafkaReceiver
        .receive()
        // To handle the transient errors like re-balancing, connection timeout
        .doOnError(error -> log.error("Error receiving event, will retry in 1 minute", error))
        .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMinutes(1)))
        // use concatMap instead of flatMapSequential if we need can;t cope with consumer crashes
        .flatMapSequential(this::handleEvent)
        .subscribe(record -> record.receiverOffset().acknowledge());
  }

  private Mono<ReceiverRecord<String, String>> handleEvent(ReceiverRecord<String, String> record) {
    return processMessages(
            record.value(), record.receiverOffset().offset(), record.partition(), record.topic())
        .onErrorResume(ex -> Mono.empty())
        .thenReturn(record);
  }

  private Mono<String> processMessages(String data, long offset, int partition, String topic) {
    var dataTrimUpperCase = data.toUpperCase().trim();
    if (dataTrimUpperCase.equals(RecordType.SUCCESS.name())) {
      log.info(
          "Data successfully consumed topic = {}, partition = {}, offset = {}, data = {}",
          topic,
          partition,
          offset,
          data);
      return Mono.just(data);
    } else if (dataTrimUpperCase.equals(RecordType.POISON_PILL.name())) {
      log.info(
          "Poison pill data consumed topic = {}, partition = {}, offset = {}, data = {}",
          topic,
          partition,
          offset,
          data);
      return Mono.error(new PoisonPillException(data, partition, (int) offset));
    } else if (dataTrimUpperCase.equals(RecordType.RETRY_SUCCESS.name())
        || dataTrimUpperCase.equals(RecordType.RETRY_FAIL.name())) {
      log.info(
          "Retry data consumed topic = {}, partition = {}, offset = {}, data = {}",
          topic,
          partition,
          offset,
          data);
      return Mono.error(
          new RetriableException(RecordType.valueOf(dataTrimUpperCase), partition, (int) offset));
    } else {
      log.info(
          "Unknown message retrieve {} from topic = {}, partition = {}, offset = {} , Message should be in {} ",
          data,
          topic,
          partition,
          offset,
          Arrays.asList(RecordType.values()));
      return Mono.error(new IllegalArgumentException("Unknown record"));
    }
  }
}
