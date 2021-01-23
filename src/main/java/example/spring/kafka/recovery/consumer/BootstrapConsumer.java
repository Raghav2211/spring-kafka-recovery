package example.spring.kafka.recovery.consumer;

import java.util.Arrays;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import example.spring.kafka.recovery.consumer.exception.PoisonPillException;
import example.spring.kafka.recovery.consumer.exception.RetryException;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class BootstrapConsumer {

    public static enum RecordType {
        SUCCESS, POISON_PILL, RETRY
    }

    @KafkaListener(topics = "${app.kafka.inbound.springrecovery.bootstrap.topic}", groupId = "${app.kafka.inbound.springrecovery.bootstrap.groupId}", containerFactory = "kafkaBootstrapListenerContainerFactory")
    public void onReceiving(String data, @Header(KafkaHeaders.OFFSET) Integer offset,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, Acknowledgment ack) {
        processMessages(data, offset, partition, topic);
        ack.acknowledge();
    }

    private void processMessages(String data, Integer offset, int partition, String topic) {
        if (data.toUpperCase().trim().equals(RecordType.SUCCESS.name())) {
            log.info("Processing topic = {}, partition = {}, offset = {}, success data = {}", topic, partition, offset,
                    data);
            log.info("Data successfully consumed");
        } else if (data.toUpperCase().trim().equals(RecordType.POISON_PILL.name())) {
            log.info("Processing topic = {}, partition = {}, offset = {}, poison pill data = {}", topic, partition,
                    offset, data);
            throw new PoisonPillException(data, partition, offset);
        } else if (data.toUpperCase().trim().equals(RecordType.RETRY.name())) {
            log.info("Processing topic = {}, partition = {}, offset = {}, retry data = {}", topic, partition, offset,
                    data);
            throw new RetryException(data, partition, offset);
        } else {
            log.info("Unknown message retrieve , Message should be in {} ", Arrays.asList(RecordType.values()));
        }
    }
}
