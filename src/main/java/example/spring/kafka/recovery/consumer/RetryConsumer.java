package example.spring.kafka.recovery.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class RetryConsumer {

    public static enum RecordType {
        SUCCESS, POISON_PILL, RETRY
    }

    @KafkaListener(topics = "${app.kafka.inbound.springrecovery.retry.topic}", groupId = "${app.kafka.inbound.springrecovery.retry.groupId}", containerFactory = "kafkaRetryListenerContainerFactory")
    public void onReceiving(String data, @Header(KafkaHeaders.OFFSET) Integer offset,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, Acknowledgment ack) {
//        throw new IllegalArgumentException();
        log.info("Consume in retry topic = {}, partition = {}, offset = {}, success data = {}", topic, partition,
                offset, data);
    }

}
