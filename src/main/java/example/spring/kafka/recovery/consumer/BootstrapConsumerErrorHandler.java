package example.spring.kafka.recovery.consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BootstrapConsumerErrorHandler implements ContainerAwareErrorHandler {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.outbound.springrecovery.dlq.topic}")
    String dlqTopic;

    @Override
    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
            MessageListenerContainer container) {
        ConsumerRecord<?, ?> record = records.get(0);
        if (thrownException.getCause() instanceof PoisonPillException) {
            try {
                kafkaTemplate.send(dlqTopic, BootstrapConsumer.RecordType.POISON_PILL.name());
                log.info("Consumer info  {} ", consumer.assignment());
                consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
            } catch (Exception e) {
                consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                throw new KafkaException("Seek to current after exception", thrownException);
            }
        } else {
            log.error("Unknown error comes , Message -> {} , Exception -> {} , Exception Cause -> {} ",
                    thrownException.getMessage(), thrownException.getClass().getName(),
                    thrownException.getCause().getClass().getName());
        }

    }

}
