package example.spring.kafka.recovery.consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import example.spring.kafka.recovery.consumer.exception.PoisonPillException;
import example.spring.kafka.recovery.consumer.exception.RetriableException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BootstrapConsumerErrorHandler implements ContainerAwareErrorHandler {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final String dlt;

    private final String retryTopic;

    public BootstrapConsumerErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.outbound.springrecovery.dlt.topic}") String dlt,
            @Value("${app.kafka.outbound.springrecovery.retry.topic}") String retryTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.dlt = dlt;
        this.retryTopic = retryTopic;
    }

    @Override
    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
            MessageListenerContainer container) {
        ConsumerRecord<?, ?> record = records.get(0);
        if (thrownException.getCause() instanceof PoisonPillException) {
            PoisonPillException poisonPillException = (PoisonPillException) thrownException.getCause();
            log.info("PoisonPillException occur, Message will send on topic {} ", dlt);
            sendErrorDataonTopic(thrownException, consumer, record, dlt, BootstrapConsumer.RecordType.POISON_PILL,
                    poisonPillException.getSourcePartition(), poisonPillException.getSourceOffset());
        } else if (thrownException.getCause() instanceof RetriableException) {
            RetriableException retryException = (RetriableException) thrownException.getCause();
            log.info("RetriableException occur, Message will send on topic {} ", retryTopic);
            sendErrorDataonTopic(thrownException, consumer, record, retryTopic, retryException.getRecordType(),
                    retryException.getSourcePartition(), retryException.getSourceOffset());
        } else {
            log.error("Unknown error comes , Message -> {} , Exception -> {} , Exception Cause -> {} ",
                    thrownException.getMessage(), thrownException.getClass().getName(),
                    thrownException.getCause().getClass().getName());
        }
    }

    private void sendErrorDataonTopic(Exception thrownException, Consumer<?, ?> consumer,
            ConsumerRecord<?, ?> failRecord, String topicToSend, BootstrapConsumer.RecordType recordType,
            int sourcepartition, int sourceOffset) {
        try {
            String dltMessage = "M(" + recordType.name() + ")-P(" + sourcepartition + ")-O(" + sourceOffset + ")";
            kafkaTemplate.send(topicToSend, failRecord.partition(), String.valueOf(failRecord.key()), dltMessage);
            consumer.seek(new TopicPartition(failRecord.topic(), failRecord.partition()), failRecord.offset() + 1);
        } catch (Exception e) {
            consumer.seek(new TopicPartition(failRecord.topic(), failRecord.partition()), failRecord.offset());
            throw new KafkaException("Seek to current after exception", thrownException);
        }
    }

}
