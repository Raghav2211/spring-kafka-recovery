package example.spring.kafka.recovery.consumer;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import example.spring.kafka.recovery.consumer.exception.AllRetryExhaustException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class RetryConsumerErrorhandler {

    private final KafkaOperations<String, String> kafkaOperation;
    private final String dlt;

    public RetryConsumerErrorhandler(KafkaOperations<String, String> kafkaOperation,
            @Value("${app.kafka.outbound.springrecovery.dlt.topic}") String dlt) {
        this.kafkaOperation = kafkaOperation;
        this.dlt = dlt;
    }

    @Bean
    public ErrorHandler retryErrorhandler() {
        return new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(kafkaOperation, (record, exception) -> {
            if (exception.getCause() instanceof AllRetryExhaustException) {
                log.info("All retry exhaust {}, Message will be send on topic {}", exception.getMessage(), dlt);
                return new TopicPartition(dlt, record.partition());
            } else {
                return null;
            }

        }), new FixedBackOff(5000, 3));
    }

}
