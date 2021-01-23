package example.spring.kafka.recovery.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import example.spring.kafka.recovery.consumer.BootstrapConsumerErrorHandler;

@Configuration
@ConditionalOnBean(type = "example.spring.kafka.recovery.config.KafkaConfig")
public class KafkaListenerConfig {

    private KafkaConfig kafkaConfig;
    @Value("${app.kafka.outbound.springrecovery.dlq.topic}")
    String dlqTopic;

    public KafkaListenerConfig(KafkaConfig appConfig) {
        this.kafkaConfig = appConfig;
    }

    public Map<String, Object> producerConfigsRecordNaming() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaConfig.getProducerKeyDeserializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaConfig.getProducerValueDeserializer());
        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigsRecordNaming());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            BootstrapConsumerErrorHandler bootstrapConsumerErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(1);
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

//        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkTemplate, (r, e) -> {
//            if (e instanceof IllegalArgumentException) {
//                log.error("Sending to Dlq topic on poisonpill data ----- {} ", e.getMessage());
//                return new TopicPartition(dlqTopic, r.partition());
//            } else {
//                log.error("Unknown exception occurs {}", e.getMessage());
//                return new TopicPartition(unknowntopic, r.partition());
//            }
//        });
//        var errorHandler = new SeekToCurrentErrorHandler(recoverer);
//        factory.setErrorHandler(errorHandler);

        factory.setErrorHandler(bootstrapConsumerErrorHandler);

        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }

    @Bean
    public Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaConfig.getConsumerKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaConfig.getConsumerValueDeserializer());
        return props;
    }

}
