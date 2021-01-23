package example.spring.kafka.recovery.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

import lombok.Getter;
import lombok.Setter;

@Configuration
@EnableKafka
@Setter
@Getter
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // consumer properties
    @Value("${spring.kafka.consumer.key-deserializer}")
    private String consumerKeyDeserializer;
    @Value("${spring.kafka.consumer.value-deserializer}")
    private String consumerValueDeserializer;

    // producer properties
    @Value("${spring.kafka.producer.key-serializer}")
    private String producerKeyDeserializer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String producerValueDeserializer;

}
