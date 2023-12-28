package example.spring.kafka.recovery.config;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

@Configuration
@Slf4j
@EnableConfigurationProperties({TopicConfig.class})
public class KafkaProducerConsumerConfiguration {

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate(KafkaProperties kafkaProperties) {
    var factory =
        new DefaultKafkaProducerFactory<String, String>(
            kafkaProperties.buildProducerProperties(null));
    return new KafkaTemplate<>(factory);
  }

  @Bean("bootStrapKafkaReceiver")
  KafkaReceiver<String, String> bootStrapKafkaReceiver(
      TopicConfig topicConfig, KafkaProperties kafkaProperties) {
    var bootstrapTopicInfo = topicConfig.getInboundTopicInfo(TopicConfig.Type.BOOSTRAP);
    var properties = kafkaProperties.buildConsumerProperties(null);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, bootstrapTopicInfo.groupId());
    ReceiverOptions<String, String> options = ReceiverOptions.create(properties);
    var receiverOptions =
        options
            .pollTimeout(Duration.ofMillis(bootstrapTopicInfo.pollTimeout()))
            .addAssignListener(this::handleAssignment)
            .addRevokeListener(this::handleAssignment)
            .subscription(Collections.singletonList(bootstrapTopicInfo.topic()));

    return KafkaReceiver.create(receiverOptions);
  }

  @Bean
  public DeadLetterPublishingRecoverer dltRecoverer(KafkaOperations<String, String> operations) {

    var dltRecoverer =
        new DeadLetterPublishingRecoverer(
            operations, (cr, e) -> new TopicPartition(cr.topic() + "-dlt", 0));

    return dltRecoverer;
  }

  private void handleAssignment(Collection<ReceiverPartition> receiverPartitions) {
    receiverPartitions.forEach(
        receiverPartition ->
            log.info(
                "Partition assigned/revoked for topic=partition {}={} with begin/end offset {}/{}",
                receiverPartition.topicPartition().topic(),
                receiverPartition.topicPartition().partition(),
                receiverPartition.beginningOffset(),
                receiverPartition.endOffset()));
  }
}
