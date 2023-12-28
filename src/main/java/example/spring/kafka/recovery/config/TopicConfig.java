package example.spring.kafka.recovery.config;

import java.util.Map;
import java.util.Optional;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.kafka")
public record TopicConfig(Map<String, Info> inbound) {

  public record Info(String topic, String groupId, long pollTimeout) {}

  public Info getInboundTopicInfo(Type type) {
    return Optional.ofNullable(inbound.get(type.type))
        .orElseThrow(
            () -> new IllegalArgumentException("No record found for kafka topic type " + type));
  }

  public enum Type {
    BOOSTRAP("bootstrap"),
    RETRY("retry");
    private String type;

    Type(String type) {
      this.type = type;
    }
  }
}
