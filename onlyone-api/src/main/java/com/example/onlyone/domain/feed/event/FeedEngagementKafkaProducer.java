package com.example.onlyone.domain.feed.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 피드 engagement 이벤트를 Kafka로 발행.
 * 좋아요/댓글/피드생성 시 호출되며, fire-and-forget으로 동작.
 * Kafka 비활성 시 빈 자체가 로드되지 않음 — FeedEngagementEventPublisher를 통해 분기.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true")
public class FeedEngagementKafkaProducer {

    public static final String TOPIC = "feed.engagement.v1";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public FeedEngagementKafkaProducer(
            @Qualifier("feedEngagementKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publish(FeedEngagementEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            String key = String.valueOf(event.feedId());
            kafkaTemplate.send(TOPIC, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Kafka engagement 전송 실패: feedId={}, type={}", event.feedId(), event.type(), ex);
                        }
                    });
            log.debug("Kafka engagement 발행: feedId={}, type={}", event.feedId(), event.type());
        } catch (JsonProcessingException e) {
            log.warn("Kafka engagement 직렬화 실패: feedId={}", event.feedId(), e);
        }
    }
}
