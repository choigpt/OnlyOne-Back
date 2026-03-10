package com.example.onlyone.domain.feed.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Kafka 활성/비활성에 따라 분기하는 facade.
 * Kafka 비활성 시 이벤트를 버림 — 기존 FeedPopularityScheduler가 fallback으로 동작.
 */
@Slf4j
@Component
public class FeedEngagementEventPublisher {

    private final FeedEngagementKafkaProducer kafkaProducer;

    public FeedEngagementEventPublisher(@Nullable FeedEngagementKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        if (kafkaProducer == null) {
            log.info("Kafka 비활성 — engagement 이벤트는 FeedPopularityScheduler fallback 사용");
        }
    }

    public void publish(FeedEngagementEvent event) {
        if (kafkaProducer != null) {
            kafkaProducer.publish(event);
        }
    }
}
