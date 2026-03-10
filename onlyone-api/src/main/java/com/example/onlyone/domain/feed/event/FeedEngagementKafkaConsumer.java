package com.example.onlyone.domain.feed.event;

import com.example.onlyone.domain.feed.repository.FeedRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka "feed.engagement.v1" 토픽 Consumer.
 *
 * 동작 흐름:
 * 1. 이벤트 수신 → in-memory 버퍼에 피드별 델타 축적 (like +1, comment +2 가중치)
 * 2. 5초 주기로 flush → 배치 popularity_score UPDATE + Redis 실시간 랭킹 갱신
 *
 * 기존 FeedPopularityScheduler(5분 배치)와 공존:
 * - Kafka ON: Consumer가 준실시간 갱신, Scheduler는 보정 역할
 * - Kafka OFF: Scheduler가 기존대로 5분 배치 갱신
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true")
public class FeedEngagementKafkaConsumer {

    private final FeedRepository feedRepository;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    /** feedId → 가중치 합산 델타 (like=1, comment=2) */
    private final ConcurrentHashMap<Long, AtomicInteger> buffer = new ConcurrentHashMap<>();

    private static final String POPULAR_FEED_ZSET = "popular:realtime";

    @KafkaListener(
            groupId = "feed-engagement",
            topics = FeedEngagementKafkaProducer.TOPIC,
            containerFactory = "feedEngagementKafkaListenerContainerFactory",
            concurrency = "3"
    )
    public void onEngagementEvent(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                FeedEngagementEvent event = objectMapper.readValue(record.value(), FeedEngagementEvent.class);
                int weight = switch (event.type()) {
                    case LIKE -> event.delta();                // +1 or -1
                    case COMMENT -> event.delta() * 2;         // 댓글 2배 가중치
                    case FEED_CREATE -> 0;                     // 생성은 초기 스코어만
                };
                if (weight != 0) {
                    buffer.computeIfAbsent(event.feedId(), k -> new AtomicInteger(0))
                            .addAndGet(weight);
                }
            } catch (Exception e) {
                log.warn("engagement 이벤트 파싱 실패: offset={}", record.offset(), e);
            }
        }
        ack.acknowledge();
    }

    /**
     * 5초 주기 flush: 버퍼에 쌓인 델타로 popularity_score 갱신.
     */
    @Scheduled(fixedDelay = 5000)
    public void flushEngagementBuffer() {
        if (buffer.isEmpty()) return;

        // 스냅샷 후 클리어
        ConcurrentHashMap<Long, AtomicInteger> snapshot = new ConcurrentHashMap<>(buffer);
        buffer.keySet().removeAll(snapshot.keySet());

        int totalFeeds = snapshot.size();
        int totalDelta = 0;

        for (Map.Entry<Long, AtomicInteger> entry : snapshot.entrySet()) {
            long feedId = entry.getKey();
            int delta = entry.getValue().get();
            totalDelta += Math.abs(delta);

            // DB popularity_score 즉시 갱신 (단건 — 배치보다 정확)
            feedRepository.updatePopularityScoreById(feedId);

            // Redis 실시간 랭킹 ZSET 갱신
            try {
                redis.opsForZSet().incrementScore(POPULAR_FEED_ZSET, String.valueOf(feedId), delta);
            } catch (Exception e) {
                log.debug("Redis 랭킹 갱신 실패: feedId={}", feedId);
            }
        }

        log.debug("engagement flush: feeds={}, totalDelta={}", totalFeeds, totalDelta);
    }
}
