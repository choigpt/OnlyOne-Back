package com.example.onlyone.domain.feed.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * popularity_score 배치 갱신 스케줄러.
 * Kafka OFF 시에만 활성화 — Kafka ON이면 {@link com.example.onlyone.domain.feed.event.FeedEngagementKafkaConsumer}가
 * 실시간 단건 갱신하므로 이 스케줄러는 불필요.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "false", matchIfMissing = true)
public class FeedPopularityScheduler {

    private final FeedPopularityBatchService batchService;

    private static final int BATCH_SIZE = 5_000;

    @Scheduled(fixedRate = 300_000) // 5분마다
    public void updatePopularityScores() {
        long cursor = 0;
        int totalUpdated = 0;
        int updated;
        do {
            long nextCursor = batchService.updateBatchAfter(cursor, BATCH_SIZE);
            updated = (nextCursor > cursor) ? BATCH_SIZE : 0;
            totalUpdated += updated;
            cursor = nextCursor;
        } while (updated >= BATCH_SIZE);
        if (totalUpdated > 0) {
            log.info("인기도 스코어 갱신 완료: {}건", totalUpdated);
        }
    }
}
