package com.example.onlyone.domain.feed.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class FeedPopularityBatchService {

    private final JdbcTemplate jdbcTemplate;

    /**
     * PK 커서 기반 배치 갱신 — feed_id > cursor 조건으로 인덱스 스캔.
     * LIMIT offset 방식의 full scan 문제 해결.
     *
     * @return 이 배치에서 처리한 마지막 feed_id (다음 cursor). 처리 건수 0이면 cursor 그대로 반환.
     */
    @Transactional
    public long updateBatchAfter(long cursor, int batchSize) {
        // 1) 대상 PK 범위 조회 (인덱스 스캔, 매우 빠름)
        Long maxId = jdbcTemplate.queryForObject(
                "SELECT MAX(feed_id) FROM (" +
                "  SELECT feed_id FROM feed " +
                "  WHERE feed_id > ? AND deleted = false AND created_at >= NOW() - INTERVAL 7 DAY " +
                "  ORDER BY feed_id LIMIT ?" +
                ") sub",
                Long.class, cursor, batchSize);

        if (maxId == null) return cursor;

        // 2) 범위 UPDATE (PK range scan, row lock 최소화)
        jdbcTemplate.update(
                "UPDATE feed SET popularity_score = " +
                "LN(GREATEST(like_count + comment_count * 2, 1)) - (TIMESTAMPDIFF(SECOND, created_at, NOW()) / 43200.0) " +
                "WHERE feed_id > ? AND feed_id <= ? AND deleted = false AND created_at >= NOW() - INTERVAL 7 DAY",
                cursor, maxId);

        return maxId;
    }
}
