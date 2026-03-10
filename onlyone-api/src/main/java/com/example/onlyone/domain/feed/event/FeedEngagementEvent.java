package com.example.onlyone.domain.feed.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 피드 engagement 이벤트 — Kafka "feed.engagement.v1" 토픽으로 발행.
 * Consumer가 윈도우 버퍼링 후 배치로 popularity_score를 갱신한다.
 */
public record FeedEngagementEvent(
        @JsonProperty("feedId") long feedId,
        @JsonProperty("type") Type type,
        @JsonProperty("delta") int delta,
        @JsonProperty("timestamp") long timestamp
) {
    public enum Type {
        LIKE,       // 좋아요 토글 (+1 / -1)
        COMMENT,    // 댓글 생성/삭제 (+1 / -1)
        FEED_CREATE // 피드 생성 (초기 스코어 계산)
    }

    public static FeedEngagementEvent like(long feedId, int delta) {
        return new FeedEngagementEvent(feedId, Type.LIKE, delta, System.currentTimeMillis());
    }

    public static FeedEngagementEvent comment(long feedId, int delta) {
        return new FeedEngagementEvent(feedId, Type.COMMENT, delta, System.currentTimeMillis());
    }

    public static FeedEngagementEvent feedCreate(long feedId) {
        return new FeedEngagementEvent(feedId, Type.FEED_CREATE, 1, System.currentTimeMillis());
    }
}
