package com.example.onlyone.domain.chat.stream;

import com.example.onlyone.domain.chat.dto.ChatMessageItemDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Redis Sorted Set 기반 채팅 메시지 읽기 캐시.
 *
 * Key: chat:room:{roomId}:recent
 * Score: sentAt epoch millis (정렬용)
 * Value: JSON serialized ChatMessageItemDto
 *
 * 채팅방당 최근 50건만 유지 → 첫 페이지 로드 시 DB 조회 제거.
 */
@Slf4j
@Component
public class ChatMessageCache {

    private static final String KEY_PREFIX = "chat:room:";
    private static final String KEY_SUFFIX = ":recent";
    private static final int MAX_CACHED = 50;
    private static final Duration TTL = Duration.ofHours(2);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public ChatMessageCache(StringRedisTemplate redis,
                             @Qualifier("redisObjectMapper") ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    /**
     * 새 메시지를 캐시에 추가 (ZADD + ZREMRANGEBYRANK로 trim).
     */
    public void addMessage(Long roomId, Long senderId, String nickname,
                            String profileImage, String text, LocalDateTime sentAt) {
        String key = buildKey(roomId);
        double score = toScore(sentAt);

        // messageId가 없으므로 0L 사용 (캐시용 임시 DTO)
        ChatMessageItemDto dto = new ChatMessageItemDto(
                0L, roomId, senderId, nickname, profileImage, text, sentAt, false);

        try {
            String json = objectMapper.writeValueAsString(dto);
            redis.opsForZSet().add(key, json, score);
            // 최대 MAX_CACHED개만 유지 (오래된 것 제거)
            redis.opsForZSet().removeRange(key, 0, -(MAX_CACHED + 1));
            redis.expire(key, TTL);
        } catch (JsonProcessingException e) {
            log.warn("[chat-cache] serialize failed for room={}", roomId, e);
        }
    }

    /**
     * 캐시에서 최근 메시지 조회 (역순 = 최신부터).
     * 캐시 히트 시 DB 조회 불필요.
     *
     * @return 캐시된 메시지 리스트, 캐시 미스 시 empty list
     */
    public List<ChatMessageItemDto> getLatest(Long roomId, int limit) {
        String key = buildKey(roomId);
        Set<ZSetOperations.TypedTuple<String>> tuples =
                redis.opsForZSet().reverseRangeWithScores(key, 0, limit - 1);

        if (tuples == null || tuples.isEmpty()) {
            return List.of();
        }

        List<ChatMessageItemDto> result = new ArrayList<>(tuples.size());
        for (var tuple : tuples) {
            try {
                result.add(objectMapper.readValue(tuple.getValue(), ChatMessageItemDto.class));
            } catch (JsonProcessingException e) {
                log.warn("[chat-cache] deserialize failed for room={}", roomId, e);
            }
        }
        return result;
    }

    /**
     * 캐시에 충분한 데이터가 있는지 확인.
     */
    public boolean hasEnoughCached(Long roomId, int needed) {
        String key = buildKey(roomId);
        Long size = redis.opsForZSet().zCard(key);
        return size != null && size >= needed;
    }

    /**
     * 채팅방 캐시 무효화 (삭제 시 사용).
     */
    public void evict(Long roomId) {
        redis.delete(buildKey(roomId));
    }

    private String buildKey(Long roomId) {
        return KEY_PREFIX + roomId + KEY_SUFFIX;
    }

    private double toScore(LocalDateTime sentAt) {
        return sentAt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
