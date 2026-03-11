package com.example.onlyone.domain.chat.stream;

import com.example.onlyone.domain.chat.dto.ChatRoomResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Redis 기반 채팅방 목록 캐시.
 *
 * Key: chat:roomlist:{userId}:{clubId}
 * Value: JSON serialized List<ChatRoomResponse>
 * TTL: 30초
 *
 * @Cacheable 대신 StringRedisTemplate + 수동 JSON으로
 * GenericJackson2JsonRedisSerializer DefaultTyping 문제 회피.
 */
@Slf4j
@Component
public class ChatRoomListCache {

    private static final String PREFIX = "chat:roomlist:";
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final TypeReference<List<ChatRoomResponse>> LIST_TYPE = new TypeReference<>() {};

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public ChatRoomListCache(StringRedisTemplate redis,
                              @Qualifier("redisObjectMapper") ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public Optional<List<ChatRoomResponse>> get(Long userId, Long clubId) {
        try {
            String json = redis.opsForValue().get(buildKey(userId, clubId));
            if (json != null) {
                return Optional.of(objectMapper.readValue(json, LIST_TYPE));
            }
        } catch (JsonProcessingException e) {
            log.warn("[room-cache] deserialize failed: userId={}, clubId={}", userId, clubId);
        } catch (Exception e) {
            log.warn("[room-cache] Redis read failed: userId={}, clubId={}", userId, clubId);
        }
        return Optional.empty();
    }

    public void put(Long userId, Long clubId, List<ChatRoomResponse> rooms) {
        try {
            String json = objectMapper.writeValueAsString(rooms);
            redis.opsForValue().set(buildKey(userId, clubId), json, TTL);
        } catch (JsonProcessingException e) {
            log.warn("[room-cache] serialize failed: userId={}, clubId={}", userId, clubId);
        } catch (Exception e) {
            log.warn("[room-cache] Redis write failed: userId={}, clubId={}", userId, clubId);
        }
    }

    private String buildKey(Long userId, Long clubId) {
        return PREFIX + userId + ":" + clubId;
    }
}
