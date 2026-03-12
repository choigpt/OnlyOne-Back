package com.example.onlyone.domain.notification.service;

import com.example.onlyone.domain.notification.dto.response.NotificationItemDto;
import com.example.onlyone.domain.notification.entity.NotificationType;
import com.example.onlyone.domain.notification.event.NotificationCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 오프라인 사용자의 미전달 알림을 Redis LIST로 캐싱.
 * SSE 재연결 시 DB 대신 Redis에서 빠르게 복구한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationUndeliveredCache {

    private static final String KEY_PREFIX = "notification:pending:";
    private static final Duration CACHE_TTL = Duration.ofMinutes(30);
    private static final int MAX_CACHED_PER_USER = 50;
    /** ASCII Unit Separator (0x1F) — 알림 필드 간 구분자 */
    private static final String FIELD_SEP = "\u001F";
    private static final int SERIALIZED_FIELD_COUNT = 5;

    private final StringRedisTemplate redis;

    public void add(NotificationCreatedEvent event) {
        String key = key(event.userId());
        try {
            redis.opsForList().rightPush(key, serialize(event));
            redis.opsForList().trim(key, -MAX_CACHED_PER_USER, -1);
            redis.expire(key, CACHE_TTL);
        } catch (Exception e) {
            log.warn("미전달 캐시 추가 실패: userId={}", event.userId(), e);
        }
    }

    public List<NotificationItemDto> popAll(Long userId) {
        String key = key(userId);
        try {
            return fetchAndDelete(key);
        } catch (Exception e) {
            log.warn("미전달 캐시 조회 실패, DB fallback: userId={}", userId, e);
            return Collections.emptyList();
        }
    }

    public boolean hasEntries(Long userId) {
        try {
            Long size = redis.opsForList().size(key(userId));
            return size != null && size > 0;
        } catch (Exception e) {
            return false;
        }
    }

    // ── private helpers ──

    private List<NotificationItemDto> fetchAndDelete(String key) {
        List<String> raw = redis.opsForList().range(key, 0, -1);
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }
        redis.delete(key);
        return raw.stream()
                .map(this::deserialize)
                .filter(Objects::nonNull)
                .toList();
    }

    private String key(Long userId) {
        return KEY_PREFIX + userId;
    }

    private String serialize(NotificationCreatedEvent e) {
        return e.notificationId() + FIELD_SEP
                + e.content() + FIELD_SEP
                + e.type().name() + FIELD_SEP
                + e.isRead() + FIELD_SEP
                + e.createdAt().toString();
    }

    private NotificationItemDto deserialize(String s) {
        try {
            String[] parts = s.split(FIELD_SEP, SERIALIZED_FIELD_COUNT);
            return new NotificationItemDto(
                    Long.parseLong(parts[0]),
                    parts[1],
                    NotificationType.valueOf(parts[2]),
                    Boolean.parseBoolean(parts[3]),
                    LocalDateTime.parse(parts[4])
            );
        } catch (Exception e) {
            log.warn("미전달 캐시 역직렬화 실패: {}", s, e);
            return null;
        }
    }
}
