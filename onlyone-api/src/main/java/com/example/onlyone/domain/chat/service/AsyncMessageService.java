package com.example.onlyone.domain.chat.service;

import com.example.onlyone.domain.chat.stream.ChatMessageCache;
import com.example.onlyone.domain.chat.stream.ChatMessageStreamConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * 채팅 메시지 비동기 저장 — Redis Streams 기반.
 *
 * DB 커넥션을 사용하지 않고 Redis XADD만 수행 (< 1ms).
 * 실제 DB 저장은 ChatMessageStreamConsumer가 배치로 처리.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AsyncMessageService {

    private final StringRedisTemplate redisTemplate;
    private final ChatMessageCache chatMessageCache;

    public void saveMessageAsync(Long chatRoomId, Long userId, String text) {
        saveMessageAsync(chatRoomId, userId, null, null, text);
    }

    public void saveMessageAsync(Long chatRoomId, Long userId,
                                  String nickname, String profileImage, String text) {
        LocalDateTime now = LocalDateTime.now();

        // 1. Redis Streams XADD (DB 저장 버퍼)
        try {
            RecordId id = redisTemplate.opsForStream().add(
                    ChatMessageStreamConsumer.STREAM,
                    Map.of(
                            "roomId", String.valueOf(chatRoomId),
                            "userId", String.valueOf(userId),
                            "text", text,
                            "sentAt", now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    ));
            log.debug("[chat-async] XADD success: stream={}, id={}", ChatMessageStreamConsumer.STREAM, id);
        } catch (Exception e) {
            log.error("[chat-async] XADD failed, message may be lost: roomId={}, userId={}", chatRoomId, userId, e);
        }

        // 2. 읽기 캐시에 즉시 반영 (nickname이 있을 때만)
        if (nickname != null) {
            try {
                chatMessageCache.addMessage(chatRoomId, userId, nickname, profileImage, text, now);
            } catch (Exception e) {
                log.warn("[chat-async] cache addMessage failed: roomId={}", chatRoomId, e);
            }
        }
    }
}
