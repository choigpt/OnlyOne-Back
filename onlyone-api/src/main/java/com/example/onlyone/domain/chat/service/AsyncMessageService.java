package com.example.onlyone.domain.chat.service;

import com.example.onlyone.domain.chat.entity.ChatRoom;
import com.example.onlyone.domain.chat.entity.Message;
import com.example.onlyone.domain.chat.repository.ChatRoomRepository;
import com.example.onlyone.domain.chat.repository.MessageRepository;
import com.example.onlyone.domain.user.entity.User;
import com.example.onlyone.domain.user.repository.UserRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 채팅 메시지 비동기 배치 저장.
 * 200ms 윈도우 또는 50건 도달 시 batch INSERT 수행.
 * DB 커넥션 사용 횟수를 1/N로 줄여 HikariCP 경합 감소.
 */
@Service
@Slf4j
public class AsyncMessageService {

    private static final int BATCH_SIZE = 50;
    private static final long FLUSH_INTERVAL_MS = 200;
    private static final String FAILED_MESSAGES_KEY = "chat:failed-messages";
    private static final Duration FAILED_MESSAGES_TTL = Duration.ofDays(7);

    private final MessageRepository messageRepository;
    private final ChatRoomRepository chatRoomRepository;
    private final UserRepository userRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private final BlockingQueue<PendingMessage> buffer = new LinkedBlockingQueue<>(10_000);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "chat-batch-flush"); t.setDaemon(true); return t; });

    public AsyncMessageService(MessageRepository messageRepository,
                                ChatRoomRepository chatRoomRepository,
                                UserRepository userRepository,
                                StringRedisTemplate redisTemplate,
                                ObjectMapper objectMapper) {
        this.messageRepository = messageRepository;
        this.chatRoomRepository = chatRoomRepository;
        this.userRepository = userRepository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;

        scheduler.scheduleWithFixedDelay(this::flushBuffer, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void saveMessageAsync(Long chatRoomId, Long userId, String text) {
        PendingMessage msg = new PendingMessage(chatRoomId, userId, text, LocalDateTime.now());
        if (!buffer.offer(msg)) {
            log.warn("채팅 배치 버퍼 가득 참, 즉시 폴백 저장: chatRoomId={}, userId={}", chatRoomId, userId);
            storeToRedis(msg);
        }
    }

    private void flushBuffer() {
        List<PendingMessage> batch = new ArrayList<>(BATCH_SIZE);
        buffer.drainTo(batch, BATCH_SIZE);
        if (batch.isEmpty()) return;

        try {
            saveBatch(batch);
        } catch (Exception e) {
            log.error("배치 저장 실패 ({}건), Redis 폴백", batch.size(), e);
            batch.forEach(this::storeToRedis);
        }
    }

    @Transactional
    public void saveBatch(List<PendingMessage> batch) {
        List<Message> entities = new ArrayList<>(batch.size());
        for (PendingMessage msg : batch) {
            ChatRoom room = chatRoomRepository.getReferenceById(msg.chatRoomId());
            User user = userRepository.getReferenceById(msg.userId());
            entities.add(Message.builder()
                    .chatRoom(room)
                    .user(user)
                    .text(msg.text())
                    .sentAt(msg.sentAt())
                    .deleted(false)
                    .build());
        }
        messageRepository.saveAll(entities);
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdown();
        // 남은 버퍼 플러시
        flushBuffer();
        while (!buffer.isEmpty()) flushBuffer();
    }

    private void storeToRedis(PendingMessage msg) {
        try {
            String entry = objectMapper.writeValueAsString(Map.of(
                    "chatRoomId", msg.chatRoomId(),
                    "userId", msg.userId(),
                    "text", msg.text(),
                    "failedAt", LocalDateTime.now().toString()));
            redisTemplate.opsForList().rightPush(FAILED_MESSAGES_KEY, entry);
            redisTemplate.expire(FAILED_MESSAGES_KEY, FAILED_MESSAGES_TTL);
        } catch (JsonProcessingException e) {
            log.error("Redis 폴백 직렬화 실패: chatRoomId={}", msg.chatRoomId(), e);
        }
    }

    record PendingMessage(Long chatRoomId, Long userId, String text, LocalDateTime sentAt) {}
}
