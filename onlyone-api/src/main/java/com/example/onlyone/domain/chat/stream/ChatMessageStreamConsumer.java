package com.example.onlyone.domain.chat.stream;

import com.example.onlyone.domain.chat.entity.ChatRoom;
import com.example.onlyone.domain.chat.entity.Message;
import com.example.onlyone.domain.chat.repository.ChatRoomRepository;
import com.example.onlyone.domain.chat.repository.MessageRepository;
import com.example.onlyone.domain.user.entity.User;
import com.example.onlyone.domain.user.repository.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Redis Streams 기반 채팅 메시지 배치 저장 컨슈머.
 * chat:msg:stream → XREADGROUP → batch saveAll → XACK.
 *
 * DB 커넥션을 배치 단위(최대 100건)로 1회만 사용하여
 * 기존 메시지당 1커넥션 대비 95%+ 절감.
 */
@Slf4j
@Component
public class ChatMessageStreamConsumer implements SmartLifecycle {

    public static final String STREAM = "chat:msg:stream";
    public static final String GROUP = "chat-persist-v1";
    private static final String CONSUMER_NAME = "c-" + UUID.randomUUID().toString().substring(0, 8);

    private static final Duration BLOCK_TIMEOUT = Duration.ofMillis(500);
    private static final int BATCH_COUNT = 100;
    private static final long MAX_BACKOFF_MS = 5000;

    private final StringRedisTemplate redis;
    private final MessageRepository messageRepository;
    private final ChatRoomRepository chatRoomRepository;
    private final UserRepository userRepository;

    private volatile boolean running = false;
    private Thread worker;

    public ChatMessageStreamConsumer(StringRedisTemplate redis,
                                      MessageRepository messageRepository,
                                      ChatRoomRepository chatRoomRepository,
                                      UserRepository userRepository) {
        this.redis = redis;
        this.messageRepository = messageRepository;
        this.chatRoomRepository = chatRoomRepository;
        this.userRepository = userRepository;
    }

    @Override
    public void start() {
        if (running) return;
        running = true;
        ensureStreamGroup();

        worker = new Thread(this::consumeLoop, "chat-stream-consumer");
        worker.setDaemon(true);
        worker.start();
        log.info("[chat-stream] consumer started: group={}, consumer={}", GROUP, CONSUMER_NAME);
    }

    private void consumeLoop() {
        final Consumer consumer = Consumer.from(GROUP, CONSUMER_NAME);
        final StreamReadOptions opts = StreamReadOptions.empty().count(BATCH_COUNT).block(BLOCK_TIMEOUT);
        long backoffMs = 50;

        while (running) {
            try {
                List<MapRecord<String, Object, Object>> records =
                        redis.opsForStream().read(consumer, opts, StreamOffset.create(STREAM, ReadOffset.lastConsumed()));

                backoffMs = 50;

                if (records == null || records.isEmpty()) continue;

                List<RecordId> ackIds = processBatch(records);
                ackRecords(ackIds);

                // 스트림 트림 (최근 10000건만 유지)
                redis.opsForStream().trim(STREAM, 10000);

            } catch (DataAccessException dae) {
                handleDataAccessError(dae);
                sleepQuiet(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            } catch (Exception ex) {
                log.warn("[chat-stream] unexpected error; will retry. err={}", ex.toString());
                sleepQuiet(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
    }

    @Transactional
    public List<RecordId> processBatch(List<MapRecord<String, Object, Object>> records) {
        List<Message> entities = new ArrayList<>(records.size());
        List<RecordId> ackIds = new ArrayList<>(records.size());

        for (var r : records) {
            var v = r.getValue();
            Object roomIdRaw = v.get("roomId");
            Object userIdRaw = v.get("userId");
            Object textRaw = v.get("text");
            Object sentAtRaw = v.get("sentAt");

            if (roomIdRaw == null || userIdRaw == null || textRaw == null) {
                log.warn("[chat-stream] invalid record id={}, skipping", r.getId());
                ackIds.add(r.getId());
                continue;
            }

            Long roomId = Long.parseLong(roomIdRaw.toString());
            Long userId = Long.parseLong(userIdRaw.toString());
            String text = textRaw.toString();
            LocalDateTime sentAt = sentAtRaw != null
                    ? LocalDateTime.parse(sentAtRaw.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    : LocalDateTime.now();

            ChatRoom room = chatRoomRepository.getReferenceById(roomId);
            User user = userRepository.getReferenceById(userId);

            entities.add(Message.builder()
                    .chatRoom(room)
                    .user(user)
                    .text(text)
                    .sentAt(sentAt)
                    .deleted(false)
                    .build());

            ackIds.add(r.getId());
        }

        if (!entities.isEmpty()) {
            messageRepository.saveAll(entities);
            log.debug("[chat-stream] batch saved {} messages", entities.size());
        }

        return ackIds;
    }

    private void ackRecords(List<RecordId> ids) {
        if (ids == null || ids.isEmpty()) return;
        try {
            redis.opsForStream().acknowledge(STREAM, GROUP, ids.toArray(RecordId[]::new));
        } catch (Exception e) {
            log.warn("[chat-stream] ack failed: {}", e.toString());
        }
    }

    private void ensureStreamGroup() {
        try {
            try { redis.opsForStream().add(STREAM, Map.of("init", "1")); } catch (Exception ignore) {}
            redis.opsForStream().createGroup(STREAM, ReadOffset.from("0-0"), GROUP);
            log.info("[chat-stream] group ready: stream={}, group={}", STREAM, GROUP);
        } catch (Exception e) {
            log.info("[chat-stream] group may already exist: {}", e.toString());
        }
    }

    private void handleDataAccessError(DataAccessException dae) {
        String msg = String.valueOf(dae.getMessage());
        if (msg.contains("NOGROUP") || msg.contains("no such key")) {
            ensureStreamGroup();
        } else {
            log.warn("[chat-stream] DB error; will NOT ack. err={}", dae.toString());
        }
    }

    private void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    @Override public void stop() {
        running = false;
        if (worker != null) worker.interrupt();
        log.info("[chat-stream] consumer stopped");
    }

    @Override public boolean isRunning() { return running; }
    @Override public boolean isAutoStartup() { return true; }
    @Override public int getPhase() { return Integer.MIN_VALUE; }
}
