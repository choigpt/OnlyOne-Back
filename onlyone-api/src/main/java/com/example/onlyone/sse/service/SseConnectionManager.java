package com.example.onlyone.sse.service;

import com.example.onlyone.global.exception.CustomException;
import com.example.onlyone.global.exception.GlobalErrorCode;
import com.example.onlyone.sse.exception.SseErrorCode;
import com.example.onlyone.sse.SseConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class SseConnectionManager {

    private final long sseTimeoutMillis;
    private final int maxConnections;
    private static final long CLEANUP_GRACE_PERIOD_MS = 60_000;

    private final ConcurrentHashMap<Long, SseConnection> activeConnections = new ConcurrentHashMap<>();

    @Autowired(required = false)
    private DistributedConnectionRegistry distributedRegistry;

    public SseConnectionManager(
            @Value("${app.notification.sse-timeout-millis:60000}") long sseTimeoutMillis,
            @Value("${app.notification.max-connections:7000}") int maxConnections) {
        this.sseTimeoutMillis = sseTimeoutMillis;
        this.maxConnections = maxConnections;
    }

    // ── 커넥션 생명주기 ──

    public SseEmitter createConnection(Long userId) {
        validateUserId(userId);
        validateCapacity(userId);

        SseConnection connection = buildConnection(userId);
        registerCallbacks(connection);
        replaceConnection(userId, connection);
        sendInitEvent(userId, connection);
        registerToDistributedRegistry(userId);

        return connection.getEmitter();
    }

    public void cleanupConnection(Long userId) {
        activeConnections.remove(userId);
        unregisterFromDistributedRegistry(userId);
    }

    public void clearAllConnections() {
        activeConnections.entrySet().removeIf(entry -> {
            closeQuietly(entry.getValue());
            unregisterFromDistributedRegistry(entry.getKey());
            return true;
        });
    }

    // ── 조회 ──

    public SseConnection getConnection(Long userId) {
        return activeConnections.get(userId);
    }

    public boolean isUserConnected(Long userId) {
        return activeConnections.containsKey(userId);
    }

    public Set<Long> getActiveUserIds() {
        return Set.copyOf(activeConnections.keySet());
    }

    public int getActiveConnectionCount() {
        return activeConnections.size();
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    // ── 좀비 커넥션 정리 (2분 주기) ──

    @Scheduled(fixedRate = 120_000)
    public void cleanupStaleConnections() {
        if (activeConnections.isEmpty()) return;

        try {
            doCleanupStaleConnections();
        } catch (Exception e) {
            log.warn("SSE 커넥션 정리 중 오류", e);
        }
    }

    // ── createConnection helpers ──

    private void validateUserId(Long userId) {
        if (userId == null) {
            throw new CustomException(GlobalErrorCode.UNAUTHORIZED);
        }
    }

    private void validateCapacity(Long userId) {
        if (activeConnections.size() >= maxConnections && !activeConnections.containsKey(userId)) {
            throw new CustomException(SseErrorCode.SSE_CONNECTION_LIMIT_EXCEEDED);
        }
    }

    private SseConnection buildConnection(Long userId) {
        return SseConnection.builder()
                .userId(userId)
                .emitter(new SseEmitter(sseTimeoutMillis))
                .connectionTime(LocalDateTime.now())
                .timeoutMillis(sseTimeoutMillis)
                .build();
    }

    private void replaceConnection(Long userId, SseConnection newConnection) {
        SseConnection old = activeConnections.put(userId, newConnection);
        if (old != null) {
            closeQuietly(old);
        }
    }

    private void sendInitEvent(Long userId, SseConnection connection) {
        try {
            connection.getEmitter().send(SseEmitter.event()
                    .id("init_" + System.currentTimeMillis())
                    .name("connected")
                    .data("OK"));
        } catch (Exception e) {
            activeConnections.remove(userId, connection);
            throw new CustomException(SseErrorCode.SSE_CONNECTION_FAILED);
        }
    }

    private void registerCallbacks(SseConnection connection) {
        SseEmitter emitter = connection.getEmitter();
        Long userId = connection.getUserId();
        emitter.onCompletion(() -> cleanupConnection(userId));
        emitter.onTimeout(() -> cleanupConnection(userId));
        emitter.onError(ex -> cleanupConnection(userId));
    }

    // ── cleanup helpers ──

    private void doCleanupStaleConnections() {
        LocalDateTime cutoff = computeCutoff();
        int cleaned = removeConnectionsBefore(cutoff);
        refreshDistributedRegistryTtl();
        logCleanupResult(cleaned);
    }

    private LocalDateTime computeCutoff() {
        return LocalDateTime.now()
                .minusSeconds((sseTimeoutMillis + CLEANUP_GRACE_PERIOD_MS) / 1000);
    }

    private int removeConnectionsBefore(LocalDateTime cutoff) {
        int[] cleaned = {0};
        activeConnections.entrySet().removeIf(entry -> {
            if (entry.getValue().getConnectionTime().isBefore(cutoff)) {
                closeQuietly(entry.getValue());
                unregisterFromDistributedRegistry(entry.getKey());
                cleaned[0]++;
                return true;
            }
            return false;
        });
        return cleaned[0];
    }

    private void refreshDistributedRegistryTtl() {
        if (distributedRegistry != null && !activeConnections.isEmpty()) {
            distributedRegistry.refreshTtl();
        }
    }

    private void logCleanupResult(int cleaned) {
        if (cleaned > 0) {
            log.info("좀비 SSE 커넥션 정리: cleaned={}, remaining={}", cleaned, activeConnections.size());
        }
    }

    // ── 공통 helpers ──

    private void closeQuietly(SseConnection connection) {
        try {
            connection.getEmitter().complete();
        } catch (Exception ignored) {
        }
    }

    private void registerToDistributedRegistry(Long userId) {
        if (distributedRegistry != null) {
            distributedRegistry.register(userId);
        }
    }

    private void unregisterFromDistributedRegistry(Long userId) {
        if (distributedRegistry != null) {
            distributedRegistry.unregister(userId);
        }
    }
}
