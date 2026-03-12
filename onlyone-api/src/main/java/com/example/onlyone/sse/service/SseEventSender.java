package com.example.onlyone.sse.service;

import com.example.onlyone.global.exception.CustomException;
import com.example.onlyone.sse.exception.SseErrorCode;
import com.example.onlyone.sse.SseConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class SseEventSender {

    private final SseConnectionManager connectionManager;
    private final Executor sseEventExecutor;
    private final AtomicLong eventIdCounter = new AtomicLong(0);

    private static final int MAX_DATA_SIZE = 64 * 1024;
    private static final int MAX_STRING_LENGTH = MAX_DATA_SIZE / 2;
    private static final Set<String> CLIENT_DISCONNECT_MESSAGES = Set.of(
            "Broken pipe",
            "Connection reset by peer",
            "An existing connection was forcibly closed"
    );

    public SseEventSender(
            SseConnectionManager connectionManager,
            @Qualifier("sseEventExecutor") Executor sseEventExecutor) {
        this.connectionManager = connectionManager;
        this.sseEventExecutor = sseEventExecutor;
    }

    // ── public API ──

    public boolean isUserConnected(Long userId) {
        return connectionManager.isUserConnected(userId);
    }

    public CompletableFuture<Boolean> sendEvent(Long userId, String eventName, Object data) {
        SseConnection connection = connectionManager.getConnection(userId);
        if (connection == null) {
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.supplyAsync(
                () -> doSend(connection, userId, eventName, data), sseEventExecutor);
    }

    /**
     * executor를 거치지 않고 현재 스레드에서 직접 전송.
     * Recovery처럼 이미 동기 컨텍스트에서 호출할 때 executor 경합을 피한다.
     */
    public boolean sendEventDirect(Long userId, String eventName, Object data) {
        SseConnection connection = connectionManager.getConnection(userId);
        if (connection == null) return false;
        return doSend(connection, userId, eventName, data);
    }

    // ── 전송 핵심 ──

    private boolean doSend(SseConnection connection, Long userId, String eventName, Object data) {
        Object payload = sanitizePayload(data, userId, eventName);
        try {
            emitEvent(connection.getEmitter(), eventName, payload);
            return true;
        } catch (IOException e) {
            return handleIOFailure(e, userId, eventName);
        } catch (IllegalStateException e) {
            return handleStateFailure(userId, SseErrorCode.SSE_CONNECTION_FAILED);
        } catch (Exception e) {
            return handleStateFailure(userId, SseErrorCode.SSE_SEND_FAILED);
        }
    }

    private void emitEvent(SseEmitter emitter, String eventName, Object payload) throws IOException {
        emitter.send(SseEmitter.event()
                .id(nextEventId())
                .name(eventName)
                .data(payload));
    }

    // ── 에러 처리 ──

    private boolean handleIOFailure(IOException e, Long userId, String eventName) {
        if (isClientDisconnect(e)) {
            connectionManager.cleanupConnection(userId);
            return false;
        }
        log.error("SSE 전송 실패: userId={}, eventName={}", userId, eventName, e);
        throw new CustomException(SseErrorCode.SSE_SEND_FAILED);
    }

    private boolean handleStateFailure(Long userId, SseErrorCode errorCode) {
        connectionManager.cleanupConnection(userId);
        throw new CustomException(errorCode);
    }

    private boolean isClientDisconnect(IOException e) {
        String msg = e.getMessage();
        return msg != null && CLIENT_DISCONNECT_MESSAGES.stream().anyMatch(msg::contains);
    }

    // ── 페이로드 처리 ──

    private Object sanitizePayload(Object data, Long userId, String eventName) {
        if (!isPayloadTooLarge(data)) {
            return data;
        }
        log.warn("SSE 페이로드 초과 truncate: userId={}, eventName={}", userId, eventName);
        return truncate(data.toString());
    }

    private boolean isPayloadTooLarge(Object data) {
        return data != null && data.toString().length() > MAX_STRING_LENGTH;
    }

    private String truncate(String value) {
        return value.substring(0, MAX_STRING_LENGTH - 3) + "...";
    }

    private String nextEventId() {
        return "evt_" + System.currentTimeMillis() + "_" + eventIdCounter.incrementAndGet();
    }
}
