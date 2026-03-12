package com.example.onlyone.global.sse;

import com.example.onlyone.domain.notification.dto.response.NotificationItemDto;
import com.example.onlyone.domain.notification.dto.response.NotificationSseDto;
import com.example.onlyone.domain.notification.port.NotificationStoragePort;
import com.example.onlyone.domain.notification.service.NotificationUndeliveredCache;
import com.example.onlyone.sse.service.SseConnectionManager;
import com.example.onlyone.sse.service.SseEventSender;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

/**
 * 놓친 알림 복구 — 별도 Bean으로 분리하여 @Transactional 프록시 정상 동작 보장.
 * Redis 캐시 우선 조회 → DB fallback으로 재연결 시 DB 부하를 줄인다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SseMissedNotificationRecovery {

    private static final int MAX_RECOVERY_SIZE = 50;

    private final NotificationStoragePort storagePort;
    private final NotificationUndeliveredCache undeliveredCache;
    private final SseEventSender sseEventSender;
    private final SseConnectionManager connectionManager;

    @Transactional
    public void recover(Long userId) {
        if (!connectionManager.isUserConnected(userId)) {
            log.debug("SSE 미연결 상태 — 복구 스킵: userId={}", userId);
            return;
        }
        try {
            recoverFromCache(userId);
            recoverFromDb(userId);
        } catch (Exception e) {
            log.warn("놓친 알림 복구 실패: userId={}", userId, e);
        }
    }

    private void recoverFromCache(Long userId) {
        List<NotificationItemDto> cached = undeliveredCache.popAll(userId);
        int sent = sendAndMark(userId, cached);
        if (sent > 0) {
            log.debug("캐시 복구: userId={}, sent={}/{}", userId, sent, cached.size());
        }
    }

    private void recoverFromDb(Long userId) {
        List<NotificationItemDto> dbMissed = storagePort
                .findUndeliveredByUserId(userId, MAX_RECOVERY_SIZE);
        int sent = sendAndMark(userId, dbMissed);
        if (sent > 0) {
            log.debug("DB 복구: userId={}, sent={}/{}", userId, sent, dbMissed.size());
        }
    }

    private int sendAndMark(Long userId, List<NotificationItemDto> items) {
        if (items.isEmpty()) return 0;

        List<Long> sentIds = sendAllDirect(userId, items);
        if (!sentIds.isEmpty()) {
            storagePort.markDeliveredByIds(sentIds);
        }
        return sentIds.size();
    }

    private List<Long> sendAllDirect(Long userId, List<NotificationItemDto> missed) {
        List<Long> sentIds = new ArrayList<>();
        for (NotificationItemDto item : missed) {
            boolean sent = sseEventSender.sendEventDirect(
                    userId, "notification", NotificationSseDto.from(item));
            if (sent) {
                sentIds.add(item.notificationId());
            }
        }
        return sentIds;
    }
}
