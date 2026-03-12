package com.example.onlyone.domain.settlement.service;

import com.example.onlyone.common.event.SettlementCompletedEvent;
import com.example.onlyone.domain.settlement.event.SettlementProcessEvent;
import com.example.onlyone.domain.settlement.repository.SettlementRepository;
import com.example.onlyone.domain.settlement.util.OperationIdUtil;
import com.example.onlyone.domain.settlement.repository.UserSettlementRepository;
import com.example.onlyone.domain.finance.exception.FinanceErrorCode;
import com.example.onlyone.domain.wallet.repository.WalletRepository;
import com.example.onlyone.global.exception.CustomException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 정산 이벤트 비즈니스 로직 프로세서.
 * Kafka 리스너에서 수신한 메시지의 비즈니스 로직을 처리한다.
 */
@Slf4j
@Component
public class SettlementEventProcessor {

    // settlementId * 10_000 + participantId로 고유 aggregateId 생성 (정산당 최대 10,000명)
    private static final long OUTBOX_AGGREGATE_MULTIPLIER = 10_000;

    private final ObjectMapper objectMapper;
    private final UserSettlementRepository userSettlementRepository;
    private final UserSettlementService userSettlementService;
    private final SettlementRepository settlementRepository;
    private final WalletRepository walletRepository;
    private final ApplicationEventPublisher eventPublisher;
    private final TransactionTemplate txTemplate;
    private final OutboxAppender outboxAppender;
    private final LedgerWriter ledgerWriter;

    public SettlementEventProcessor(
            ObjectMapper objectMapper,
            UserSettlementRepository userSettlementRepository,
            UserSettlementService userSettlementService,
            SettlementRepository settlementRepository,
            WalletRepository walletRepository,
            ApplicationEventPublisher eventPublisher,
            @Qualifier("requiresNewTransactionTemplate") TransactionTemplate requiresNewTransactionTemplate,
            OutboxAppender outboxAppender,
            LedgerWriter ledgerWriter
    ) {
        this.objectMapper = objectMapper;
        this.userSettlementRepository = userSettlementRepository;
        this.userSettlementService = userSettlementService;
        this.settlementRepository = settlementRepository;
        this.walletRepository = walletRepository;
        this.eventPublisher = eventPublisher;
        this.txTemplate = requiresNewTransactionTemplate;
        this.outboxAppender = outboxAppender;
        this.ledgerWriter = ledgerWriter;
    }

    // ========== 정산 처리 (settlement.process.v1) ==========

    /**
     * 정산 이벤트 payload 문자열 리스트를 처리한다.
     */
    public void processSettlementEvents(List<String> payloads) {
        for (String payload : payloads) {
            SettlementProcessEvent event = parseSettlementEvent(payload);
            processSettlementBatch(event);
        }
    }

    /**
     * 원장 기록 이벤트 처리.
     */
    public void processLedgerEvents(List<ConsumerRecord<String, String>> records) {
        ledgerWriter.writeBatch(records);
    }

    // ========== 배치 정산 처리 ==========

    private void processSettlementBatch(SettlementProcessEvent event) {
        List<Long> sortedUserIds = sortUserIdsForLockOrder(event.targetUserIds());

        try {
            txTemplate.executeWithoutResult(status ->
                    executeSettlementTransaction(status, event, sortedUserIds));
            completeSettlement(event);
        } catch (Exception e) {
            log.error("배치 정산 실패: settlementId={}", event.settlementId(), e);
            revertSettlementToFailed(event.settlementId());
        }
    }

    /**
     * 락 순서를 userId 오름차순으로 고정하여 데드락 방지.
     */
    private List<Long> sortUserIdsForLockOrder(List<Long> userIds) {
        List<Long> sorted = new ArrayList<>(userIds);
        Collections.sort(sorted);
        return sorted;
    }

    /**
     * 단일 트랜잭션 내에서 captureHold, markCompleted, outbox append를 수행한다.
     */
    private void executeSettlementTransaction(
            org.springframework.transaction.TransactionStatus status,
            SettlementProcessEvent event,
            List<Long> sortedUserIds) {

        if (!batchCaptureHold(status, event.settlementId(), sortedUserIds, event.costPerUser())) {
            return;
        }

        userSettlementRepository.batchMarkCompleted(
                event.settlementId(), sortedUserIds, LocalDateTime.now());

        Map<Long, Long> walletIdMap = buildWalletIdMap(sortedUserIds);
        Map<Long, Long> usIdMap = buildUserSettlementIdMap(event.settlementId(), sortedUserIds);

        appendOutboxForParticipants(event, sortedUserIds, walletIdMap, usIdMap);
    }

    /**
     * 배치 captureHold를 수행하고 부분 실패 시 롤백을 설정한다.
     *
     * @return true면 성공, false면 롤백 설정됨
     */
    private boolean batchCaptureHold(
            org.springframework.transaction.TransactionStatus status,
            Long settlementId,
            List<Long> sortedUserIds,
            long amount) {

        int captured = walletRepository.batchCaptureHold(sortedUserIds, amount);
        if (captured != sortedUserIds.size()) {
            log.error("배치 captureHold 부분 실패: settlementId={}, expected={}, captured={}",
                    settlementId, sortedUserIds.size(), captured);
            status.setRollbackOnly();
            return false;
        }
        return true;
    }

    private Map<Long, Long> buildWalletIdMap(List<Long> userIds) {
        Map<Long, Long> map = new HashMap<>();
        for (var row : walletRepository.findWalletIdsByUserIds(userIds)) {
            map.put(row.getUserId(), row.getWalletId());
        }
        return map;
    }

    private Map<Long, Long> buildUserSettlementIdMap(Long settlementId, List<Long> userIds) {
        Map<Long, Long> map = new HashMap<>();
        for (var row : userSettlementRepository
                .findUserSettlementIdsBySettlementIdAndUserIds(settlementId, userIds)) {
            map.put(row.getUserId(), row.getUserSettlementId());
        }
        return map;
    }

    private void appendOutboxForParticipants(
            SettlementProcessEvent event,
            List<Long> sortedUserIds,
            Map<Long, Long> walletIdMap,
            Map<Long, Long> usIdMap) {

        for (Long participantId : sortedUserIds) {
            appendSuccessOutbox(event, participantId,
                    walletIdMap.getOrDefault(participantId, 0L),
                    usIdMap.getOrDefault(participantId, 0L));
        }
    }

    private void appendSuccessOutbox(SettlementProcessEvent event, Long participantId,
                                     Long memberWalletId, Long userSettlementId) {
        Long settlementId = event.settlementId();
        String operationId = OperationIdUtil.generate(settlementId, participantId);
        outboxAppender.append(
                "UserSettlement",
                settlementId * OUTBOX_AGGREGATE_MULTIPLIER + participantId,
                "ParticipantSettlementResult",
                String.valueOf(participantId),
                Map.of(
                        "type", "SUCCESS",
                        "operationId", operationId,
                        "occurredAt", Instant.now().toString(),
                        "settlementId", settlementId,
                        "userSettlementId", userSettlementId,
                        "participantId", participantId,
                        "memberWalletId", memberWalletId,
                        "leaderId", event.leaderId(),
                        "leaderWalletId", event.leaderWalletId(),
                        "amount", event.costPerUser()
                )
        );
    }

    private void completeSettlement(SettlementProcessEvent event) {
        txTemplate.executeWithoutResult(status -> {
            int updated = settlementRepository.markCompleted(event.settlementId(), LocalDateTime.now());
            if (updated == 0) {
                log.info("Settlement already completed, skipping. id={}", event.settlementId());
                return;
            }

            userSettlementService.creditToLeader(event.leaderId(), event.totalAmount());

            eventPublisher.publishEvent(new SettlementCompletedEvent(
                    event.settlementId(), event.scheduleId(), event.clubId(), LocalDateTime.now()));
        });
    }

    private void revertSettlementToFailed(Long settlementId) {
        try {
            txTemplate.executeWithoutResult(status ->
                    settlementRepository.revertToFailed(settlementId));
        } catch (Exception e) {
            log.error("Failed to revert settlement to FAILED. id={}", settlementId, e);
        }
    }

    // ========== Parsing ==========

    public SettlementProcessEvent parseSettlementEvent(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;
            return objectMapper.treeToValue(payload, SettlementProcessEvent.class);
        } catch (Exception e) {
            throw new CustomException(FinanceErrorCode.INVALID_EVENT_PAYLOAD);
        }
    }
}
