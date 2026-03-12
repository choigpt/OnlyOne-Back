package com.example.onlyone.domain.settlement.service;

import com.example.onlyone.domain.settlement.entity.UserSettlement;
import com.example.onlyone.domain.settlement.repository.TransferRepository;
import com.example.onlyone.domain.settlement.util.OperationIdUtil;
import com.example.onlyone.domain.settlement.repository.UserSettlementRepository;
import com.example.onlyone.domain.wallet.entity.Transfer;
import com.example.onlyone.domain.wallet.entity.Wallet;
import com.example.onlyone.domain.wallet.entity.WalletTransaction;
import com.example.onlyone.domain.wallet.entity.WalletTransactionStatus;
import com.example.onlyone.domain.wallet.repository.WalletRepository;
import com.example.onlyone.domain.wallet.repository.WalletTransactionRepository;
import com.example.onlyone.domain.wallet.entity.TransactionType;
import com.example.onlyone.domain.finance.exception.FinanceErrorCode;
import com.example.onlyone.global.exception.CustomException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LedgerWriter: user-settlement.result.v1 토픽만 구독
 *
 * balance 시점 불일치 설명:
 * WalletTransaction.balance에 기록되는 값은 Kafka 메시지 소비 시점의 JPA 엔티티 스냅샷이며,
 * 실제 지갑 잔액(captureHold/creditByUserId로 변경된 값)과 다를 수 있다.
 * 이는 감사(audit) 기록 목적이며, 정확한 실시간 잔액은 native conditional UPDATE가 보장한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LedgerWriter {

    // MySQL bulk INSERT 최적 배치 크기
    private static final int TRANSFER_BATCH_SIZE = 1000;

    private final ObjectMapper objectMapper;
    private final WalletTransactionRepository walletTransactionRepository;
    private final TransferRepository transferRepository;
    private final WalletRepository walletRepository;
    private final UserSettlementRepository userSettlementRepository;

    @Transactional
    public void writeBatch(List<ConsumerRecord<String, String>> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        List<JsonNode> events = parseAll(records);
        Set<String> existing = findExistingOperationIds(events);

        List<WalletTransaction> walletTransactions = new ArrayList<>();
        List<Transfer> transfers = new ArrayList<>();
        buildTransactionsAndTransfers(events, existing, walletTransactions, transfers);

        saveWalletTransactions(walletTransactions);
        saveTransfers(transfers);
    }

    private List<JsonNode> parseAll(List<ConsumerRecord<String, String>> records) {
        return records.stream()
                .map(r -> parse(r.value()))
                .toList();
    }

    private Set<String> findExistingOperationIds(List<JsonNode> events) {
        Set<String> candidateOperationIds = collectCandidateOperationIds(events);
        return new HashSet<>(walletTransactionRepository.findExistingOperationIds(candidateOperationIds));
    }

    private Set<String> collectCandidateOperationIds(List<JsonNode> events) {
        return events.stream()
                .map(root -> root.path("operationId").asText())
                .filter(id -> id != null && !id.isBlank())
                .flatMap(id -> Stream.of(
                        id + OperationIdUtil.OUTGOING_SUFFIX,
                        id + OperationIdUtil.INCOMING_SUFFIX))
                .collect(Collectors.toSet());
    }

    private void buildTransactionsAndTransfers(List<JsonNode> events, Set<String> existing,
                                               List<WalletTransaction> walletTransactions,
                                               List<Transfer> transfers) {
        for (JsonNode root : events) {
            String operationId = root.path("operationId").asText();
            if (operationId == null || operationId.isBlank()) continue;
            buildSingleEvent(root, operationId, existing, walletTransactions, transfers);
        }
    }

    private void buildSingleEvent(JsonNode root, String operationId, Set<String> existing,
                                  List<WalletTransaction> walletTransactions, List<Transfer> transfers) {
        long memberWalletId   = root.path("memberWalletId").asLong();
        long leaderWalletId   = root.path("leaderWalletId").asLong();
        long amount           = root.path("amount").asLong();

        Wallet memberWallet = walletRepository.getReferenceById(memberWalletId);
        Wallet leaderWallet = walletRepository.getReferenceById(leaderWalletId);
        UserSettlement us   = userSettlementRepository.getReferenceById(root.path("userSettlementId").asLong());

        WalletTransactionStatus status = resolveStatus(root.path("type").asText("SUCCESS"));

        appendIfNew(existing, operationId + OperationIdUtil.OUTGOING_SUFFIX,
                TransactionType.OUTGOING, memberWallet, leaderWallet, amount, status,
                us.getUserSettlementId(), walletTransactions, transfers);

        appendIfNew(existing, operationId + OperationIdUtil.INCOMING_SUFFIX,
                TransactionType.INCOMING, leaderWallet, memberWallet, amount, status,
                us.getUserSettlementId(), walletTransactions, transfers);
    }

    private WalletTransactionStatus resolveStatus(String type) {
        return "SUCCESS".equals(type) ? WalletTransactionStatus.COMPLETED : WalletTransactionStatus.FAILED;
    }

    private void appendIfNew(Set<String> existing, String txOperationId,
                             TransactionType txType, Wallet wallet, Wallet targetWallet,
                             long amount, WalletTransactionStatus status, long userSettlementId,
                             List<WalletTransaction> walletTransactions, List<Transfer> transfers) {
        if (existing.contains(txOperationId)) return;

        WalletTransaction tx = WalletTransaction.builder()
                .operationId(txOperationId)
                .type(txType)
                .wallet(wallet)
                .targetWallet(targetWallet)
                .amount(amount)
                .balance(wallet.getPostedBalance())
                .walletTransactionStatus(status)
                .build();
        walletTransactions.add(tx);

        Transfer transfer = Transfer.builder()
                .userSettlementId(userSettlementId)
                .walletTransaction(tx)
                .build();
        transfers.add(transfer);
        tx.updateTransfer(transfer);
    }

    private void saveWalletTransactions(List<WalletTransaction> walletTransactions) {
        if (walletTransactions.isEmpty()) return;
        saveBulkOrFallback(walletTransactions);
    }

    private void saveBulkOrFallback(List<WalletTransaction> walletTransactions) {
        try {
            walletTransactionRepository.saveAll(walletTransactions);
            walletTransactionRepository.flush();
        } catch (DataIntegrityViolationException dup) {
            insertIndividuallyIgnoringDuplicate(walletTransactions);
        }
    }

    private void saveTransfers(List<Transfer> transfers) {
        if (transfers.isEmpty()) return;
        try {
            saveTransfersInBatches(transfers);
        } catch (DataIntegrityViolationException dup) {
            log.warn("Transfer 배치 저장 중 중복 감지, 개별 저장으로 전환: {}", dup.getMessage());
            insertTransfersIndividually(transfers);
        }
    }

    private void saveTransfersInBatches(List<Transfer> transfers) {
        for (int i = 0; i < transfers.size(); i += TRANSFER_BATCH_SIZE) {
            int endIndex = Math.min(i + TRANSFER_BATCH_SIZE, transfers.size());
            transferRepository.saveAll(transfers.subList(i, endIndex));
        }
        transferRepository.flush();
    }

    private void insertTransfersIndividually(List<Transfer> transfers) {
        transfers.forEach(this::saveTransferIgnoringDuplicate);
    }

    private void saveTransferIgnoringDuplicate(Transfer transfer) {
        try {
            transferRepository.saveAndFlush(transfer);
        } catch (DataIntegrityViolationException ignored) {
            log.debug("Transfer 중복 스킵: userSettlementId={}", transfer.getUserSettlementId());
        }
    }

    private JsonNode parse(String s) {
        try {
            return objectMapper.readTree(s);
        } catch (Exception e) {
            throw new CustomException(FinanceErrorCode.INVALID_EVENT_PAYLOAD);
        }
    }

    private void insertIndividuallyIgnoringDuplicate(List<WalletTransaction> walletTransactions) {
        Set<String> existing = new HashSet<>(
                walletTransactionRepository.findExistingOperationIds(
                        walletTransactions.stream().map(WalletTransaction::getOperationId).collect(Collectors.toSet())
                )
        );
        walletTransactions.stream()
                .filter(tx -> !existing.contains(tx.getOperationId()))
                .forEach(this::saveTransactionIgnoringDuplicate);
    }

    private void saveTransactionIgnoringDuplicate(WalletTransaction tx) {
        try {
            walletTransactionRepository.saveAndFlush(tx);
        } catch (DataIntegrityViolationException ignored) {
            // 동시경합으로 중복키면 스킵
        }
    }
}
