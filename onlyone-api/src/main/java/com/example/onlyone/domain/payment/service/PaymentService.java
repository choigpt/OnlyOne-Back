package com.example.onlyone.domain.payment.service;

import com.example.onlyone.domain.payment.dto.request.CancelTossPayRequest;
import com.example.onlyone.domain.payment.dto.request.ConfirmTossPayRequest;
import com.example.onlyone.domain.payment.dto.response.ConfirmTossPayResponse;
import com.example.onlyone.domain.payment.dto.request.SavePaymentRequestDto;
import com.example.onlyone.domain.payment.feign.TossPaymentClient;
import com.example.onlyone.domain.finance.exception.FinanceErrorCode;
import com.example.onlyone.global.exception.CustomException;
import com.example.onlyone.global.exception.ErrorCode;
import com.example.onlyone.global.exception.GlobalErrorCode;
import feign.FeignException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 결제 오케스트레이터.
 * 각 Phase의 트랜잭션 경계는 {@link PaymentTransactionService}에서 관리하며,
 * 이 클래스는 Phase 간 조합과 보상 로직만 담당한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentTransactionService txService;
    private final TossPaymentClient tossPaymentClient;
    private final RedisTemplate<String, Object> redisTemplate;
    private static final String REDIS_PAYMENT_KEY_PREFIX = "payment:";
    private static final String REDIS_PAYMENT_GATE_PREFIX = "payment:gate:";
    private static final long PAYMENT_INFO_TTL_SECONDS = 30 * 60;
    private static final long PAYMENT_GATE_TTL_SECONDS = 5 * 60;

    /* Redis에 결제 정보 임시 저장 (DB 트랜잭션 불필요) */
    public void savePaymentInfo(SavePaymentRequestDto dto) {
        String redisKey = REDIS_PAYMENT_KEY_PREFIX + dto.orderId();
        redisTemplate.opsForValue()
                .set(redisKey, dto.amount(), PAYMENT_INFO_TTL_SECONDS, TimeUnit.SECONDS);
    }

    /* Redis에 저장한 결제 정보와 일치 여부 확인 (DB 트랜잭션 불필요) */
    public void confirmPayment(@Valid SavePaymentRequestDto dto) {
        String redisKey = REDIS_PAYMENT_KEY_PREFIX + dto.orderId();
        Object saved = redisTemplate.opsForValue().get(redisKey);
        if (saved == null) {
            throw new CustomException(FinanceErrorCode.INVALID_PAYMENT_INFO);
        }
        long savedAmount = Long.parseLong(saved.toString());
        if (savedAmount != dto.amount()) {
            throw new CustomException(FinanceErrorCode.INVALID_PAYMENT_INFO);
        }
        redisTemplate.delete(redisKey);
    }

    /**
     * 토스페이먼츠 결제 승인 — 3-Phase 오케스트레이터 (트랜잭션 없음)
     *
     * Gate  : Redis SET NX로 orderId 중복 요청 사전 차단 (DB 히트 없이 즉시 거부)
     * Phase 1: txService.claimPayment()          [REQUIRES_NEW] — CAS 기반 Payment 선점, 즉시 커밋
     * Phase 2: tossPaymentClient.confirmPayment() [tx 없음]     — 외부 API
     * Phase 3: txService.applyPaymentResult()     [REQUIRES_NEW] — paymentKey 저장 + 지갑 원자적 잔액 반영 + DONE
     * 보상   : Phase 3 실패 → Toss 취소 + markPaymentAborted()
     */
    public ConfirmTossPayResponse confirm(ConfirmTossPayRequest req) {
        log.info("결제 승인 시작: orderId={}, amount={}", req.orderId(), req.amount());

        String gateKey = acquireIdempotencyGate(req.orderId());

        try {
            txService.claimPayment(req.orderId(), req.amount());
            ConfirmTossPayResponse response = callTossPayment(req);
            applyResultOrCompensate(req.orderId(), req.amount(), response);
            return response;
        } catch (Exception e) {
            redisTemplate.delete(gateKey);
            throw e;
        }
    }

    /* Gate: 멱등성 게이트 — DB 히트 전 중복 요청 즉시 차단 */
    private String acquireIdempotencyGate(String orderId) {
        String gateKey = REDIS_PAYMENT_GATE_PREFIX + orderId;
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(gateKey, "1", PAYMENT_GATE_TTL_SECONDS, TimeUnit.SECONDS);
        if (Boolean.FALSE.equals(acquired)) {
            throw new CustomException(FinanceErrorCode.PAYMENT_IN_PROGRESS);
        }
        return gateKey;
    }

    /* Phase 2: 토스페이먼츠 결제 호출 (트랜잭션 밖, Payment lock 없음) */
    private ConfirmTossPayResponse callTossPayment(ConfirmTossPayRequest req) {
        try {
            return tossPaymentClient.confirmPayment(req);
        } catch (Exception e) {
            throw handlePaymentFailure(req, resolvePaymentErrorCode(e), e);
        }
    }

    private ErrorCode resolvePaymentErrorCode(Exception e) {
        if (e instanceof FeignException.BadRequest) {
            return FinanceErrorCode.INVALID_PAYMENT_INFO;
        }
        if (e instanceof FeignException) {
            return FinanceErrorCode.TOSS_PAYMENT_FAILED;
        }
        return GlobalErrorCode.INTERNAL_SERVER_ERROR;
    }

    /* Phase 3: paymentKey 저장 + 지갑 반영 + 트랜잭션 기록. 실패 시 보상 */
    private void applyResultOrCompensate(String orderId, long amount, ConfirmTossPayResponse response) {
        try {
            txService.applyPaymentResult(orderId, amount, response);
        } catch (Exception e) {
            log.error("Phase 3 failed for orderId={}. Initiating Toss cancel compensation.", orderId, e);
            cancelAndAbort(response.paymentKey(), orderId);
            throw new CustomException(FinanceErrorCode.TOSS_PAYMENT_FAILED);
        }
    }

    /* 결제 실패 기록 (Controller에서 직접 호출용) */
    public void reportFail(ConfirmTossPayRequest req) {
        txService.reportFail(req);
    }

    private CustomException handlePaymentFailure(ConfirmTossPayRequest req, ErrorCode errorCode, Exception cause) {
        txService.reportFail(req);
        log.error("Payment failed: orderId={}, error={}", req.orderId(), cause.getMessage(), cause);
        return new CustomException(errorCode);
    }

    /* 보상: Toss 결제 취소 + Payment 상태 CANCELED */
    private void cancelAndAbort(String paymentKey, String orderId) {
        try {
            tossPaymentClient.cancelPayment(
                    paymentKey,
                    new CancelTossPayRequest("지갑 반영 실패로 인한 자동 취소")
            );
        } catch (Exception cancelEx) {
            log.error("Toss cancel compensation FAILED for paymentKey={}. Manual intervention required.",
                    paymentKey, cancelEx);
        }
        txService.markPaymentAborted(orderId);
    }
}
