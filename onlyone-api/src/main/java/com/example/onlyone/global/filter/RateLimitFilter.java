package com.example.onlyone.global.filter;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@ConditionalOnProperty(name = "app.rate-limit.enabled", havingValue = "true", matchIfMissing = true)
public class RateLimitFilter extends OncePerRequestFilter {

    private static final String[] AUTH_PATHS = {"/api/v1/auth/", "/api/v1/kakao/", "/api/v1/login/"};
    private static final String[] WS_PATHS = {"/ws", "/ws-native", "/ws-reactive"};
    private static final long AUTH_WINDOW_MS = 30_000L;
    private static final long GENERAL_WINDOW_MS = 60_000L;
    private static final long CLEANUP_INTERVAL_MINUTES = 5L;
    private static final int MAX_TRACKED_KEYS = 50_000;

    private final ConcurrentHashMap<String, Deque<Long>> requestCounts = new ConcurrentHashMap<>();
    private ScheduledExecutorService cleanupScheduler;

    @Value("${app.rate-limit.requests-per-minute:60}")
    private int requestsPerMinute;

    @Value("${app.rate-limit.auth-requests-per-30s:10}")
    private int authRequestsPer30s;

    @PostConstruct
    void startCleanup() {
        cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rate-limit-cleanup");
            t.setDaemon(true);
            return t;
        });
        cleanupScheduler.scheduleAtFixedRate(this::evictStaleEntries,
                CLEANUP_INTERVAL_MINUTES, CLEANUP_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    @PreDestroy
    void stopCleanup() {
        if (cleanupScheduler != null) {
            cleanupScheduler.shutdownNow();
        }
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        if (isWebSocketPath(request.getRequestURI())) {
            filterChain.doFilter(request, response);
            return;
        }

        if (isRateLimited(request)) {
            rejectWithTooManyRequests(request, response);
            return;
        }

        filterChain.doFilter(request, response);
    }

    private boolean isRateLimited(HttpServletRequest request) {
        String clientIp = getClientIp(request);
        boolean authPath = isAuthPath(request.getRequestURI());

        String key = authPath ? "auth:" + clientIp : "general:" + clientIp;
        long windowMs = authPath ? AUTH_WINDOW_MS : GENERAL_WINDOW_MS;
        int maxRequests = authPath ? authRequestsPer30s : requestsPerMinute;

        return checkAndRecordRequest(key, windowMs, maxRequests);
    }

    private void rejectWithTooManyRequests(HttpServletRequest request,
                                           HttpServletResponse response) throws IOException {
        log.warn("Rate limit exceeded for IP: {}, path: {}", getClientIp(request), request.getRequestURI());
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write("{\"error\":\"Too many requests. Please try again later.\"}");
    }

    private boolean checkAndRecordRequest(String key, long windowMs, int maxRequests) {
        if (isNewKeyOverCapacity(key)) {
            return true;
        }

        Deque<Long> timestamps = requestCounts.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>());
        return recordAndCheckLimit(timestamps, windowMs, maxRequests);
    }

    private boolean isNewKeyOverCapacity(String key) {
        if (requestCounts.size() < MAX_TRACKED_KEYS || requestCounts.containsKey(key)) {
            return false;
        }
        log.warn("Rate limit 추적 키 상한 도달 ({}), 새 IP 추적 건너뜀: {}", MAX_TRACKED_KEYS, key);
        return true;
    }

    private boolean recordAndCheckLimit(Deque<Long> timestamps, long windowMs, int maxRequests) {
        long now = System.currentTimeMillis();
        synchronized (timestamps) {
            removeExpiredEntries(timestamps, now, windowMs);
            timestamps.addLast(now);
            return timestamps.size() > maxRequests;
        }
    }

    private void evictStaleEntries() {
        long now = System.currentTimeMillis();
        int removed = 0;
        for (var it = requestCounts.entrySet().iterator(); it.hasNext(); ) {
            if (evictIfStale(it.next().getValue(), now)) {
                it.remove();
                removed++;
            }
        }
        logEvictionResult(removed);
    }

    private boolean evictIfStale(Deque<Long> deque, long now) {
        synchronized (deque) {
            removeExpiredEntries(deque, now, GENERAL_WINDOW_MS);
            return deque.isEmpty();
        }
    }

    private void removeExpiredEntries(Deque<Long> timestamps, long now, long windowMs) {
        while (!timestamps.isEmpty() && now - timestamps.peekFirst() > windowMs) {
            timestamps.pollFirst();
        }
    }

    private void logEvictionResult(int removed) {
        if (removed > 0) {
            log.debug("Rate limit cleanup: removed {} stale entries, remaining {}", removed, requestCounts.size());
        }
    }

    private boolean isWebSocketPath(String path) {
        for (String wsPath : WS_PATHS) {
            if (path.equals(wsPath) || path.startsWith(wsPath + "/")) {
                return true;
            }
        }
        return false;
    }

    private boolean isAuthPath(String path) {
        for (String authPath : AUTH_PATHS) {
            if (path.startsWith(authPath)) {
                return true;
            }
        }
        return false;
    }

    private String getClientIp(HttpServletRequest request) {
        String forwarded = extractHeader(request, "X-Forwarded-For");
        if (forwarded != null) {
            return forwarded.split(",")[0].trim();
        }
        String realIp = extractHeader(request, "X-Real-IP");
        return realIp != null ? realIp : request.getRemoteAddr();
    }

    private String extractHeader(HttpServletRequest request, String headerName) {
        String value = request.getHeader(headerName);
        return (value != null && !value.isEmpty()) ? value : null;
    }
}
