package com.example.onlyone.domain.feed.service;

import com.example.onlyone.domain.feed.dto.response.FeedCommentResponseDto;
import com.example.onlyone.domain.feed.dto.response.FeedOverviewDto;
import com.example.onlyone.domain.feed.port.FeedStoragePort.FeedDetailItem;
import com.example.onlyone.domain.feed.repository.FeedRepositoryCustom.FeedIdWithCounts;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class FeedCacheService {

    private final StringRedisTemplate redis;
    private final boolean enabled;

    static final String PERSONAL_FEED_KEY_PREFIX = "pf:";
    static final String POPULAR_FEED_KEY_PREFIX = "ppf:";
    private static final int MAX_CACHEABLE_PAGE = 5;
    private static final int DEFAULT_PAGE_SIZE = 20;

    private static final Duration PASS1_CACHE_TTL = Duration.ofSeconds(30);

    private static final int MAX_CACHE_SIZE = 2000;

    private final Cache<String, List<FeedOverviewDto>> resultCache;
    private final Cache<Long, DetailCacheEntry> detailCache;

    public FeedCacheService(StringRedisTemplate redis,
                            @Value("${app.feed.cache.enabled:false}") boolean enabled) {
        this.redis = redis;
        this.enabled = enabled;

        this.resultCache = Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterWrite(Duration.ofSeconds(10))
                .build();

        this.detailCache = Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterWrite(Duration.ofSeconds(30))
                .build();

        if (!enabled) {
            log.info("피드 캐시 비활성화 (app.feed.cache.enabled=false)");
        }
    }

    // ── Pass1 (Redis) ──

    public List<FeedIdWithCounts> getPass1(String key) {
        if (!enabled) return null;
        try {
            String raw = redis.opsForValue().get(key);
            if (raw == null || raw.isEmpty()) return null;
            return Arrays.stream(raw.split(","))
                    .map(entry -> {
                        String[] parts = entry.split(":");
                        return new FeedIdWithCounts(
                                Long.parseLong(parts[0]),
                                Long.parseLong(parts[1]),
                                Long.parseLong(parts[2]));
                    })
                    .toList();
        } catch (IllegalArgumentException e) {
            log.debug("pass1 캐시 파싱 실패: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            log.warn("pass1 캐시 조회 중 예상치 못한 오류: {}", e.getMessage());
            return null;
        }
    }

    public void putPass1(String key, List<FeedIdWithCounts> pass1) {
        if (!enabled || pass1.isEmpty()) return;
        try {
            String value = pass1.stream()
                    .map(r -> r.feedId() + ":" + r.likeCount() + ":" + r.commentCount())
                    .collect(Collectors.joining(","));
            redis.opsForValue().set(key, value, PASS1_CACHE_TTL);
        } catch (Exception e) {
            log.warn("pass1 캐시 저장 중 예상치 못한 오류: {}", e.getMessage());
        }
    }

    // ── Overview Result (Caffeine in-memory) ──

    public List<FeedOverviewDto> getResult(String key) {
        if (!enabled || key == null) return null;
        return resultCache.getIfPresent(key);
    }

    public void putResult(String key, List<FeedOverviewDto> result) {
        if (!enabled || key == null) return;
        resultCache.put(key, result);
    }

    // ── Detail (Caffeine in-memory) ──

    record DetailCacheEntry(
            FeedDetailItem detail, List<String> imageUrls,
            List<FeedCommentResponseDto> comments, long repostCount
    ) {}

    public DetailCacheEntry getDetail(Long feedId) {
        if (!enabled) return null;
        return detailCache.getIfPresent(feedId);
    }

    public void putDetail(Long feedId, FeedDetailItem detail, List<String> imageUrls,
                          List<FeedCommentResponseDto> comments, long repostCount) {
        if (!enabled) return;
        detailCache.put(feedId, new DetailCacheEntry(detail, imageUrls, comments, repostCount));
    }

    // ── Invalidation ──

    public void invalidateDetail(Long feedId) {
        if (!enabled) return;
        detailCache.invalidate(feedId);
    }

    public void invalidatePersonalFeedForUser(Long userId) {
        if (!enabled) return;
        invalidateListCaches(PERSONAL_FEED_KEY_PREFIX, userId);
    }

    public void invalidatePopularFeedForUser(Long userId) {
        if (!enabled) return;
        invalidateListCaches(POPULAR_FEED_KEY_PREFIX, userId);
    }

    public void invalidateAllFeedCachesForUser(Long userId) {
        if (!enabled) return;
        invalidatePersonalFeedForUser(userId);
        invalidatePopularFeedForUser(userId);
    }

    private void invalidateListCaches(String prefix, Long userId) {
        // in-memory 먼저 삭제 → Redis 삭제 순서로 stale repopulation 방지
        String resultPrefix = prefix + userId + ":";
        resultCache.asMap().keySet().removeIf(k -> k.startsWith(resultPrefix));

        String pass1Prefix = prefix + "p1:" + userId + ":";
        List<String> keysToDelete = new ArrayList<>();
        for (int page = 0; page <= MAX_CACHEABLE_PAGE; page++) {
            keysToDelete.add(pass1Prefix + page + ":" + DEFAULT_PAGE_SIZE);
        }
        try {
            redis.delete(keysToDelete);
        } catch (Exception e) {
            log.debug("pass1 캐시 삭제 실패: {}", e.getMessage());
        }
    }
}
