package com.example.onlyone.domain.feed.service;

import com.example.onlyone.domain.club.repository.ClubRepository;
import com.example.onlyone.domain.club.repository.UserClubRepository;
import com.example.onlyone.domain.feed.dto.response.FeedCommentResponseDto;
import com.example.onlyone.domain.feed.dto.response.FeedDetailResponseDto;
import com.example.onlyone.domain.feed.dto.response.FeedOverviewDto;
import com.example.onlyone.domain.feed.dto.response.FeedSummaryResponseDto;
import com.example.onlyone.domain.feed.port.FeedStoragePort;
import com.example.onlyone.domain.feed.port.FeedStoragePort.FeedDetailItem;
import com.example.onlyone.domain.feed.repository.FeedRepositoryCustom.FeedIdWithCounts;
import com.example.onlyone.domain.feed.service.FeedCacheService.DetailCacheEntry;
import com.example.onlyone.domain.club.exception.ClubErrorCode;
import com.example.onlyone.domain.feed.exception.FeedErrorCode;
import com.example.onlyone.domain.user.service.UserService;
import com.example.onlyone.global.exception.CustomException;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class FeedQueryService {

    private final ClubRepository clubRepository;
    private final FeedStoragePort feedStoragePort;
    private final UserService userService;
    private final UserClubRepository userClubRepository;
    private final FeedCacheService cache;
    private final FeedRenderService renderService;

    /** 캐시 메모리 절약을 위해 처음 5페이지만 Redis에 캐싱 (이후 페이지는 DB 직접 조회) */
    private static final int MAX_CACHEABLE_PAGE = 5;
    /** 피드 상세 조회 시 댓글 첫 페이지 기본 크기 */
    private static final int DEFAULT_COMMENT_PAGE_SIZE = 20;
    private static final String PERSONAL_FEED_KEY_PREFIX = FeedCacheService.PERSONAL_FEED_KEY_PREFIX;
    private static final String POPULAR_FEED_KEY_PREFIX = FeedCacheService.POPULAR_FEED_KEY_PREFIX;

    // ── 모임 피드 ──

    public Page<FeedSummaryResponseDto> getFeedList(Long clubId, Pageable pageable) {
        if (!clubRepository.existsById(clubId)) {
            throw new CustomException(ClubErrorCode.CLUB_NOT_FOUND);
        }
        return feedStoragePort.findClubFeedSummaries(clubId, pageable);
    }

    // ── 피드 상세 ──

    public FeedDetailResponseDto getFeedDetail(Long clubId, Long feedId) {
        Long currentUserId = userService.getCurrentUserId();

        DetailCacheEntry cached = cache.getDetail(feedId);
        if (cached != null) {
            return buildDetailFromCache(cached, feedId, currentUserId);
        }

        FeedDetailItem detail = feedStoragePort.findFeedDetailWithRelations(feedId, clubId)
                .orElseThrow(() -> new CustomException(FeedErrorCode.FEED_NOT_FOUND));

        List<String> imageUrls = detail.imageUrls();
        boolean isLiked = feedStoragePort.isLikedByUser(feedId, currentUserId);
        boolean isMine = detail.userId().equals(currentUserId);

        Pageable commentPage = PageRequest.of(0, DEFAULT_COMMENT_PAGE_SIZE, Sort.by(Sort.Direction.ASC, "createdAt"));
        List<FeedCommentResponseDto> comments = feedStoragePort.findCommentsByFeedId(feedId, commentPage).stream()
                .map(c -> c.toDto(currentUserId)).toList();
        long repostCount = feedStoragePort.countRepostsByParentId(feedId);

        cache.putDetail(feedId, detail, imageUrls, comments, repostCount);
        return FeedDetailResponseDto.from(detail, imageUrls, isLiked, isMine, comments, repostCount);
    }

    // ── 전체 피드 ──

    public List<FeedOverviewDto> getPersonalFeed(Pageable pageable, Long cursor) {
        return loadPersonalFeed(pageable, cursor);
    }

    public List<FeedOverviewDto> getPersonalFeed(Pageable pageable) {
        return loadPersonalFeed(pageable, null);
    }

    public List<FeedOverviewDto> getPopularFeed(Pageable pageable) {
        return loadPopularFeed(pageable);
    }

    // ── private ──

    private List<FeedOverviewDto> loadPersonalFeed(Pageable pageable, Long cursor) {
        Long userId = userService.getCurrentUserId();

        // 커서 기반은 캐시 없이 직접 조회
        if (cursor != null) {
            List<Long> clubIds = userClubRepository.findAccessibleClubIds(userId);
            if (clubIds.isEmpty()) return Collections.emptyList();
            List<FeedIdWithCounts> pass1 = feedStoragePort.findPersonalFeedIdsCursor(clubIds, cursor, pageable.getPageSize());
            return renderService.buildOverviewList(pass1, userId);
        }

        // offset 기반 — pass1 캐시 → result 캐시 → DB fallback
        return loadFeedWithCache(PERSONAL_FEED_KEY_PREFIX, pageable, userId, () -> {
            List<Long> clubIds = userClubRepository.findAccessibleClubIds(userId);
            if (clubIds.isEmpty()) return Collections.emptyList();
            return feedStoragePort.findPersonalFeedIds(clubIds, pageable);
        });
    }

    private List<FeedOverviewDto> loadPopularFeed(Pageable pageable) {
        Long userId = userService.getCurrentUserId();

        return loadFeedWithCache(POPULAR_FEED_KEY_PREFIX, pageable, userId, () -> {
            List<Long> clubIds = userClubRepository.findAccessibleClubIds(userId);
            if (clubIds.isEmpty()) return Collections.emptyList();
            return feedStoragePort.findPopularFeedIds(clubIds, pageable);
        });
    }

    /**
     * 공통 캐시 흐름: result 캐시 확인 → pass1 캐시 확인 → DB 조회 → 캐시 저장.
     * MAX_CACHEABLE_PAGE 이하인 페이지만 캐싱한다.
     */
    private List<FeedOverviewDto> loadFeedWithCache(String keyPrefix, Pageable pageable, Long userId,
                                                     Supplier<List<FeedIdWithCounts>> pass1Fetcher) {
        boolean cacheable = pageable.getPageNumber() <= MAX_CACHEABLE_PAGE;
        String pageSuffix = userId + ":" + pageable.getPageNumber() + ":" + pageable.getPageSize();

        // 1) result 캐시 확인
        List<FeedOverviewDto> cachedResult = getCachedResult(keyPrefix, pageSuffix, cacheable);
        if (cachedResult != null) return cachedResult;

        // 2) pass1 캐시 확인 → DB fallback
        List<FeedIdWithCounts> pass1 = resolvePass1(keyPrefix, pageSuffix, cacheable, pass1Fetcher);
        if (pass1.isEmpty()) return Collections.emptyList();

        // 3) 렌더링 + result 캐시 저장
        List<FeedOverviewDto> result = renderService.buildOverviewList(pass1, userId);
        if (cacheable) cache.putResult(keyPrefix + pageSuffix, result);
        return result;
    }

    /** result 캐시 조회. 캐시 대상이 아니면 null 반환. */
    private List<FeedOverviewDto> getCachedResult(String keyPrefix, String pageSuffix, boolean cacheable) {
        if (!cacheable) return null;
        return cache.getResult(keyPrefix + pageSuffix);
    }

    /** pass1 캐시 조회 → 미스 시 DB 조회 후 캐시 저장. */
    private List<FeedIdWithCounts> resolvePass1(String keyPrefix, String pageSuffix, boolean cacheable,
                                                 Supplier<List<FeedIdWithCounts>> pass1Fetcher) {
        if (cacheable) {
            String pass1Key = keyPrefix + "p1:" + pageSuffix;
            List<FeedIdWithCounts> cached = cache.getPass1(pass1Key);
            if (cached != null) return cached;
            return fetchAndCachePass1(pass1Key, pass1Fetcher);
        }
        return pass1Fetcher.get();
    }

    /** DB에서 pass1을 조회하고 결과가 비어있지 않으면 캐시에 저장. */
    private List<FeedIdWithCounts> fetchAndCachePass1(String pass1Key, Supplier<List<FeedIdWithCounts>> pass1Fetcher) {
        List<FeedIdWithCounts> pass1 = pass1Fetcher.get();
        if (!pass1.isEmpty()) cache.putPass1(pass1Key, pass1);
        return pass1;
    }

    private FeedDetailResponseDto buildDetailFromCache(DetailCacheEntry cached, Long feedId, Long currentUserId) {
        boolean isLiked = feedStoragePort.isLikedByUser(feedId, currentUserId);
        boolean isMine = cached.detail().userId().equals(currentUserId);
        List<FeedCommentResponseDto> comments = cached.comments().stream()
                .map(c -> new FeedCommentResponseDto(c.commentId(), c.userId(), c.nickname(), c.profileImage(),
                        c.content(), c.createdAt(), c.userId().equals(currentUserId)))
                .toList();
        return FeedDetailResponseDto.from(cached.detail(), cached.imageUrls(), isLiked, isMine, comments, cached.repostCount());
    }
}
