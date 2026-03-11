package com.example.onlyone.domain.chat.stream;

import com.example.onlyone.domain.chat.repository.UserChatRoomRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

/**
 * Redis 기반 채팅방 멤버십 + 유저정보 캐시.
 *
 * Key: chat:member:{userId}:{chatRoomId}
 * Value: "nickname|profileImage"
 * TTL: 5분
 *
 * 메시지 전송/삭제 시 DB 쿼리 1개 제거.
 */
@Slf4j
@Component
public class ChatMembershipCache {

    private static final String PREFIX = "chat:member:";
    private static final Duration TTL = Duration.ofMinutes(5);

    private final StringRedisTemplate redis;
    private final UserChatRoomRepository userChatRoomRepository;

    public ChatMembershipCache(StringRedisTemplate redis,
                                UserChatRoomRepository userChatRoomRepository) {
        this.redis = redis;
        this.userChatRoomRepository = userChatRoomRepository;
    }

    /**
     * 멤버십 + 유저정보 조회 (Redis → DB fallback → Redis 캐시).
     * @return nickname, profileImage 또는 empty (미가입)
     */
    public Optional<UserInfo> getMemberInfo(Long userId, Long chatRoomId) {
        String key = buildKey(userId, chatRoomId);

        try {
            String cached = redis.opsForValue().get(key);
            if (cached != null) {
                if ("NOT_MEMBER".equals(cached)) return Optional.empty();
                String[] parts = cached.split("\\|", 2);
                return Optional.of(new UserInfo(parts[0], parts.length > 1 ? parts[1] : null));
            }
        } catch (Exception e) {
            log.warn("[membership-cache] Redis read failed: userId={}, roomId={}", userId, chatRoomId);
        }

        // DB fallback
        Optional<UserChatRoomRepository.UserInfoProjection> dbResult =
                userChatRoomRepository.findUserInfoIfMember(userId, chatRoomId);

        try {
            if (dbResult.isPresent()) {
                var info = dbResult.get();
                String value = info.getNickname() + "|" + (info.getProfileImage() != null ? info.getProfileImage() : "");
                redis.opsForValue().set(key, value, TTL);
            } else {
                redis.opsForValue().set(key, "NOT_MEMBER", Duration.ofSeconds(30));
            }
        } catch (Exception e) {
            log.warn("[membership-cache] Redis write failed: userId={}, roomId={}", userId, chatRoomId);
        }

        return dbResult.map(p -> new UserInfo(p.getNickname(), p.getProfileImage()));
    }

    public record UserInfo(String nickname, String profileImage) {}

    private String buildKey(Long userId, Long chatRoomId) {
        return PREFIX + userId + ":" + chatRoomId;
    }
}
