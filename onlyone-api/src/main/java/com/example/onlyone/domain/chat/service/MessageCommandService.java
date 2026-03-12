package com.example.onlyone.domain.chat.service;

import com.example.onlyone.domain.chat.dto.ChatMessageItemDto;
import com.example.onlyone.domain.chat.dto.ChatMessageResponse;
import com.example.onlyone.domain.chat.port.ChatMessageStoragePort;
import com.example.onlyone.domain.chat.repository.UserChatRoomRepository;
import com.example.onlyone.domain.chat.stream.ChatMessageCache;
import com.example.onlyone.domain.chat.stream.ChatMembershipCache;
import com.example.onlyone.domain.chat.util.MessageUtils;
import com.example.onlyone.domain.chat.exception.ChatErrorCode;
import com.example.onlyone.global.exception.CustomException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageCommandService {

    private final ChatMessageStoragePort chatMessageStoragePort;
    private final UserChatRoomRepository userChatRoomRepository;
    private final ChatPublisher chatPublisher;
    private final ChatMessageCache chatMessageCache;
    private final ChatMembershipCache membershipCache;
    private final AsyncMessageService asyncMessageService;
    private final ObjectMapper objectMapper;

    private static final int MAX_TEXT_LENGTH = 2000;

    /**
     * REST 경로: 멤버십 캐시 + Redis Streams 비동기 저장 + 즉시 응답.
     * DB 커넥션 사용 0 (캐시 히트 시).
     */
    public ChatMessageResponse sendAndPublish(Long chatRoomId, Long userId, String text) {
        if (text == null || text.isBlank()) throw new CustomException(ChatErrorCode.MESSAGE_BAD_REQUEST);

        // 멤버십 + 유저정보: Redis 캐시 우선 (DB fallback + 캐시 저장)
        ChatMembershipCache.UserInfo userInfo = membershipCache.getMemberInfo(userId, chatRoomId)
                .orElseThrow(() -> new CustomException(ChatErrorCode.FORBIDDEN_CHAT_ROOM));

        String storedText = resolveStoredText(text);
        LocalDateTime now = LocalDateTime.now();

        // Redis Pub/Sub 즉시 발행
        ChatMessageResponse response = ChatMessageResponse.forWebSocket(
                chatRoomId, userId, userInfo.nickname(), userInfo.profileImage(), storedText);
        publish(chatRoomId, response);

        // Redis Streams 비동기 DB 저장 + 읽기 캐시 반영
        asyncMessageService.saveMessageAsync(chatRoomId, userId,
                userInfo.nickname(), userInfo.profileImage(), storedText);

        return response;
    }

    /**
     * WebSocket 경로: DB 저장 없이 즉시 Redis 발행 (비동기 저장은 AsyncMessageService가 담당)
     */
    public void publishImmediately(Long chatRoomId, Long senderId,
                                   String nickname, String profileImage, String rawText) {
        ChatMessageResponse response = ChatMessageResponse.forWebSocket(
                chatRoomId, senderId, nickname, profileImage, rawText);
        publish(chatRoomId, response);

        chatMessageCache.addMessage(chatRoomId, senderId, nickname, profileImage, rawText, LocalDateTime.now());
    }

    /**
     * 메시지 DB 저장 (AsyncMessageService에서도 호출)
     */
    @Transactional
    public ChatMessageResponse saveMessage(Long chatRoomId, Long userId, String text) {
        if (text == null || text.isBlank()) throw new CustomException(ChatErrorCode.MESSAGE_BAD_REQUEST);

        UserChatRoomRepository.UserInfoProjection userInfo = userChatRoomRepository
                .findUserInfoIfMember(userId, chatRoomId)
                .orElseThrow(() -> new CustomException(ChatErrorCode.FORBIDDEN_CHAT_ROOM));
        String nickname = userInfo.getNickname();
        String profileImage = userInfo.getProfileImage();

        String storedText = resolveStoredText(text);

        ChatMessageItemDto item = chatMessageStoragePort.save(
                chatRoomId, userId, nickname, profileImage,
                storedText, LocalDateTime.now());

        return ChatMessageResponse.from(item);
    }

    @Transactional
    public void deleteMessage(Long messageId, Long userId) {
        ChatMessageItemDto item = chatMessageStoragePort.findById(messageId)
                .orElseThrow(() -> new CustomException(ChatErrorCode.MESSAGE_NOT_FOUND));
        if (item.deleted()) throw new CustomException(ChatErrorCode.MESSAGE_CONFLICT);
        if (!item.senderId().equals(userId)) throw new CustomException(ChatErrorCode.MESSAGE_FORBIDDEN);
        chatMessageStoragePort.markAsDeleted(messageId);
    }

    // ── private ──

    private void publish(Long chatRoomId, ChatMessageResponse response) {
        try {
            String payload = objectMapper.writeValueAsString(response);
            chatPublisher.publish(chatRoomId, payload);
        } catch (JsonProcessingException e) {
            log.error("메시지 JSON 직렬화 실패: chatRoomId={}", chatRoomId, e);
            throw new CustomException(ChatErrorCode.MESSAGE_SERVER_ERROR);
        }
    }

    private String resolveStoredText(String text) {
        if (!MessageUtils.isImageMessage(text)) {
            return truncateText(text);
        }
        return resolveImageText(text);
    }

    private String truncateText(String text) {
        return text.length() > MAX_TEXT_LENGTH ? text.substring(0, MAX_TEXT_LENGTH) : text;
    }

    private String resolveImageText(String text) {
        String url = MessageUtils.extractImageUrl(text);
        validateImageUrl(url);
        return MessageUtils.IMAGE_PREFIX + url;
    }

    private void validateImageUrl(String url) {
        if (!MessageUtils.isValidImageUrlFormat(url)) {
            throw new CustomException(ChatErrorCode.MESSAGE_BAD_REQUEST);
        }
        if (!MessageUtils.hasValidImageExtension(url)) {
            throw new CustomException(ChatErrorCode.INVALID_IMAGE_CONTENT_TYPE);
        }
    }
}
