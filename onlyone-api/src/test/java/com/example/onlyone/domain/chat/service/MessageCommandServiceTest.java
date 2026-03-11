package com.example.onlyone.domain.chat.service;

import com.example.onlyone.domain.chat.dto.ChatMessageItemDto;
import com.example.onlyone.domain.chat.dto.ChatMessageResponse;
import com.example.onlyone.domain.chat.port.ChatMessageStoragePort;
import com.example.onlyone.domain.chat.repository.UserChatRoomRepository;
import com.example.onlyone.domain.chat.stream.ChatMessageCache;
import com.example.onlyone.domain.chat.stream.ChatMembershipCache;
import com.example.onlyone.domain.chat.exception.ChatErrorCode;
import com.example.onlyone.global.exception.CustomException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;

import static com.example.onlyone.domain.chat.fixture.ChatFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("MessageCommandService 단위 테스트")
class MessageCommandServiceTest {

    @InjectMocks private MessageCommandService messageCommandService;
    @Mock private ChatMessageStoragePort chatMessageStoragePort;
    @Mock private UserChatRoomRepository userChatRoomRepository;
    @Mock private ChatPublisher chatPublisher;
    @Mock private ChatMessageCache chatMessageCache;
    @Mock private ChatMembershipCache membershipCache;
    @Mock private AsyncMessageService asyncMessageService;
    @Mock private ObjectMapper objectMapper;

    private ChatMessageItemDto stubItem(Long messageId, Long chatRoomId, String text) {
        return new ChatMessageItemDto(messageId, chatRoomId, DEFAULT_USER_ID,
                "테스트유저", "https://example.com/profile.jpg", text,
                DEFAULT_SENT_AT, false);
    }

    // ========== sendAndPublish 테스트 ==========

    @Nested
    @DisplayName("메시지 저장 + 발행 (sendAndPublish)")
    class SendAndPublish {

        @Test
        @DisplayName("성공: 멤버십 캐시 조회 → Redis 발행 → 비동기 저장")
        void success() throws Exception {
            given(membershipCache.getMemberInfo(DEFAULT_USER_ID, 1L))
                    .willReturn(Optional.of(new ChatMembershipCache.UserInfo("테스트유저", "https://example.com/profile.jpg")));
            given(objectMapper.writeValueAsString(any())).willReturn("{\"text\":\"안녕하세요!\"}");

            ChatMessageResponse response =
                    messageCommandService.sendAndPublish(1L, DEFAULT_USER_ID, "안녕하세요!");

            assertThat(response.chatRoomId()).isEqualTo(1L);
            assertThat(response.senderId()).isEqualTo(DEFAULT_USER_ID);
            then(chatPublisher).should().publish(eq(1L), anyString());
            then(asyncMessageService).should().saveMessageAsync(eq(1L), eq(DEFAULT_USER_ID),
                    eq("테스트유저"), eq("https://example.com/profile.jpg"), eq("안녕하세요!"));
        }

        @Test
        @DisplayName("실패: 빈 텍스트면 MESSAGE_BAD_REQUEST")
        void failBlankText() {
            assertThatThrownBy(() ->
                    messageCommandService.sendAndPublish(1L, DEFAULT_USER_ID, "   "))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_BAD_REQUEST);
        }

        @Test
        @DisplayName("실패: null 텍스트면 MESSAGE_BAD_REQUEST")
        void failNullText() {
            assertThatThrownBy(() ->
                    messageCommandService.sendAndPublish(1L, DEFAULT_USER_ID, null))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_BAD_REQUEST);
        }

        @Test
        @DisplayName("실패: 멤버십 없으면 FORBIDDEN_CHAT_ROOM")
        void failNotMember() {
            given(membershipCache.getMemberInfo(DEFAULT_USER_ID, 999L))
                    .willReturn(Optional.empty());

            assertThatThrownBy(() ->
                    messageCommandService.sendAndPublish(999L, DEFAULT_USER_ID, "메시지"))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.FORBIDDEN_CHAT_ROOM);
        }

        @Test
        @DisplayName("실패: JSON 직렬화 실패시 MESSAGE_SERVER_ERROR")
        void failJsonSerialization() throws Exception {
            given(membershipCache.getMemberInfo(DEFAULT_USER_ID, 1L))
                    .willReturn(Optional.of(new ChatMembershipCache.UserInfo("테스트유저", null)));
            given(objectMapper.writeValueAsString(any()))
                    .willThrow(new JsonProcessingException("fail") {});

            assertThatThrownBy(() ->
                    messageCommandService.sendAndPublish(1L, DEFAULT_USER_ID, "테스트"))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_SERVER_ERROR);
        }
    }

    // ========== publishImmediately 테스트 ==========

    @Nested
    @DisplayName("즉시 발행 (WebSocket)")
    class PublishImmediately {

        @Test
        @DisplayName("성공: DB 저장 없이 Redis 발행 + 캐시 반영")
        void success() throws Exception {
            given(objectMapper.writeValueAsString(any())).willReturn("{\"text\":\"hello\"}");

            messageCommandService.publishImmediately(
                    1L, DEFAULT_USER_ID, "테스트유저", null, "hello");

            then(chatPublisher).should().publish(eq(1L), eq("{\"text\":\"hello\"}"));
            then(chatMessageCache).should().addMessage(eq(1L), eq(DEFAULT_USER_ID),
                    eq("테스트유저"), isNull(), eq("hello"), any(LocalDateTime.class));
            then(chatMessageStoragePort).shouldHaveNoInteractions();
        }

        @Test
        @DisplayName("성공: 이미지 메시지도 올바르게 발행된다")
        void successWithImage() throws Exception {
            given(objectMapper.writeValueAsString(any())).willReturn("{\"imageUrl\":\"url\"}");

            messageCommandService.publishImmediately(
                    1L, DEFAULT_USER_ID, "테스트유저", null, "IMAGE::https://cdn.example.com/img.jpg");

            then(chatPublisher).should().publish(eq(1L), eq("{\"imageUrl\":\"url\"}"));
        }
    }

    // ========== 메시지 저장 테스트 (saveMessage) ==========

    @Nested
    @DisplayName("메시지 저장")
    class SaveMessage {

        private final UserChatRoomRepository.UserInfoProjection STUB_PROJECTION =
                new UserChatRoomRepository.UserInfoProjection() {
                    @Override public String getNickname() { return "테스트유저"; }
                    @Override public String getProfileImage() { return "https://example.com/profile.jpg"; }
                };

        @Test
        @DisplayName("성공: 텍스트 메시지가 저장된다")
        void saveTextMessage_success() {
            given(userChatRoomRepository.findUserInfoIfMember(DEFAULT_USER_ID, 1L))
                    .willReturn(Optional.of(STUB_PROJECTION));
            given(chatMessageStoragePort.save(eq(1L), eq(DEFAULT_USER_ID),
                    anyString(), any(), anyString(), any(LocalDateTime.class)))
                    .willReturn(stubItem(10L, 1L, "안녕하세요!"));

            ChatMessageResponse response = messageCommandService.saveMessage(1L, DEFAULT_USER_ID, "안녕하세요!");

            assertThat(response.messageId()).isEqualTo(10L);
            assertThat(response.chatRoomId()).isEqualTo(1L);
        }

        @Test
        @DisplayName("실패: 빈 텍스트면 MESSAGE_BAD_REQUEST")
        void saveMessage_blankText_throwsException() {
            assertThatThrownBy(() -> messageCommandService.saveMessage(1L, DEFAULT_USER_ID, "   "))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_BAD_REQUEST);
        }

        @Test
        @DisplayName("실패: null 텍스트면 MESSAGE_BAD_REQUEST")
        void saveMessage_nullText_throwsException() {
            assertThatThrownBy(() -> messageCommandService.saveMessage(1L, DEFAULT_USER_ID, null))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_BAD_REQUEST);
        }

        @Test
        @DisplayName("실패: 채팅방 미참여면 FORBIDDEN_CHAT_ROOM")
        void saveMessage_notJoined_throwsException() {
            given(userChatRoomRepository.findUserInfoIfMember(DEFAULT_USER_ID, 1L))
                    .willReturn(Optional.empty());

            assertThatThrownBy(() -> messageCommandService.saveMessage(1L, DEFAULT_USER_ID, "메시지"))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.FORBIDDEN_CHAT_ROOM);
        }

        @Test
        @DisplayName("성공: 2000자 초과 텍스트가 잘린다")
        void saveMessage_truncatesAt2000() {
            given(userChatRoomRepository.findUserInfoIfMember(DEFAULT_USER_ID, 1L))
                    .willReturn(Optional.of(STUB_PROJECTION));
            String longText = "a".repeat(2500);
            String truncated = "a".repeat(2000);

            given(chatMessageStoragePort.save(eq(1L), eq(DEFAULT_USER_ID),
                    anyString(), any(), eq(truncated), any(LocalDateTime.class)))
                    .willReturn(new ChatMessageItemDto(
                            12L, 1L, DEFAULT_USER_ID, "테스트유저",
                            "https://example.com/profile.jpg",
                            truncated, DEFAULT_SENT_AT, false));

            ChatMessageResponse response = messageCommandService.saveMessage(1L, DEFAULT_USER_ID, longText);

            assertThat(response.text()).hasSize(2000);
        }

        @Test
        @DisplayName("실패: 이미지 URL에 쉼표가 있으면 MESSAGE_BAD_REQUEST")
        void saveMessage_imageUrlWithComma_throwsException() {
            given(userChatRoomRepository.findUserInfoIfMember(DEFAULT_USER_ID, 1L))
                    .willReturn(Optional.of(STUB_PROJECTION));

            assertThatThrownBy(() -> messageCommandService.saveMessage(1L, DEFAULT_USER_ID, "IMAGE::https://example.com/a,b.png"))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_BAD_REQUEST);
        }

        @Test
        @DisplayName("실패: 이미지 확장자 유효하지 않으면 INVALID_IMAGE_CONTENT_TYPE")
        void saveMessage_invalidImageExtension_throwsException() {
            given(userChatRoomRepository.findUserInfoIfMember(DEFAULT_USER_ID, 1L))
                    .willReturn(Optional.of(STUB_PROJECTION));

            assertThatThrownBy(() -> messageCommandService.saveMessage(1L, DEFAULT_USER_ID, "IMAGE::https://example.com/file.gif"))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.INVALID_IMAGE_CONTENT_TYPE);
        }
    }

    // ========== 메시지 삭제 테스트 ==========

    @Nested
    @DisplayName("메시지 삭제")
    class DeleteMessage {

        @Test
        @DisplayName("성공: 메시지가 논리 삭제된다")
        void deleteMessage_success() {
            ChatMessageItemDto item = stubItem(1L, 1L, "삭제할 메시지");
            given(chatMessageStoragePort.findById(1L)).willReturn(Optional.of(item));
            given(chatMessageStoragePort.markAsDeleted(1L)).willReturn(true);

            messageCommandService.deleteMessage(1L, DEFAULT_USER_ID);

            verify(chatMessageStoragePort).markAsDeleted(1L);
        }

        @Test
        @DisplayName("실패: 메시지가 없으면 MESSAGE_NOT_FOUND")
        void deleteMessage_notFound_throwsException() {
            given(chatMessageStoragePort.findById(999L)).willReturn(Optional.empty());

            assertThatThrownBy(() -> messageCommandService.deleteMessage(999L, DEFAULT_USER_ID))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_NOT_FOUND);
        }

        @Test
        @DisplayName("실패: 이미 삭제된 메시지면 MESSAGE_CONFLICT")
        void deleteMessage_alreadyDeleted_throwsException() {
            ChatMessageItemDto deleted = new ChatMessageItemDto(1L, 1L, DEFAULT_USER_ID,
                    "테스트유저", null, "삭제된 메시지입니다.", DEFAULT_SENT_AT, true);
            given(chatMessageStoragePort.findById(1L)).willReturn(Optional.of(deleted));

            assertThatThrownBy(() -> messageCommandService.deleteMessage(1L, DEFAULT_USER_ID))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_CONFLICT);
        }

        @Test
        @DisplayName("실패: 본인 메시지가 아니면 MESSAGE_FORBIDDEN")
        void deleteMessage_notOwner_throwsException() {
            ChatMessageItemDto item = stubItem(1L, 1L, "다른 사람 메시지");
            given(chatMessageStoragePort.findById(1L)).willReturn(Optional.of(item));

            assertThatThrownBy(() -> messageCommandService.deleteMessage(1L, 999L))
                    .isInstanceOf(CustomException.class)
                    .extracting(e -> ((CustomException) e).getErrorCode())
                    .isEqualTo(ChatErrorCode.MESSAGE_FORBIDDEN);
        }
    }
}
