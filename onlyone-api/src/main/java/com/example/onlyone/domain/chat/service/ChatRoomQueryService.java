package com.example.onlyone.domain.chat.service;

import com.example.onlyone.domain.chat.dto.ChatMessageItemDto;
import com.example.onlyone.domain.chat.dto.ChatRoomResponse;
import com.example.onlyone.domain.chat.entity.ChatRoom;
import com.example.onlyone.domain.chat.port.ChatMessageStoragePort;
import com.example.onlyone.domain.chat.repository.ChatRoomRepository;
import com.example.onlyone.domain.chat.stream.ChatRoomListCache;
import com.example.onlyone.domain.club.repository.UserClubRepository;
import com.example.onlyone.domain.user.service.UserService;
import com.example.onlyone.domain.club.exception.ClubErrorCode;
import com.example.onlyone.global.exception.CustomException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ChatRoomQueryService {

    private final ChatRoomRepository chatRoomRepository;
    private final ChatMessageStoragePort chatMessageStoragePort;
    private final UserClubRepository userClubRepository;
    private final UserService userService;
    private final ChatRoomListCache chatRoomListCache;

    public List<ChatRoomResponse> getChatRoomsUserJoinedInClub(Long clubId) {
        Long userId = userService.getCurrentUserId();

        // Redis 캐시 우선 조회 (DB 3개 쿼리 스킵)
        Optional<List<ChatRoomResponse>> cached = chatRoomListCache.get(userId, clubId);
        if (cached.isPresent()) {
            return cached.get();
        }

        if (!userClubRepository.existsByUser_UserIdAndClub_ClubId(userId, clubId)) {
            throw new CustomException(ClubErrorCode.CLUB_NOT_JOIN);
        }

        List<ChatRoom> chatRooms = chatRoomRepository.findChatRoomsByUserIdAndClubId(userId, clubId);
        Map<Long, ChatMessageItemDto> lastMessageMap = findLastMessages(chatRooms);

        List<ChatRoomResponse> result = chatRooms.stream()
                .map(room -> ChatRoomResponse.from(room, lastMessageMap.get(room.getChatRoomId())))
                .toList();

        // 캐시에 저장 (30초 TTL)
        chatRoomListCache.put(userId, clubId, result);
        return result;
    }

    private Map<Long, ChatMessageItemDto> findLastMessages(List<ChatRoom> chatRooms) {
        List<Long> chatRoomIds = chatRooms.stream()
                .map(ChatRoom::getChatRoomId)
                .toList();

        if (chatRoomIds.isEmpty()) {
            return Collections.emptyMap();
        }

        return chatMessageStoragePort.findLastMessagesByChatRoomIds(chatRoomIds).stream()
                .collect(Collectors.toMap(
                        ChatMessageItemDto::chatRoomId,
                        Function.identity()));
    }
}
