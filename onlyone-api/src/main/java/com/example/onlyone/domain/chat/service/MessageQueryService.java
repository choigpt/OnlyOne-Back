package com.example.onlyone.domain.chat.service;

import com.example.onlyone.domain.chat.dto.ChatMessageItemDto;
import com.example.onlyone.domain.chat.dto.ChatRoomMessageResponse;
import com.example.onlyone.domain.chat.port.ChatMessageStoragePort;
import com.example.onlyone.domain.chat.repository.ChatRoomRepository;
import com.example.onlyone.domain.chat.stream.ChatMessageCache;
import com.example.onlyone.domain.chat.exception.ChatErrorCode;
import com.example.onlyone.global.exception.CustomException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class MessageQueryService {

    private final ChatMessageStoragePort chatMessageStoragePort;
    private final ChatRoomRepository chatRoomRepository;
    private final ChatMessageCache chatMessageCache;

    private static final int DEFAULT_PAGE_SIZE = 30;
    private static final int MAX_PAGE_SIZE = 100;

    public ChatRoomMessageResponse getChatRoomMessages(
            Long chatRoomId, Integer size, Long cursorId, LocalDateTime cursorAt) {

        String chatRoomName = chatRoomRepository.findChatRoomName(chatRoomId)
                .orElseThrow(() -> new CustomException(ChatErrorCode.CHAT_ROOM_NOT_FOUND));

        int pageSize = clampPageSize(size);

        // 커서 없음 = 첫 페이지 → Redis 캐시 우선 조회
        if (cursorId == null || cursorAt == null) {
            List<ChatMessageItemDto> cached = chatMessageCache.getLatest(chatRoomId, pageSize + 1);
            if (cached.size() > pageSize) {
                // 캐시에 충분한 데이터 있음 → DB 조회 스킵
                List<ChatMessageItemDto> page = new ArrayList<>(cached.subList(0, pageSize));
                Collections.reverse(page);
                return ChatRoomMessageResponse.ofItems(chatRoomId, chatRoomName, page, true);
            }
        }

        // 캐시 미스 또는 커서 페이징 → DB fallback
        List<ChatMessageItemDto> slice = new ArrayList<>(fetchSlice(chatRoomId, pageSize, cursorId, cursorAt));
        boolean hasMore = slice.size() > pageSize;
        if (hasMore) slice = new ArrayList<>(slice.subList(0, pageSize));
        Collections.reverse(slice);

        return ChatRoomMessageResponse.ofItems(chatRoomId, chatRoomName, slice, hasMore);
    }

    private List<ChatMessageItemDto> fetchSlice(Long chatRoomId, int pageSize,
                                                Long cursorId, LocalDateTime cursorAt) {
        if (cursorId == null || cursorAt == null) {
            return chatMessageStoragePort.findLatest(chatRoomId, pageSize + 1);
        }
        return chatMessageStoragePort.findOlderThan(chatRoomId, cursorAt, cursorId, pageSize + 1);
    }

    private int clampPageSize(Integer size) {
        return (size == null || size <= 0) ? DEFAULT_PAGE_SIZE : Math.min(size, MAX_PAGE_SIZE);
    }
}
