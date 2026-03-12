package com.example.onlyone.domain.feed.entity;

import com.example.onlyone.domain.feed.exception.FeedErrorCode;
import com.example.onlyone.domain.user.entity.User;
import com.example.onlyone.common.BaseTimeEntity;
import com.example.onlyone.global.exception.CustomException;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "feed_comment")
@Getter
@NoArgsConstructor
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FeedComment extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "feed_comment_id", updatable = false)
    private Long feedCommentId;

    @Column(name = "content")
    private String content;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "feed_id")
    @NotNull
    private Feed feed;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", updatable = false)
    @NotNull
    private User user;

    public void assertBelongsToFeed(Long feedId) {
        if (!this.feed.getFeedId().equals(feedId)) {
            throw new CustomException(FeedErrorCode.FEED_NOT_FOUND);
        }
    }

    public void assertDeletableBy(Long userId, Long feedOwnerId) {
        if (!userId.equals(this.user.getUserId()) && !userId.equals(feedOwnerId)) {
            throw new CustomException(FeedErrorCode.UNAUTHORIZED_COMMENT_ACCESS);
        }
    }
}