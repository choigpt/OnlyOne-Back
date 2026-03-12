package com.example.onlyone.domain.club.entity;

import com.example.onlyone.domain.club.exception.ClubErrorCode;
import com.example.onlyone.domain.user.entity.User;
import com.example.onlyone.common.BaseTimeEntity;
import com.example.onlyone.global.exception.CustomException;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Entity
@Table(name = "user_club",
    uniqueConstraints = @UniqueConstraint(name = "uk_user_club", columnNames = {"user_id", "club_id"}),
    indexes = {
        @Index(name = "idx_user_club_club_user", columnList = "club_id, user_id"),
        @Index(name = "idx_user_club_user", columnList = "user_id")
    })
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class UserClub extends BaseTimeEntity  {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "user_club_id", updatable = false)
    private Long userClubId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    @NotNull
    private User user;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "club_id")
    @NotNull
    private Club club;

    @Column(name = "role")
    @NotNull
    @Enumerated(EnumType.STRING)
    private ClubRole clubRole;

    public void assertLeader() {
        if (this.clubRole != ClubRole.LEADER) {
            throw new CustomException(ClubErrorCode.LEADER_ONLY_CLUB_MODIFY);
        }
    }

    public void assertCanLeave() {
        if (this.clubRole == ClubRole.GUEST) {
            throw new CustomException(ClubErrorCode.CLUB_NOT_LEAVE);
        }
        if (this.clubRole == ClubRole.LEADER) {
            throw new CustomException(ClubErrorCode.CLUB_LEADER_NOT_LEAVE);
        }
    }
}