package com.example.onlyone.domain.schedule.entity;

import com.example.onlyone.domain.schedule.exception.ScheduleErrorCode;
import com.example.onlyone.domain.user.entity.User;
import com.example.onlyone.common.BaseTimeEntity;
import com.example.onlyone.global.exception.CustomException;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Entity
@Table(name = "user_schedule",
    uniqueConstraints = @UniqueConstraint(name = "uk_user_schedule", columnNames = {"user_id", "schedule_id"}),
    indexes = {
        @Index(name = "idx_user_schedule_schedule", columnList = "schedule_id"),
        @Index(name = "idx_user_schedule_user", columnList = "user_id")
    })
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class UserSchedule extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "user_schedule_id", updatable = false)
    private Long userScheduleId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    @NotNull
    private User user;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "schedule_id")
    @NotNull
    private Schedule schedule;

    @Column(name = "role")
    @NotNull
    @Enumerated(EnumType.STRING)
    private ScheduleRole scheduleRole;

    public void assertLeader() {
        if (this.scheduleRole != ScheduleRole.LEADER) {
            throw new CustomException(ScheduleErrorCode.MEMBER_CANNOT_MODIFY_SCHEDULE);
        }
    }

    public void assertCanLeave() {
        if (this.scheduleRole == ScheduleRole.LEADER) {
            throw new CustomException(ScheduleErrorCode.LEADER_CANNOT_LEAVE_SCHEDULE);
        }
    }
}