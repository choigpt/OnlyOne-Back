package com.example.onlyone.domain.chat.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

/**
 * MongoDB 채팅 메시지 저장소 설정.
 * {@code app.chat.storage=mongodb} 일 때만 활성화.
 * {@code @EnableMongoAuditing}은 MongoNotificationConfig에서 선언 (중복 방지).
 */
@Configuration
@ConditionalOnProperty(name = "app.chat.storage", havingValue = "mongodb")
public class MongoChatConfig {
}
