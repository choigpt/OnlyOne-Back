package com.example.onlyone.domain.notification.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "app.notification")
public class NotificationProperties {
    private int batchSize = 10;
    private int maxQueueSizePerUser = 100;
    private long batchProcessingInterval = 100;
    private int batchTimeoutSeconds = 5;
    private int sseExecutorPermits = 10;
    private int notificationExecutorPermits = 10;
    private long sseTimeoutMillis = 60000;
    private int maxConnections = 7000;
    private int cleanupIntervalMinutes = 1;
}
