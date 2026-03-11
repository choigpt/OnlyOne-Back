package com.example.onlyone.domain.feed.config;

import com.example.onlyone.domain.feed.event.FeedEngagementKafkaProducer;
import com.example.onlyone.domain.settlement.config.kafka.KafkaProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 피드 engagement Kafka Consumer/Topic 설정.
 * 정산 Kafka 설정(settlement 패키지)과 분리.
 */
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true")
public class FeedKafkaConfig {

    private final KafkaProperties kafkaProperties;

    /** Fire-and-forget producer — 트랜잭션 없음 (정산용 ledgerKafkaTemplate과 분리) */
    @Bean
    public KafkaTemplate<String, String> feedEngagementKafkaTemplate() {
        var producer = kafkaProperties.getProducer().getCommonConfig();
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producer.getBootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "feed-engagement-producer");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // transactionalIdPrefix 미설정 → non-transactional
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }

    @Bean
    public ConsumerFactory<String, String> feedEngagementConsumerFactory() {
        var common = kafkaProperties.getConsumer().getCommonConfig();
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, common.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "feed-engagement");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "feed-engagement-consumer");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 배치 최적화: 최대 500건 또는 1초 대기
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> feedEngagementKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(feedEngagementConsumerFactory());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic feedEngagementTopic() {
        return TopicBuilder.name(FeedEngagementKafkaProducer.TOPIC)
                .partitions(6)
                .replicas(1)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(1).toMillis()))
                .build();
    }
}
