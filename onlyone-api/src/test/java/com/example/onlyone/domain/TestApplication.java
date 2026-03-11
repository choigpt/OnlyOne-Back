package com.example.onlyone.domain;

import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(scanBasePackages = "com.example.onlyone")
public class TestApplication {

    @Bean
    @ConditionalOnClass(name = "jakarta.persistence.EntityManager")
    public JPAQueryFactory jpaQueryFactory(ObjectProvider<EntityManager> entityManagerProvider) {
        EntityManager em = entityManagerProvider.getIfAvailable();
        return em != null ? new JPAQueryFactory(em) : null;
    }
}
