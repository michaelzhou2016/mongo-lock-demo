package com.example.config;

import net.javacrumbs.shedlock.core.ExtensibleLockProvider;
import net.javacrumbs.shedlock.provider.mongo.MongoLockProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class ChangeStreamConfig {

    /** 默认使用集合 shedLock */
    @Bean
    public ExtensibleLockProvider lockProvider(MongoTemplate mongoTemplate) {
//        return new MongoLockProvider(mongoTemplate.getDb());
        return MongoLockProvider.create(mongoTemplate.getDb(), true);
    }

    @Bean
    public MessageListenerContainer messageListenerContainer(MongoTemplate template) {
        return new DefaultMessageListenerContainer(template); // SmartLifecycle，自动启动
    }

    /** 单线程调度器，保证 tick 串行执行，不会并发修改状态 */
    @Bean
    public ThreadPoolTaskScheduler changeStreamScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(1);
        s.setThreadNamePrefix("cs-coordinator-");
        return s;
    }
}

