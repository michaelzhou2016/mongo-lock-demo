package com.example.service;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.messaging.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChangeStreamCoordinator {
    private static final String owner = ManagementFactory.getRuntimeMXBean().getName();

    private static final Duration START_DELAY = Duration.ofMinutes(1);
    private static final Duration TICK_INTERVAL = Duration.ofSeconds(10);
    private static final Duration LOCK_AT_MOST = Duration.ofSeconds(30);

    private final LockProvider lockProvider;
    private final MessageListenerContainer container;
    private final ThreadPoolTaskScheduler scheduler;
    private final ResumeTokenStore tokenStore;

    private List<Watcher> watchers;

    /**
     * 每个监听任务一个 Watcher，只在调度线程中访问
     */
    private static class Watcher {
        final String lockName;
        final Supplier<ChangeStreamRequest<Document>> requestFactory;
        SimpleLock lock;
        Subscription subscription;

        Watcher(String lockName, Supplier<ChangeStreamRequest<Document>> f) {
            this.lockName = lockName;
            this.requestFactory = f;
        }

        boolean held() {
            return lock != null;
        }
    }

    @PostConstruct
    void init() {
        watchers = List.of(
                new Watcher("SessionTSCHN-insert",
                        () -> buildRequest("SessionTSCHN", "SessionTSCHN-insert",
                                List.of("insert"), false, this::handleSessionInsert)),
                new Watcher("TransactionCHN-update",
                        () -> buildRequest("TransactionCHN", "TransactionCHN-update",
                                List.of("update", "replace"), true, this::handleTransactionChange))
        );
    }

    /**
     * 应用完全启动后，再延迟 1 分钟开始第一次 tick
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        scheduler.scheduleWithFixedDelay(this::safeTick,
                Instant.now().plus(START_DELAY), TICK_INTERVAL);
        log.info("Change stream coordinator will start in {}", START_DELAY);
    }

    private void safeTick() {
        try {
            tick();
        } catch (Throwable t) {
            log.error("tick failed", t);
        } // 异常不能抛出，否则调度会终止
    }

    private void tick() {
        // 1. 续期已持有的锁。续期失败或订阅已死就释放
        for (Watcher w : watchers) {
            if (!w.held()) continue;
            Optional<SimpleLock> extended = w.lock.extend(LOCK_AT_MOST, Duration.ZERO);
            if (extended.isEmpty()) {
                log.warn("[{}] lock lost, stop watching", w.lockName);
                stop(w, false);
            } else {
                w.lock = extended.get();
                if (!w.subscription.isActive()) {
                    log.warn("[{}] subscription inactive, release & retry", w.lockName);
                    stop(w, true);
                }
            }
        }

        // 2. 一个实例只持有一把锁。没有持有任何锁时，按顺序尝试
        if (watchers.stream().anyMatch(Watcher::held)) return;

        for (Watcher w : watchers) {
            if (tryStart(w)) break;
        }
    }

    private boolean tryStart(Watcher w) {
        LockConfiguration cfg = new LockConfiguration(
                Instant.now(), w.lockName, LOCK_AT_MOST, Duration.ZERO);
        Optional<SimpleLock> lock = lockProvider.lock(cfg);
        if (lock.isEmpty()) return false;

        w.lock = lock.get();
        try {
            w.subscription = container.register(w.requestFactory.get(), Document.class);
            log.info("[{}] lock acquired, change stream started", w.lockName);
            return true;
        } catch (Exception e) {
            log.error("[{}] failed to start stream", w.lockName, e);
            stop(w, true);
            return false;
        }
    }

    private void stop(Watcher w, boolean unlock) {
        if (w.subscription != null) {
            container.remove(w.subscription);
            w.subscription = null;
        }
        if (unlock && w.lock != null) {
            try {
                w.lock.unlock();
            } catch (Exception e) {
                log.warn("unlock failed", e);
            }
        }
        w.lock = null;
    }

    @PreDestroy
    void shutdown() {
        // 主动释放锁，让另一个实例立刻接管，不用等锁过期
        watchers.forEach(w -> {
            if (w.held()) stop(w, true);
        });
    }

    // ---------- 构建 Change Stream 请求 ----------

    private ChangeStreamRequest<Document> buildRequest(
            String collection, String streamName, List<String> opTypes,
            boolean updateLookup,
            Consumer<Message<ChangeStreamDocument<Document>, Document>> handler) {

        MessageListener<ChangeStreamDocument<Document>, Document> listener = msg -> {
            handler.accept(msg);
            tokenStore.save(streamName, msg.getRaw().getResumeToken(), msg.getRaw().getClusterTime(), owner); // 处理后保存位置
        };

        var builder = ChangeStreamRequest.builder(listener)
                .collection(collection)
                .filter(new Document("$match",
                        new Document("operationType", new Document("$in", opTypes))));

        if (updateLookup) {
            builder.fullDocumentLookup(FullDocument.UPDATE_LOOKUP); // update 事件需要这个才带完整文档
        }
        BsonDocument token = tokenStore.load(streamName);
        if (token != null) {
            builder.resumeToken(token);
        }
        return builder.build();
    }

    // ---------- 业务处理（请实现为幂等） ----------

    private void handleSessionInsert(Message<ChangeStreamDocument<Document>, Document> msg) {
        log.info("SessionTSCHN insert: {}", msg.getBody());
    }

    private void handleTransactionChange(Message<ChangeStreamDocument<Document>, Document> msg) {
        log.info("TransactionCHN {}: {}", msg.getRaw().getOperationType().getValue(), msg.getBody());
    }
}

