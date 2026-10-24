package com.cascada.cache.application.service;

import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.BucketCachePort;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.domain.frame.ResultFrame;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class CacheExecutionEngineConcurrentQueriesTest {
    @Test
    void fiftyQueriesEnterCacheExecutionWhileSharingEightMergeWorkers() throws Exception {
        int queries = 50;
        CountDownLatch entered = new CountDownLatch(queries);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger(), maximum = new AtomicInteger();
        ResultFrame partial = EngineTestFixtures.appFrame("app", 1.0);
        BucketCachePort backend = new BucketCachePort() {
            @Override public List<Boolean> existsForKeys(List<String> keys) {
                return keys.stream().map(key -> true).toList();
            }
            @Override public List<Optional<ResultFrame>> multiGet(List<String> keys) {
                throw new AssertionError("use streaming visits");
            }
            @Override public void visitKeys(List<String> keys,
                    java.util.function.BiConsumer<Integer, Optional<ResultFrame>> visitor) {
                int current = active.incrementAndGet();
                maximum.accumulateAndGet(current, Math::max);
                entered.countDown();
                try {
                    if (!release.await(20, TimeUnit.SECONDS)) throw new AssertionError("cache visitors did not release");
                    for (int i = 0; i < keys.size(); i++) visitor.accept(i, Optional.of(partial));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                } finally { active.decrementAndGet(); }
            }
            @Override public void store(String key, ResultFrame frame, Duration ttl) { }
            @Override public void evict(String key) { }
        };
        ExecutorService requests = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService io = Executors.newVirtualThreadPerTaskExecutor();
        ThreadPoolExecutor merge = new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), Thread.ofPlatform().name("shared-merge-", 0).factory());
        merge.prestartAllCoreThreads();
        try {
            CacheExecutionEngine engine = new CacheExecutionEngine(backend,
                    sql -> { throw new AssertionError("all buckets must come from cache"); },
                    (sql, gap) -> { throw new AssertionError("no gaps expected"); },
                    new CacheExecutionConfiguration(86400, 300, "ts", 8), null, null, io, Clock.systemUTC(), merge);
            var query = EngineTestFixtures.globalAggregateOverThreeDays();
            var hash = new QueryHashGenerator().generateQueryHash(query, 300);
            List<Future<ResultFrame>> results = new ArrayList<>();
            for (int i = 0; i < queries; i++) results.add(requests.submit(() -> engine.execute(query, hash)));
            assertThat(entered.await(10, TimeUnit.SECONDS)).as("all 50 queries enter before any is released").isTrue();
            assertThat(maximum.get()).isEqualTo(queries);
            release.countDown();
            for (Future<ResultFrame> future : results) {
                ResultFrame answer = future.get(20, TimeUnit.SECONDS);
                assertThat(answer.rowCount()).isEqualTo(1);
                assertThat(answer.doubleAt(0, answer.columnIndex("bytes"))).isEqualTo(3.0);
            }
            assertThat(merge.getLargestPoolSize()).isEqualTo(8);
            assertThat(active.get()).isZero();
        } finally {
            release.countDown();
            requests.shutdownNow();
            io.shutdownNow();
            merge.shutdownNow();
        }
    }
}
