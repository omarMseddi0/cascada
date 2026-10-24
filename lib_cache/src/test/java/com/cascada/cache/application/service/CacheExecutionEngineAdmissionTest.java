package com.cascada.cache.application.service;

import com.cascada.cache.application.execution.QueryAdmissionBudget;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.BucketCachePort;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.identity.domain.model.QueryHash;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import com.cascada.cache.domain.merge.AggregateFunction;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CacheExecutionEngineAdmissionTest {

    private static final long DAY = 86_400L;
    private enum ReadMode { MISS, PRESENCE_FAILURE, VISIT_FAILURE }

    @Test
    void sharedBudgetBoundsConcurrentCacheVisitsWithoutSerializingAdmittedQueries() throws Exception {
        AtomicInteger activeVisits = new AtomicInteger();
        AtomicInteger maximumVisits = new AtomicInteger();
        AtomicInteger totalVisits = new AtomicInteger();
        AtomicInteger directQueries = new AtomicInteger();
        CountDownLatch firstTwoVisits = new CountDownLatch(2);
        CountDownLatch releaseVisits = new CountDownLatch(1);
        BucketCachePort backend = new BucketCachePort() {
            @Override public List<Boolean> existsForKeys(List<String> keys) {
                return keys.stream().map(ignored -> true).toList();
            }
            @Override public List<Optional<ResultFrame>> multiGet(List<String> keys) {
                throw new AssertionError("streamed cache reads should be used");
            }
            @Override public void visitKeys(List<String> keys,
                    java.util.function.BiConsumer<Integer, Optional<ResultFrame>> visitor) {
                int active = activeVisits.incrementAndGet();
                maximumVisits.accumulateAndGet(active, Math::max);
                totalVisits.incrementAndGet();
                firstTwoVisits.countDown();
                try {
                    if (!releaseVisits.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("test did not release cache visits");
                    }
                    for (int index = 0; index < keys.size(); index++) {
                        visitor.accept(index, Optional.of(EngineTestFixtures.appFrame("netflix", 10)));
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException("test interrupted");
                } finally {
                    activeVisits.decrementAndGet();
                }
            }
            @Override public void store(String key, ResultFrame frame, Duration ttl) { }
            @Override public void evict(String key) { }
        };

        QueryAdmissionBudget budget = new QueryAdmissionBudget(2);
        ExecutorService cacheWorkers = Executors.newFixedThreadPool(3);
        ExecutorService requestWorkers = Executors.newFixedThreadPool(3);
        try {
            CacheExecutionEngine engine = new CacheExecutionEngine(backend, sql -> {
                directQueries.incrementAndGet();
                return EngineTestFixtures.appFrame("netflix", 999);
            }, (sql, gap) -> "GAP_SQL", CacheExecutionConfiguration.defaults(), null, null,
                    cacheWorkers, Clock.systemUTC(), Runnable::run, budget);
            CanonicalQueryObject query = EngineTestFixtures.globalAggregateOverThreeDays();
            QueryHash hash = new QueryHashGenerator().generateQueryHash(query, 300);

            List<Future<ResultFrame>> results = List.of(
                    requestWorkers.submit(() -> engine.execute(query, hash)),
                    requestWorkers.submit(() -> engine.execute(query, hash)),
                    requestWorkers.submit(() -> engine.execute(query, hash)));

            assertThat(firstTwoVisits.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(activeVisits.get()).isEqualTo(2);
            assertThat(maximumVisits.get()).isEqualTo(2);
            releaseVisits.countDown();
            for (Future<ResultFrame> result : results) {
                assertThat(result.get(5, TimeUnit.SECONDS).rows().get(0).get("bytes")).isEqualTo(30.0);
            }
            assertThat(totalVisits.get()).isEqualTo(3);
            assertThat(maximumVisits.get()).isEqualTo(2);
            assertThat(directQueries.get()).isZero();
        } finally {
            releaseVisits.countDown();
            requestWorkers.shutdownNow();
            cacheWorkers.shutdownNow();
        }
    }

    @Test
    void releasesAdmissionAfterCubeHitDirectFallbackAndCacheFailures() throws Exception {
        AtomicReference<ReadMode> mode = new AtomicReference<>(ReadMode.MISS);
        AtomicInteger presenceCalls = new AtomicInteger();
        BucketCachePort backend = new BucketCachePort() {
            @Override public List<Boolean> existsForKeys(List<String> keys) {
                presenceCalls.incrementAndGet();
                return switch (mode.get()) {
                    case MISS -> keys.stream().map(ignored -> false).toList();
                    case PRESENCE_FAILURE -> throw new IllegalStateException("cache unavailable");
                    case VISIT_FAILURE -> keys.stream().map(ignored -> true).toList();
                };
            }
            @Override public List<Optional<ResultFrame>> multiGet(List<String> keys) {
                throw new AssertionError("streamed cache reads should be used");
            }
            @Override public void visitKeys(List<String> keys,
                    java.util.function.BiConsumer<Integer, Optional<ResultFrame>> visitor) {
                throw new IllegalStateException("cache decode failed");
            }
            @Override public Optional<String> revision() { return Optional.of("revision-1"); }
            @Override public void store(String key, ResultFrame frame, Duration ttl) { }
            @Override public void evict(String key) { }
        };
        AtomicInteger directQueries = new AtomicInteger();
        QueryAdmissionBudget budget = new QueryAdmissionBudget(1);
        CubeShapeCatalog cube = new CubeShapeCatalog();
        ExecutorService request = Executors.newSingleThreadExecutor();
        try {
            CacheExecutionEngine engine = new CacheExecutionEngine(backend, sql -> {
                assertThat(sql).isEqualTo("FULL_SQL");
                directQueries.incrementAndGet();
                return EngineTestFixtures.appFrame("netflix", 42);
            }, (sql, gap) -> "GAP_SQL", CacheExecutionConfiguration.defaults(), null, cube,
                    Runnable::run, Clock.systemUTC(), Runnable::run, budget);
            CanonicalQueryObject firstWindow = query(0);
            CanonicalQueryObject secondWindow = query(10 * DAY);
            CanonicalQueryObject thirdWindow = query(20 * DAY);

            assertDirectResult(request.submit(() -> execute(engine, firstWindow)));
            // The full-window answer is now in the cube. This call takes the fast path under the same budget.
            assertDirectResult(request.submit(() -> execute(engine, firstWindow)));
            assertThat(presenceCalls.get()).isEqualTo(1);
            assertThat(directQueries.get()).isEqualTo(1);

            mode.set(ReadMode.PRESENCE_FAILURE);
            assertDirectResult(request.submit(() -> execute(engine, secondWindow)));
            mode.set(ReadMode.VISIT_FAILURE);
            assertDirectResult(request.submit(() -> execute(engine, thirdWindow)));

            assertThat(directQueries.get()).isEqualTo(3);
            assertThat(presenceCalls.get()).isEqualTo(3);
        } finally {
            request.shutdownNow();
        }
    }

    private static CanonicalQueryObject query(long start) {
        return new CanonicalQueryObject(HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of()),
                new TimeRange(start, start + 3 * DAY - 1), PostProcessing.none(),
                QueryMetadata.globalAggregate().withMeasureAggregates(Map.of("bytes", AggregateFunction.SUM)), "FULL_SQL", List.of("traffic"), List.of());
    }

    private static ResultFrame execute(CacheExecutionEngine engine, CanonicalQueryObject query) throws Exception {
        return engine.execute(query, new QueryHashGenerator().generateQueryHash(query, 300));
    }

    private static void assertDirectResult(Future<ResultFrame> result) throws Exception {
        assertThat(result.get(5, TimeUnit.SECONDS).rows().get(0).get("bytes")).isEqualTo(42.0);
    }
}
