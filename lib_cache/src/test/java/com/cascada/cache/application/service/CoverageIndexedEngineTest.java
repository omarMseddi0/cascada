package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.adapter.out.index.InMemoryCoverageIndexAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.model.admin.CacheSizeReport;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.fixture.ValkeyTestDatabase;
import com.cascada.identity.domain.model.QueryHash;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

import static com.cascada.cache.application.service.EngineTestFixtures.appFrame;
import static com.cascada.cache.application.service.EngineTestFixtures.globalAggregateOverThreeDays;

/**
 * The coverage-bitmap fast path (plan Appendix J.1) at engine level: presence comes from ONE bitmap
 * lookup (zero EXISTS round trips), and a stale present-bit degrades to a surgical single-bucket
 * re-fetch — never a wrong answer, never a full-window recompute.
 */
@EnabledIfEnvironmentVariable(named = ValkeyTestDatabase.ENVIRONMENT_VARIABLE, matches = ".+")
class CoverageIndexedEngineTest {

    @RegisterExtension
    final ValkeyTestDatabase valkey = new ValkeyTestDatabase();

    private static final long DAY = 86_400L;

    private final PortableFrameSerializer serializer = new PortableFrameSerializer();
    private final QueryHashGenerator hashGenerator = new QueryHashGenerator();

    /** Counts EXISTS round trips so the test can prove the bitmap replaced them. */
    private static final class CountingBackend implements CacheBackendPort {
        private final CacheBackendPort delegate;
        final AtomicInteger existsCalls = new AtomicInteger();

        CountingBackend(CacheBackendPort delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<Boolean> existsForKeys(List<String> keys) {
            existsCalls.incrementAndGet();
            return delegate.existsForKeys(keys);
        }

        @Override
        public List<Optional<ResultFrame>> multiGet(List<String> keys) {
            return delegate.multiGet(keys);
        }

        @Override
        public void store(String key, ResultFrame frame, Duration timeToLive) {
            delegate.store(key, frame, timeToLive);
        }

        @Override
        public void evict(String key) {
            delegate.evict(key);
        }

        @Override
        public Optional<com.cascada.cache.domain.model.validation.ValidationBaseline> loadValidationBaseline() {
            return delegate.loadValidationBaseline();
        }

        @Override
        public void saveValidationBaseline(com.cascada.cache.domain.model.validation.ValidationBaseline baseline,
                                           Duration timeToLive) {
            delegate.saveValidationBaseline(baseline, timeToLive);
        }

        @Override
        public CacheSizeReport sizeReport() {
            return delegate.sizeReport();
        }

        @Override
        public long flush(CacheScope scope) {
            return delegate.flush(scope);
        }
    }

    @Test
    void bitmapAnswersPresenceWithZeroExistsRoundTrips() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);

        ValkeyCacheBackendAdapter store = valkey.cache(serializer);
        store.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10), Duration.ofDays(1));
        store.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20), Duration.ofDays(1));
        CountingBackend backend = new CountingBackend(store);

        InMemoryCoverageIndexAdapter coverage = new InMemoryCoverageIndexAdapter();
        coverage.markCached(hash, DAY, 0L);
        coverage.markCached(hash, DAY, DAY);

        List<String> gapSqls = new CopyOnWriteArrayList<>();
        QueryExecutorPort fakeSpark = sql -> {
            gapSqls.add(sql);
            return appFrame("netflix", 30); // the day-2 gap
        };
        GapQueryRewriterPort fakeRewriter = (physicalSql, gapPlan) -> "GAP_SQL";

        CacheExecutionEngine engine = new CacheExecutionEngine(
                backend, fakeSpark, fakeRewriter, CacheExecutionConfiguration.defaults(), coverage);
        ResultFrame result = engine.execute(canonical, hash);

        assertThat(backend.existsCalls.get()).isZero(); // the bitmap replaced the EXISTS phase
        assertThat(gapSqls).containsExactly("GAP_SQL"); // only the day-2 gap hit Spark
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(60.0);
    }

    @Test
    void completeReadUsesAuthoritativePresenceWhenAnotherProcessWarmedBuckets() {
        CanonicalQueryObject query = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(query, 300);
        var store = valkey.cache(serializer);
        store.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10), Duration.ofDays(1));
        store.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20), Duration.ofDays(1));
        store.store(CacheKeyFactory.buildBucketKey(hash, 2 * DAY, DAY), appFrame("netflix", 30), Duration.ofDays(1));
        var coverage = new InMemoryCoverageIndexAdapter();
        coverage.markCached(hash, DAY, 0L);
        var backend = new CountingBackend(store);
        var engine = new CacheExecutionEngine(backend, sql -> { throw new AssertionError(sql); },
                (sql, gap) -> { throw new AssertionError(sql); }, CacheExecutionConfiguration.defaults(), coverage);

        assertThat(engine.tryReadComplete(query, hash)).hasValueSatisfying(frame ->
                assertThat(((Number) frame.rows().getFirst().get("bytes")).doubleValue()).isEqualTo(60));
        assertThat(backend.existsCalls.get()).isEqualTo(1);
    }

    @Test
    void completeReadDoesNotReturnPartialDataWhenBitmapContainsAnEvictedBucket() {
        CanonicalQueryObject query = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(query, 300);
        var store = valkey.cache(serializer);
        store.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10), Duration.ofDays(1));
        store.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20), Duration.ofDays(1));
        var coverage = new InMemoryCoverageIndexAdapter();
        coverage.markCached(hash, DAY, 0L);
        coverage.markCached(hash, DAY, DAY);
        coverage.markCached(hash, DAY, 2 * DAY);
        var backend = new CountingBackend(store);
        var engine = new CacheExecutionEngine(backend, sql -> { throw new AssertionError(sql); },
                (sql, gap) -> { throw new AssertionError(sql); }, CacheExecutionConfiguration.defaults(), coverage);

        assertThat(engine.tryReadComplete(query, hash)).isEmpty();
        assertThat(backend.existsCalls.get()).isZero();
    }

    @Test
    void stalePresentBitIsRepairedBySurgicalSingleBucketRefetchNotFullRecompute() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);

        ValkeyCacheBackendAdapter store = valkey.cache(serializer);
        // Days 0 and 1 really exist; day 2's coverage bit is STALE (bucket evicted).
        store.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10), Duration.ofDays(1));
        store.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20), Duration.ofDays(1));

        InMemoryCoverageIndexAdapter coverage = new InMemoryCoverageIndexAdapter();
        coverage.markCached(hash, DAY, 0L);
        coverage.markCached(hash, DAY, DAY);
        coverage.markCached(hash, DAY, 2 * DAY); // lies: the backend has no day-2 bucket

        List<String> executedSqls = new CopyOnWriteArrayList<>();
        QueryExecutorPort fakeSpark = sql -> {
            executedSqls.add(sql);
            return appFrame("netflix", 30); // day-2 recovery
        };
        GapQueryRewriterPort fakeRewriter = (physicalSql, gapPlan) -> "GAP_SQL";

        CacheExecutionEngine engine = new CacheExecutionEngine(
                new CountingBackend(store), fakeSpark, fakeRewriter,
                CacheExecutionConfiguration.defaults(), coverage);
        ResultFrame result = engine.execute(canonical, hash);

        // Exactly one surgical gap fetch for the vanished bucket; the original FULL_SQL never ran.
        assertThat(executedSqls).containsExactly("GAP_SQL");
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(60.0);
    }

    @Test
    void vanishedBucketRecoveryCanStartWhileTheOriginalGapIsRunning() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        var store = valkey.cache(serializer);
        store.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10), Duration.ofDays(1));
        var coverage = new InMemoryCoverageIndexAdapter();
        coverage.markCached(hash, DAY, 0L);
        coverage.markCached(hash, DAY, 2 * DAY);

        var originalGapStarted = new CountDownLatch(1);
        var recoveryStarted = new CountDownLatch(1);
        QueryExecutorPort spark = sql -> {
            switch (sql) {
                case "original gap" -> {
                    originalGapStarted.countDown();
                    await(recoveryStarted);
                    return appFrame("netflix", 20);
                }
                case "recovery gap" -> {
                    await(originalGapStarted);
                    recoveryStarted.countDown();
                    return appFrame("netflix", 30);
                }
                default -> throw new AssertionError("Unexpected SQL: " + sql);
            }
        };
        GapQueryRewriterPort rewriter = (sql, gap) -> {
            if (gap.body().equals(List.of(DAY))) {
                return "original gap";
            }
            assertThat(gap.body()).containsExactly(2 * DAY);
            return "recovery gap";
        };

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CacheExecutionEngine engine = new CacheExecutionEngine(store, spark, rewriter,
                    CacheExecutionConfiguration.defaults(), coverage, null, executor);
            ResultFrame result = engine.execute(canonical, hash);
            assertThat((Number) result.rows().getFirst().get("bytes")).isEqualTo(60.0);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("both gap queries can overlap").isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for a gap query", interrupted);
        }
    }
}
