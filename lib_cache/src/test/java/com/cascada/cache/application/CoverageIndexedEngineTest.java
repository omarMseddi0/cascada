package com.cascada.cache.application;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.index.InMemoryCoverageIndexAdapter;
import com.cascada.cache.adapter.serialization.PortableFrameSerializer;
import com.cascada.cache.domain.CacheKeyFactory;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The coverage-bitmap fast path (plan Appendix J.1) at engine level: presence comes from ONE bitmap
 * lookup (zero EXISTS round trips), and a stale present-bit degrades to a surgical single-bucket
 * re-fetch — never a wrong answer, never a full-window recompute.
 */
class CoverageIndexedEngineTest {

    private static final long DAY = 86_400L;

    private final PortableFrameSerializer serializer = new PortableFrameSerializer();
    private final QueryHashGenerator hashGenerator = new QueryHashGenerator();

    private CanonicalQueryObject globalAggregateOverThreeDays() {
        HashComponents components = HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of());
        return new CanonicalQueryObject(components, new TimeRange(0, 3 * DAY - 1),
                PostProcessing.none(), QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"), List.of());
    }

    private ResultFrame appFrame(String app, double bytes) {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("bytes", ColumnType.DOUBLE)
                .row(Map.of("appName", app, "bytes", bytes))
                .build();
    }

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
        public void store(String key, ResultFrame frame) {
            delegate.store(key, frame);
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

        InMemoryBlobCacheBackendAdapter store = new InMemoryBlobCacheBackendAdapter(serializer);
        store.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10));
        store.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20));
        CountingBackend backend = new CountingBackend(store);

        InMemoryCoverageIndexAdapter coverage = new InMemoryCoverageIndexAdapter();
        coverage.markCached(hash, DAY, 0L);
        coverage.markCached(hash, DAY, DAY);

        List<String> gapSqls = new CopyOnWriteArrayList<>();
        SparkQueryExecutorPort fakeSpark = sql -> {
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
    void stalePresentBitIsRepairedBySurgicalSingleBucketRefetchNotFullRecompute() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);

        InMemoryBlobCacheBackendAdapter store = new InMemoryBlobCacheBackendAdapter(serializer);
        // Days 0 and 1 really exist; day 2's coverage bit is STALE (bucket evicted).
        store.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10));
        store.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20));

        InMemoryCoverageIndexAdapter coverage = new InMemoryCoverageIndexAdapter();
        coverage.markCached(hash, DAY, 0L);
        coverage.markCached(hash, DAY, DAY);
        coverage.markCached(hash, DAY, 2 * DAY); // lies: the backend has no day-2 bucket

        List<String> executedSqls = new CopyOnWriteArrayList<>();
        SparkQueryExecutorPort fakeSpark = sql -> {
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
}
