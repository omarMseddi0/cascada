package com.cascada.cache.application.service;

import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.query.PostProcessing;
import com.cascada.cache.domain.query.QueryMetadata;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.time.TimeBucketCalculator;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end exercise of the runnable engine with an in-memory backend and a fake Spark executor:
 * partial-hit merge, and the two direct-to-Spark fast paths.
 */
class CacheExecutionEngineTest {

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

    private CanonicalQueryObject timeSeriesOverThreeDays() {
        HashComponents components = HashComponents.of(List.of("ts", "appName"), List.of("SUM(bytes)"), List.of());
        QueryMetadata metadata = QueryMetadata.timeSeries(300)
                .withMeasureAggregates(Map.of("bytes", AggregateFunction.SUM));
        return new CanonicalQueryObject(components, new TimeRange(0, 3 * DAY - 1),
                PostProcessing.none(), metadata, "FULL_SQL", List.of("traffic"), List.of());
    }

    private ResultFrame timeSeriesFrame(double bytes) {
        return ResultFrame.builder().column("ts", ColumnType.LONG).column("appName", ColumnType.STRING)
                .column("bytes", ColumnType.DOUBLE).row(0L, "netflix", bytes).build();
    }

    private ResultFrame emptyTimeSeriesFrame() {
        return ResultFrame.builder().column("ts", ColumnType.LONG).column("appName", ColumnType.STRING)
                .column("bytes", ColumnType.DOUBLE).build();
    }

    private CacheBackendPort failingBackend(InMemoryBlobCacheBackendAdapter delegate,
                                            boolean failPresence, boolean failMultiGet, boolean failStore) {
        return new CacheBackendPort() {
            @Override public List<Boolean> existsForKeys(List<String> keys) {
                if (failPresence) throw new IllegalStateException("cache unavailable");
                return delegate.existsForKeys(keys);
            }
            @Override public List<Optional<ResultFrame>> multiGet(List<String> keys) {
                if (failMultiGet) throw new IllegalStateException("corrupt cache value");
                return delegate.multiGet(keys);
            }
            @Override public void store(String key, ResultFrame frame) {
                if (failStore) throw new IllegalStateException("cache write failed");
                delegate.store(key, frame);
            }
            @Override public CacheSizeReport sizeReport() { return delegate.sizeReport(); }
            @Override public long flush(com.cascada.cache.domain.admin.CacheScope scope) {
                return delegate.flush(scope);
            }
        };
    }

    @Test
    void partialHitMergesCachedBucketsWithTheSparkGap() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);

        InMemoryBlobCacheBackendAdapter backend = new InMemoryBlobCacheBackendAdapter(serializer);
        // Cache day 0 and day 1; leave day 2 to be computed by the (fake) Spark gap.
        backend.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10));
        backend.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20));

        AtomicInteger sparkCallCount = new AtomicInteger();
        QueryExecutorPort fakeSpark = sql -> {
            sparkCallCount.incrementAndGet();
            return appFrame("netflix", 30); // the day-2 gap result
        };
        GapQueryRewriterPort fakeRewriter = (physicalSql, gapPlan) -> "GAP_SQL";

        CacheExecutionEngine engine = new CacheExecutionEngine(
                backend, fakeSpark, fakeRewriter, CacheExecutionConfiguration.defaults());

        ResultFrame result = engine.execute(canonical, hash);

        assertThat(sparkCallCount.get()).isEqualTo(1); // only the gap, not the whole range
        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.rows().get(0).get("appName")).isEqualTo("netflix");
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(60.0);
    }

    @Test
    void zeroCacheHitsBypassToDirectSparkWithTheOriginalSql() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        InMemoryBlobCacheBackendAdapter backend = new InMemoryBlobCacheBackendAdapter(serializer);

        StringBuilder seenSql = new StringBuilder();
        QueryExecutorPort fakeSpark = sql -> {
            seenSql.append(sql);
            return appFrame("netflix", 99);
        };
        GapQueryRewriterPort fakeRewriter = (physicalSql, gapPlan) -> "GAP_SQL";

        CacheExecutionEngine engine = new CacheExecutionEngine(
                backend, fakeSpark, fakeRewriter, CacheExecutionConfiguration.defaults());
        ResultFrame result = engine.execute(canonical, hash);

        assertThat(seenSql.toString()).isEqualTo("FULL_SQL");
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(99.0);
    }

    @Test
    void coldTimeSeriesQueryFillsCompleteBucketsForTheNextRequest() {
        HashComponents components = HashComponents.of(List.of("ts", "appName"), List.of("SUM(bytes)"), List.of());
        CanonicalQueryObject canonical = new CanonicalQueryObject(components, new TimeRange(0, 3 * DAY - 1),
                PostProcessing.none(), QueryMetadata.timeSeries(300), "FULL_SQL", List.of("traffic"), List.of());
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        InMemoryBlobCacheBackendAdapter backend = new InMemoryBlobCacheBackendAdapter(serializer);
        AtomicInteger calls = new AtomicInteger();
        QueryExecutorPort fakeSpark = sql -> {
            calls.incrementAndGet();
            return ResultFrame.builder().column("ts", ColumnType.LONG).column("appName", ColumnType.STRING)
                    .column("bytes", ColumnType.DOUBLE).row(0L, "netflix", 99.0).build();
        };
        CacheExecutionEngine engine = new CacheExecutionEngine(backend, fakeSpark,
                (physicalSql, gapPlan) -> "GAP_SQL", CacheExecutionConfiguration.defaults());

        engine.execute(canonical, hash);
        assertThat(calls.get()).isEqualTo(3);
        assertThat(backend.storedBucketCount()).isEqualTo(3);
        engine.execute(canonical, hash);
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void mergeServicePreservesEqualContributionsFromDisjointBuckets() {
        // Equal values from disjoint buckets are independent contributions, not duplicates.
        FrameMergeService merge = new FrameMergeService(300, "ts");
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        ResultFrame fromCache = appFrame("netflix", 10);
        ResultFrame fromSpark = appFrame("netflix", 10); // equal contribution from a different bucket
        ResultFrame merged = merge.mergeAndReconstruct(List.of(fromCache, fromSpark), canonical);
        assertThat(((Number) merged.rows().get(0).get("bytes")).doubleValue()).isEqualTo(20.0);
    }

    @Test
    void emptyGapPlanReportsNoGaps() {
        assertThat(new GapPlan(java.util.Optional.empty(), List.of(), java.util.Optional.empty()).hasGaps())
                .isFalse();
    }

    @Test
    void cachePresenceOutageFallsBackToTheOriginalAuthoritativeQuery() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        InMemoryBlobCacheBackendAdapter delegate = new InMemoryBlobCacheBackendAdapter(serializer);
        CacheBackendPort unavailable = failingBackend(delegate, true, false, false);
        AtomicInteger calls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            assertThat(sql).isEqualTo("FULL_SQL");
            calls.incrementAndGet();
            return appFrame("netflix", 99);
        };

        ResultFrame result = new CacheExecutionEngine(unavailable, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(99.0);
    }

    @Test
    void cacheDecodeFailureFallsBackToTheOriginalAuthoritativeQuery() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        InMemoryBlobCacheBackendAdapter delegate = new InMemoryBlobCacheBackendAdapter(serializer);
        for (long day : List.of(0L, DAY, 2 * DAY)) {
            delegate.store(CacheKeyFactory.buildBucketKey(hash, day, DAY), appFrame("netflix", 1));
        }
        CacheBackendPort corrupt = failingBackend(delegate, false, true, false);
        QueryExecutorPort spark = sql -> {
            assertThat(sql).isEqualTo("FULL_SQL");
            return appFrame("netflix", 77);
        };

        ResultFrame result = new CacheExecutionEngine(corrupt, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(77.0);
    }

    @Test
    void failedCacheWritesDoNotDiscardSuccessfullyComputedBuckets() {
        CanonicalQueryObject canonical = timeSeriesOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        CacheBackendPort writeFailing = failingBackend(
                new InMemoryBlobCacheBackendAdapter(serializer), false, false, true);
        AtomicInteger sparkCalls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            sparkCalls.incrementAndGet();
            if (sql.equals("FULL_SQL")) return timeSeriesFrame(123.0);
            return timeSeriesFrame(10.0);
        };

        ResultFrame result = new CacheExecutionEngine(writeFailing, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(sparkCalls.get()).isEqualTo(3);
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(30.0);
    }

    @Test
    void coldQueryKeepsSchemaWhenEverySparkBucketIsEmpty() {
        CanonicalQueryObject canonical = timeSeriesOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        AtomicInteger calls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            calls.incrementAndGet();
            return emptyTimeSeriesFrame();
        };

        ResultFrame result = new CacheExecutionEngine(new InMemoryBlobCacheBackendAdapter(serializer), spark,
                (sql, plan) -> "GAP_SQL", CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(calls.get()).isEqualTo(3);
        assertThat(result.rowCount()).isZero();
        assertThat(result.columnNames()).containsExactly("ts", "appName", "bytes");
    }

    @Test
    void incompatibleColdBucketFramesFallBackToTheOriginalQuery() {
        CanonicalQueryObject canonical = timeSeriesOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        AtomicInteger calls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            if (sql.equals("FULL_SQL")) {
                calls.incrementAndGet();
                return timeSeriesFrame(123.0);
            }
            int bucketCall = calls.incrementAndGet();
            return bucketCall == 2 ? incompatibleTimeSeriesFrame() : timeSeriesFrame(10.0);
        };

        ResultFrame result = new CacheExecutionEngine(new InMemoryBlobCacheBackendAdapter(serializer), spark,
                (sql, plan) -> "GAP_SQL", CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(calls.get()).isEqualTo(4);
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(123.0);
    }

    @Test
    void incompatibleCachedAndGapFramesFallBackToTheOriginalQuery() {
        CanonicalQueryObject canonical = timeSeriesOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        InMemoryBlobCacheBackendAdapter backend = new InMemoryBlobCacheBackendAdapter(serializer);
        backend.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), timeSeriesFrame(10.0));
        backend.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), timeSeriesFrame(20.0));
        AtomicInteger calls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            calls.incrementAndGet();
            if (sql.equals("GAP_SQL")) {
                return incompatibleTimeSeriesFrame();
            }
            assertThat(sql).isEqualTo("FULL_SQL");
            return timeSeriesFrame(123.0);
        };

        ResultFrame result = new CacheExecutionEngine(backend, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(calls.get()).isEqualTo(2);
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(123.0);
    }

    @Test
    void authoritativeExecutorFailureIsPropagatedWithoutRetry() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        CacheBackendPort unavailable = failingBackend(
                new InMemoryBlobCacheBackendAdapter(serializer), true, false, false);
        IllegalStateException authoritativeFailure = new IllegalStateException("authoritative query failed");
        AtomicInteger calls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            assertThat(sql).isEqualTo("FULL_SQL");
            calls.incrementAndGet();
            throw authoritativeFailure;
        };

        assertThatThrownBy(() -> new CacheExecutionEngine(unavailable, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical, hash))
                .isSameAs(authoritativeFailure);
        assertThat(calls.get()).isEqualTo(1);
    }

    private ResultFrame incompatibleTimeSeriesFrame() {
        return ResultFrame.builder().column("ts", ColumnType.LONG).column("different", ColumnType.STRING)
                .column("bytes", ColumnType.DOUBLE).row(0L, "unexpected", 10.0).build();
    }

    @Test
    void cacheReadResponseLengthMismatchFallsBackToOriginalSql() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        CacheBackendPort malformedPresence = new CacheBackendPort() {
            @Override public List<Boolean> existsForKeys(List<String> keys) {
                return keys.stream().map(key -> true).toList();
            }
            @Override public List<Optional<ResultFrame>> multiGet(List<String> keys) {
                return List.of(Optional.empty());
            }
            @Override public void store(String key, ResultFrame frame) { }
            @Override public CacheSizeReport sizeReport() { return CacheSizeReport.empty(); }
            @Override public long flush(com.cascada.cache.domain.admin.CacheScope scope) { return 0; }
        };
        AtomicInteger fullCalls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            assertThat(sql).isEqualTo("FULL_SQL");
            fullCalls.incrementAndGet();
            return appFrame("netflix", 12);
        };

        ResultFrame result = new CacheExecutionEngine(malformedPresence, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(fullCalls.get()).isEqualTo(1);
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(12.0);
    }

    @Test
    void malformedPresenceLengthFallsBackToOriginalSql() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);
        CacheBackendPort malformedPresence = new CacheBackendPort() {
            @Override public List<Boolean> existsForKeys(List<String> keys) { return List.of(true); }
            @Override public List<Optional<ResultFrame>> multiGet(List<String> keys) {
                throw new AssertionError("MGET must not run with a malformed presence mask");
            }
            @Override public void store(String key, ResultFrame frame) { }
            @Override public CacheSizeReport sizeReport() { return CacheSizeReport.empty(); }
            @Override public long flush(com.cascada.cache.domain.admin.CacheScope scope) { return 0; }
        };
        AtomicInteger fullCalls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            assertThat(sql).isEqualTo("FULL_SQL");
            fullCalls.incrementAndGet();
            return appFrame("netflix", 13);
        };

        ResultFrame result = new CacheExecutionEngine(malformedPresence, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical, hash);

        assertThat(fullCalls.get()).isEqualTo(1);
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(13.0);
    }

    @Test
    void excessivelyLargeWindowBypassesBeforeMaterializingBucketKeys() {
        long end = (long) (TimeBucketCalculator.MAX_BUCKETS_PER_PLAN + 1) * DAY - 1;
        CanonicalQueryObject canonical = new CanonicalQueryObject(
                HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of()),
                new TimeRange(0, end), PostProcessing.none(), QueryMetadata.globalAggregate(),
                "FULL_SQL", List.of("traffic"), List.of());
        CacheBackendPort unusedCache = new CacheBackendPort() {
            @Override public List<Boolean> existsForKeys(List<String> keys) {
                throw new AssertionError("oversized query must skip cache enumeration");
            }
            @Override public List<Optional<ResultFrame>> multiGet(List<String> keys) { throw new AssertionError(); }
            @Override public void store(String key, ResultFrame frame) { throw new AssertionError(); }
            @Override public CacheSizeReport sizeReport() { return CacheSizeReport.empty(); }
            @Override public long flush(com.cascada.cache.domain.admin.CacheScope scope) { return 0; }
        };
        QueryExecutorPort spark = sql -> {
            assertThat(sql).isEqualTo("FULL_SQL");
            return appFrame("netflix", 14);
        };

        ResultFrame result = new CacheExecutionEngine(unusedCache, spark, (sql, plan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults()).execute(canonical,
                hashGenerator.generateQueryHash(canonical, 300));

        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(14.0);
    }
}
