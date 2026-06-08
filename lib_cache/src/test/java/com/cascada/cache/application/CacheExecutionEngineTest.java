package com.cascada.cache.application;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.PortableFrameSerializer;
import com.cascada.cache.domain.CacheKeyFactory;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void partialHitMergesCachedBucketsWithTheSparkGap() {
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        QueryHash hash = hashGenerator.generateQueryHash(canonical, 300);

        InMemoryBlobCacheBackendAdapter backend = new InMemoryBlobCacheBackendAdapter(serializer);
        // Cache day 0 and day 1; leave day 2 to be computed by the (fake) Spark gap.
        backend.store(CacheKeyFactory.buildBucketKey(hash, 0L, DAY), appFrame("netflix", 10));
        backend.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), appFrame("netflix", 20));

        AtomicInteger sparkCallCount = new AtomicInteger();
        SparkQueryExecutorPort fakeSpark = sql -> {
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
        SparkQueryExecutorPort fakeSpark = sql -> {
            seenSql.append(sql);
            return appFrame("netflix", 99);
        };
        GapQueryRewriterPort fakeRewriter = (physicalSql, gapPlan) -> "GAP_SQL";

        CacheExecutionEngine engine = new CacheExecutionEngine(
                backend, fakeSpark, fakeRewriter, CacheExecutionConfiguration.defaults());
        ResultFrame result = engine.execute(canonical, hash);

        assertThat(seenSql.toString()).isEqualTo("FULL_SQL"); // ran the original SQL, not a gap rewrite
        assertThat(((Number) result.rows().get(0).get("bytes")).doubleValue()).isEqualTo(99.0);
    }

    @Test
    void mergeServiceDeduplicatesAnExactOverlapBucketAcrossCacheAndSpark() {
        // RC3 at the engine level: identical frames from two sources must not double-count.
        FrameMergeService merge = new FrameMergeService(300, "ts");
        CanonicalQueryObject canonical = globalAggregateOverThreeDays();
        ResultFrame fromCache = appFrame("netflix", 10);
        ResultFrame fromSpark = appFrame("netflix", 10); // exact duplicate
        ResultFrame merged = merge.mergeAndReconstruct(List.of(fromCache, fromSpark), canonical);
        assertThat(((Number) merged.rows().get(0).get("bytes")).doubleValue()).isEqualTo(10.0);
    }

    @Test
    void emptyGapPlanReportsNoGaps() {
        assertThat(new GapPlan(java.util.Optional.empty(), List.of(), java.util.Optional.empty()).hasGaps())
                .isFalse();
    }
}
