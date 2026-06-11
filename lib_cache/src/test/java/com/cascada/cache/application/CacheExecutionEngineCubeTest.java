package com.cascada.cache.application;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.PortableFrameSerializer;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.cube.CubeShapeCatalog;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cube path wired into the engine (plan §8.12): a full-window answer computed at a fine grain is
 * catalogued by shape, and a later <em>coarser</em> query over the exact same window is answered by
 * verified roll-up — zero Spark calls. Any mismatch (different window, time series) falls back to the
 * reference behaviour untouched.
 */
class CacheExecutionEngineCubeTest {

    private static final long DAY = 86_400L;

    private final PortableFrameSerializer serializer = new PortableFrameSerializer();
    private final QueryHashGenerator hashGenerator = new QueryHashGenerator();
    private final GapQueryRewriterPort fakeRewriter = (physicalSql, gapPlan) -> "GAP_SQL";

    private CanonicalQueryObject globalAggregate(List<String> groupBy, TimeRange timeRange) {
        HashComponents components = HashComponents.of(groupBy, List.of("SUM(bytes)"), List.of());
        return new CanonicalQueryObject(components, timeRange, PostProcessing.none(),
                QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"), List.of());
    }

    private ResultFrame fineGrainedFrame() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "deviceType", "mobile", "SUM(bytes)", 10.0))
                .row(Map.of("appName", "netflix", "deviceType", "tablet", "SUM(bytes)", 5.0))
                .row(Map.of("appName", "youtube", "deviceType", "mobile", "SUM(bytes)", 7.0))
                .build();
    }

    private CacheExecutionEngine engineWith(SparkQueryExecutorPort spark, CubeShapeCatalog catalog) {
        return new CacheExecutionEngine(new InMemoryBlobCacheBackendAdapter(serializer), spark,
                fakeRewriter, CacheExecutionConfiguration.defaults(), null, catalog);
    }

    @Test
    void aCoarserQueryOverTheSameWindowIsServedByRollUpWithoutSpark() {
        TimeRange window = new TimeRange(0, 3 * DAY - 1);
        AtomicInteger sparkCalls = new AtomicInteger();
        SparkQueryExecutorPort fakeSpark = sql -> {
            sparkCalls.incrementAndGet();
            return fineGrainedFrame();
        };
        CacheExecutionEngine engine = engineWith(fakeSpark, new CubeShapeCatalog());

        // 1. fine grain: full miss -> Spark computes, the answer is catalogued by shape
        CanonicalQueryObject fine = globalAggregate(List.of("appName", "deviceType"), window);
        engine.execute(fine, hashGenerator.generateQueryHash(fine, 300));
        assertThat(sparkCalls.get()).isEqualTo(1);

        // 2. coarser grain, same window: answered by verified roll-up, Spark untouched
        CanonicalQueryObject coarse = globalAggregate(List.of("appName"), window);
        ResultFrame answer = engine.execute(coarse, hashGenerator.generateQueryHash(coarse, 300));

        assertThat(sparkCalls.get()).isEqualTo(1);
        Map<String, Double> byApp = new java.util.HashMap<>();
        for (Map<String, Object> row : answer.rows()) {
            byApp.put((String) row.get("appName"), ((Number) row.get("SUM(bytes)")).doubleValue());
        }
        assertThat(byApp.get("netflix")).isEqualTo(15.0); // 10 + 5 rolled up across deviceType
        assertThat(byApp.get("youtube")).isEqualTo(7.0);
    }

    @Test
    void aDifferentTimeWindowNeverSeesTheCataloguedShape() {
        AtomicInteger sparkCalls = new AtomicInteger();
        SparkQueryExecutorPort fakeSpark = sql -> {
            sparkCalls.incrementAndGet();
            return fineGrainedFrame();
        };
        CacheExecutionEngine engine = engineWith(fakeSpark, new CubeShapeCatalog());

        CanonicalQueryObject fine = globalAggregate(List.of("appName", "deviceType"), new TimeRange(0, 3 * DAY - 1));
        engine.execute(fine, hashGenerator.generateQueryHash(fine, 300));

        // coarser shape but a SHORTER window: serving the 3-day roll-up would be wrong data
        CanonicalQueryObject differentWindow = globalAggregate(List.of("appName"), new TimeRange(0, 2 * DAY - 1));
        engine.execute(differentWindow, hashGenerator.generateQueryHash(differentWindow, 300));

        assertThat(sparkCalls.get()).isEqualTo(2);
    }

    @Test
    void queriesWithADeferredLimitAreNeverCataloguedOrServedFromTheCube() {
        TimeRange window = new TimeRange(0, 3 * DAY - 1);
        AtomicInteger sparkCalls = new AtomicInteger();
        SparkQueryExecutorPort fakeSpark = sql -> {
            sparkCalls.incrementAndGet();
            return fineGrainedFrame();
        };
        CacheExecutionEngine engine = engineWith(fakeSpark, new CubeShapeCatalog());

        HashComponents components =
                HashComponents.of(List.of("appName", "deviceType"), List.of("SUM(bytes)"), List.of());
        PostProcessing limited = new PostProcessing(java.util.Optional.of(1), List.of());
        CanonicalQueryObject withLimit = new CanonicalQueryObject(components, window, limited,
                QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"), List.of());

        engine.execute(withLimit, hashGenerator.generateQueryHash(withLimit, 300));
        engine.execute(withLimit, hashGenerator.generateQueryHash(withLimit, 300));

        // a LIMITed answer is truncated — cataloguing it would later roll up wrong totals
        assertThat(sparkCalls.get()).isEqualTo(2);
    }

    @Test
    void aRollUpTheVerifierRefusesFallsBackToTheNormalPath() {
        TimeRange window = new TimeRange(0, 3 * DAY - 1);
        // The fine answer CLAIMS SUM+COUNT in its shape but the frame lacks the COUNT column, so a
        // later AVG query subsumes statically yet cannot be reconstructed — the verifier vetoes and
        // the engine must pay Spark instead of serving a frame with no AVG in it.
        ResultFrame missingCount = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("SUM(latency)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "deviceType", "mobile", "SUM(latency)", 100.0))
                .build();
        AtomicInteger sparkCalls = new AtomicInteger();
        SparkQueryExecutorPort fakeSpark = sql -> {
            sparkCalls.incrementAndGet();
            return missingCount;
        };
        CacheExecutionEngine engine = engineWith(fakeSpark, new CubeShapeCatalog());

        HashComponents fineComponents = HashComponents.of(List.of("appName", "deviceType"),
                List.of("SUM(latency)", "COUNT(latency)"), List.of());
        CanonicalQueryObject fine = new CanonicalQueryObject(fineComponents, window, PostProcessing.none(),
                QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"), List.of());
        engine.execute(fine, hashGenerator.generateQueryHash(fine, 300));

        HashComponents avgComponents =
                HashComponents.of(List.of("appName"), List.of("AVG(latency)"), List.of());
        CanonicalQueryObject avgQuery = new CanonicalQueryObject(avgComponents, window, PostProcessing.none(),
                QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"), List.of());
        engine.execute(avgQuery, hashGenerator.generateQueryHash(avgQuery, 300));

        assertThat(sparkCalls.get()).isEqualTo(2); // the cube refused; Spark answered
    }

    @Test
    void timeSeriesQueriesBypassTheCubePathEntirely() {
        TimeRange window = new TimeRange(0, 3 * DAY - 1);
        AtomicInteger sparkCalls = new AtomicInteger();
        SparkQueryExecutorPort fakeSpark = sql -> {
            sparkCalls.incrementAndGet();
            return fineGrainedFrame();
        };
        CacheExecutionEngine engine = engineWith(fakeSpark, new CubeShapeCatalog());

        HashComponents components =
                HashComponents.of(List.of("appName", "deviceType"), List.of("SUM(bytes)"), List.of());
        CanonicalQueryObject timeSeries = new CanonicalQueryObject(components, window, PostProcessing.none(),
                QueryMetadata.timeSeries(300), "FULL_SQL", List.of("traffic"), List.of());

        engine.execute(timeSeries, hashGenerator.generateQueryHash(timeSeries, 300));
        engine.execute(timeSeries, hashGenerator.generateQueryHash(timeSeries, 300));

        assertThat(sparkCalls.get()).isEqualTo(2); // never catalogued, never served from the cube
    }
}
