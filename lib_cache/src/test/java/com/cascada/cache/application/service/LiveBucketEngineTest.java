package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.time.GapPlan;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.identity.domain.model.QueryHash;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The bucket holding "now" is live data: it is always computed and never served from any cache tier. */
class LiveBucketEngineTest {

    private static final long DAY = 86_400L;
    private static final Clock MIDDAY_OF_THIRD_DAY =
            Clock.fixed(Instant.ofEpochSecond(2 * DAY + 12 * 3_600), ZoneOffset.UTC);

    private final InMemoryBlobCacheBackendAdapter backend =
            new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());

    private CanonicalQueryObject trafficByApp(TimeRange window) {
        return new CanonicalQueryObject(HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of()),
                window, PostProcessing.none(), QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"),
                List.of());
    }

    private ResultFrame bytes(double value) {
        return ResultFrame.builder().column("appName", ColumnType.STRING).column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "SUM(bytes)", value)).build();
    }

    @Test
    void aStaleKeyForTheLiveBucketIsNeverReadAndItsRangeIsRecomputed() {
        CanonicalQueryObject query = trafficByApp(new TimeRange(0, 3 * DAY - 1));
        QueryHash hash = new QueryHashGenerator().generateQueryHash(query);
        backend.store(CacheKeyFactory.buildBucketKey(hash, 0, DAY), bytes(1), Duration.ofDays(1));
        backend.store(CacheKeyFactory.buildBucketKey(hash, DAY, DAY), bytes(2), Duration.ofDays(1));
        backend.store(CacheKeyFactory.buildBucketKey(hash, 2 * DAY, DAY), bytes(1_000), Duration.ofDays(1));
        AtomicReference<GapPlan> requestedGap = new AtomicReference<>();
        CacheExecutionEngine engine = new CacheExecutionEngine(backend, sql -> bytes(4),
                (sql, gapPlan) -> {
                    requestedGap.set(gapPlan);
                    return "GAP_SQL";
                },
                CacheExecutionConfiguration.defaults(), null, null, ForkJoinPool.commonPool(), MIDDAY_OF_THIRD_DAY);

        ResultFrame answer = engine.execute(query, hash);

        assertThat(requestedGap.get().body()).isEmpty();
        assertThat(requestedGap.get().tail()).contains(new TimeRange(2 * DAY, 3 * DAY - 1));
        assertThat(((Number) answer.rows().get(0).get("SUM(bytes)")).doubleValue()).isEqualTo(7.0);
    }

    @Test
    void aWindowTouchingTheLiveBucketIsNotCataloguedAsACube() {
        AtomicInteger sparkCalls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            sparkCalls.incrementAndGet();
            return bytes(5);
        };
        CacheExecutionEngine engine = new CacheExecutionEngine(backend, spark, (sql, gapPlan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults(), null, new CubeShapeCatalog(), ForkJoinPool.commonPool(),
                MIDDAY_OF_THIRD_DAY);
        CanonicalQueryObject liveWindow = trafficByApp(new TimeRange(0, 3 * DAY - 1));
        QueryHash hash = new QueryHashGenerator().generateQueryHash(liveWindow);

        engine.execute(liveWindow, hash);
        engine.execute(liveWindow, hash);

        assertThat(sparkCalls.get()).isEqualTo(2);
    }

    @Test
    void aFinishedWindowIsStillCataloguedAsACube() {
        AtomicInteger sparkCalls = new AtomicInteger();
        QueryExecutorPort spark = sql -> {
            sparkCalls.incrementAndGet();
            return bytes(5);
        };
        CacheExecutionEngine engine = new CacheExecutionEngine(backend, spark, (sql, gapPlan) -> "GAP_SQL",
                CacheExecutionConfiguration.defaults(), null, new CubeShapeCatalog(), ForkJoinPool.commonPool(),
                MIDDAY_OF_THIRD_DAY);
        CanonicalQueryObject finishedWindow = trafficByApp(new TimeRange(0, 2 * DAY - 1));
        QueryHash hash = new QueryHashGenerator().generateQueryHash(finishedWindow);

        engine.execute(finishedWindow, hash);
        engine.execute(finishedWindow, hash);

        assertThat(sparkCalls.get()).isEqualTo(1);
    }
}
