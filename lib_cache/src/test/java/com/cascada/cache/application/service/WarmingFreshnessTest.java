package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.application.config.WarmingConfiguration;
import com.cascada.cache.application.result.WarmingReport;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.warming.DataChangeDetector;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.PostProcessing;
import com.cascada.cache.domain.query.QueryMetadata;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.identity.domain.QueryHash;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The warmer is the only writer: complete buckets only, always with a time-to-live, bounded in size. */
class WarmingFreshnessTest {

    private static final long DAY = 86_400L;
    private static final QueryHash TRAFFIC = QueryHash.of("0000000000000000000000000000000a");
    private static final String TRAFFIC_SQL = "SELECT appName, SUM(bytes) FROM traffic GROUP BY appName";

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.ofEpochSecond(2 * DAY + 12 * 3_600));
    private final Clock movableClock = new Clock() {
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }
    };
    private final InMemoryBlobCacheBackendAdapter backend =
            new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer(), movableClock);
    private final AtomicInteger sparkCalls = new AtomicInteger();
    private final QueryPopularityTracker popularity = new QueryPopularityTracker();

    private ResultFrame trafficRows(int rows) {
        sparkCalls.incrementAndGet();
        var builder = ResultFrame.builder().column("appName", ColumnType.STRING).column("SUM(bytes)", ColumnType.DOUBLE);
        for (int row = 0; row < rows; row++) {
            builder.row("app-" + row, 1.0);
        }
        return builder.build();
    }

    private CanonicalQueryObject traffic() {
        return new CanonicalQueryObject(HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of()),
                new TimeRange(0, 2 * DAY - 1), PostProcessing.none(), QueryMetadata.globalAggregate(), TRAFFIC_SQL,
                List.of("traffic"), List.of());
    }

    private WarmingOrchestrator warmer(int resultRows, int maximumCachedRows, DataChangeDetector detector,
                                       CubeShapeCatalog cubeCatalog) {
        return new WarmingOrchestrator(backend, sql -> trafficRows(resultRows), (sql, gapPlan) -> sql,
                new WarmingQueue(), popularity, null, sql -> traffic(), detector, cubeCatalog,
                new WarmingConfiguration(DAY, 10, 100, Duration.ofDays(1), maximumCachedRows), movableClock);
    }

    private boolean isCached(long bucketStart) {
        return backend.existsForKeys(List.of(CacheKeyFactory.buildBucketKey(TRAFFIC, bucketStart, DAY))).get(0);
    }

    @Test
    void aRequestedWindowIsClampedToBucketsThatHaveFinished() {
        WarmingOrchestrator warmer = warmer(1, 100, null, null);
        warmer.recordQuery(TRAFFIC, traffic());

        WarmingReport report = warmer.warmCycle(0, 4 * DAY - 1, false);

        assertThat(report.bucketsWarmed()).isEqualTo(2);
        assertThat(isCached(DAY)).isTrue();
        assertThat(isCached(2 * DAY)).isFalse();
    }

    @Test
    void everyWarmedBucketExpiresAfterTheConfiguredTimeToLive() {
        WarmingOrchestrator warmer = warmer(1, 100, null, null);
        warmer.recordQuery(TRAFFIC, traffic());
        warmer.warmCycle(0, 2 * DAY - 1, false);

        now.set(now.get().plus(Duration.ofDays(1)));

        assertThat(isCached(0)).isFalse();
        assertThat(isCached(DAY)).isFalse();
    }

    @Test
    void aFrameAboveTheRowCapIsComputedButNotStored() {
        WarmingOrchestrator warmer = warmer(3, 2, null, null);
        warmer.recordQuery(TRAFFIC, traffic());

        WarmingReport report = warmer.warmCycle(0, 2 * DAY - 1, false);

        assertThat(report.bucketsWarmed()).isZero();
        assertThat(report.bucketsSkipped()).isEqualTo(2);
        assertThat(backend.storedBucketCount()).isZero();
    }

    @Test
    void changedHistoryRewritesCachedBucketsAndDropsCataloguedCubes() {
        CubeShapeCatalog cubeCatalog = new CubeShapeCatalog();
        cubeCatalog.register(new TimeRange(0, DAY - 1), new QueryShape(java.util.Set.of("appName"),
                java.util.Set.of(), java.util.Set.of("SUM(bytes)")), trafficRows(1));
        DataChangeDetector detector = new DataChangeDetector(backend, logicalSql -> logicalSql,
                sql -> trafficRows(1), "SELECT 1 FROM traffic WHERE ts >= {start} AND ts <= {end}", Duration.ofDays(7));
        WarmingOrchestrator warmer = warmer(1, 100, detector, cubeCatalog);
        warmer.recordQuery(TRAFFIC, traffic());
        warmer.warmCycle(0, 2 * DAY - 1, false);
        warmer.recordQuery(TRAFFIC, traffic());
        now.set(Instant.ofEpochSecond(4 * DAY + 12 * 3_600));

        WarmingReport report = warmer.warmLookbackWindow(now.get().getEpochSecond(), Duration.ofDays(4));

        assertThat(report.bucketsWarmed()).isEqualTo(4);
        assertThat(report.bucketsSkipped()).isZero();
        assertThat(cubeCatalog.windowCount()).isZero();
    }

    @Test
    void popularQueriesAreWarmedAfterARestartFromTheirPersistedSql() {
        popularity.recordObservation(TRAFFIC, TRAFFIC_SQL);
        WarmingOrchestrator restartedWarmer = warmer(1, 100, null, null);

        WarmingReport report = restartedWarmer.warmCycle(0, 2 * DAY - 1, false);

        assertThat(report.patternsWarmed()).isEqualTo(1);
        assertThat(isCached(0)).isTrue();
    }
}
