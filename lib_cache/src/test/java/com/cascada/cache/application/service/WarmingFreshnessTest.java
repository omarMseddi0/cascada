package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.adapter.out.index.InMemoryCoverageIndexAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.tracking.ValkeyQueryPopularityAdapter;
import com.cascada.cache.application.model.warming.WarmingReport;
import com.cascada.cache.application.model.config.WarmingConfiguration;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.application.service.warming.DataChangeDetector;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.fixture.ValkeyTestDatabase;
import com.cascada.identity.domain.model.QueryHash;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/** The warmer is the only writer: complete buckets only, always with a time-to-live, bounded in size. */
@EnabledIfEnvironmentVariable(named = ValkeyTestDatabase.ENVIRONMENT_VARIABLE, matches = ".+")
class WarmingFreshnessTest {

    @RegisterExtension
    final ValkeyTestDatabase valkey = new ValkeyTestDatabase();

    @BeforeEach
    void connectValkey() {
        backend = valkey.cache(new PortableFrameSerializer());
        popularity = valkey.popularity();
    }

    private static final long DAY = 86_400L;
    private static final QueryHash TRAFFIC = QueryHash.of("0000000000000000000000000000000a");
    private static final String TRAFFIC_SQL = "SELECT appName, SUM(bytes) FROM traffic GROUP BY appName";

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.ofEpochSecond(2 * DAY + 12 * 3_600));
    private final Clock movableClock = new Clock() {
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }
    };
    private ValkeyCacheBackendAdapter backend;
    private final AtomicInteger sparkCalls = new AtomicInteger();
    private ValkeyQueryPopularityAdapter popularity;

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

        var keys = List.of(CacheKeyFactory.buildBucketKey(TRAFFIC, 0, DAY),
                CacheKeyFactory.buildBucketKey(TRAFFIC, DAY, DAY));
        assertThat(backend.remainingTimeToLive(keys)).hasValueSatisfying(remaining ->
                assertThat(remaining).isBetween(Duration.ofHours(23), Duration.ofDays(1)));
        keys.forEach(valkey::expireBucket);

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
        assertThat(backend.sizeReport().bucketCount()).isZero();
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
    void aBackfillInvalidatesOnlyTheChangedBucketAcrossQueryHashes() {
        var totals = new AtomicReference<>(Map.of(0L, 1L, DAY, 2L));
        var warmed = new ArrayList<Long>();
        var warmer = groupedWarmer(totals, warmed, () -> {});
        runGroupedCycle(warmer);
        assertThat(warmed).containsExactly(0L, DAY);
        var other = QueryHash.of("0000000000000000000000000000000b");
        var changed = CacheKeyFactory.buildBucketKey(other, 0, DAY);
        var unchanged = CacheKeyFactory.buildBucketKey(other, DAY, DAY);
        var otherWidth = CacheKeyFactory.buildBucketKey(other, 0, 2 * DAY);
        for (String key : List.of(changed, unchanged, otherWidth)) {
            backend.store(key, trafficRows(1), Duration.ofDays(1));
        }
        totals.set(Map.of(DAY, 2L)); // Last source row in the first bucket was deleted.
        warmed.clear();

        var report = runGroupedCycle(warmer);

        assertThat(warmed).containsExactly(0L);
        assertThat(report.bucketsWarmed()).isEqualTo(1);
        assertThat(report.bucketsSkipped()).isEqualTo(1);
        assertThat(backend.existsForKeys(List.of(changed, unchanged, otherWidth))).containsExactly(false, true, true);
        warmed.clear();
        runGroupedCycle(warmer);
        assertThat(warmed).isEmpty();
    }

    @Test
    void aSecondBackfillDuringWarmingEvictsTheAffectedWriteAndRetriesTheBucket() {
        var totals = new AtomicReference<>(Map.of(0L, 1L, DAY, 2L));
        var warmed = new ArrayList<Long>();
        var appendDuringWarm = new AtomicBoolean();
        var warmer = groupedWarmer(totals, warmed, () -> {
            if (appendDuringWarm.getAndSet(false)) totals.set(Map.of(0L, 9L, DAY, 2L));
        });
        runGroupedCycle(warmer);
        var baseline = backend.loadValidationBaseline();
        totals.set(Map.of(0L, 5L, DAY, 2L));
        appendDuringWarm.set(true);
        warmed.clear();

        var unstable = runGroupedCycle(warmer);

        assertThat(warmed).containsExactly(0L);
        assertThat(unstable.bucketsWarmed()).isZero();
        assertThat(isCached(0)).isFalse();
        assertThat(isCached(DAY)).isTrue();
        var preWarmBaseline = backend.loadValidationBaseline();
        assertThat(preWarmBaseline).isNotEqualTo(baseline);
        warmed.clear();
        warmer.warmLookbackWindow(now.get().getEpochSecond(), Duration.ofDays(2));
        assertThat(warmed).containsExactly(0L);
        assertThat(isCached(0)).isTrue();
        assertThat(backend.loadValidationBaseline()).isNotEqualTo(preWarmBaseline);
    }

    @Test
    void anUnavailablePostWarmCheckDiscardsOnlyUnverifiedWrites() {
        var totals = new AtomicReference<>(Map.of(0L, 1L, DAY, 2L));
        var warmed = new ArrayList<Long>();
        var failValidation = new AtomicBoolean();
        var warmer = groupedWarmer(totals, warmed, () -> {
            if (failValidation.getAndSet(false)) totals.set(null);
        });
        runGroupedCycle(warmer);
        var baseline = backend.loadValidationBaseline();
        totals.set(Map.of(0L, 5L, DAY, 2L));
        failValidation.set(true);
        warmed.clear();

        var report = runGroupedCycle(warmer);

        assertThat(report.bucketsWarmed()).isZero();
        assertThat(warmed).containsExactly(0L);
        assertThat(isCached(0)).isFalse();
        assertThat(isCached(DAY)).isTrue();
        assertThat(backend.loadValidationBaseline()).isEqualTo(baseline);
        totals.set(Map.of(0L, 7L, DAY, 2L));
        warmed.clear();
        warmer.warmLookbackWindow(now.get().getEpochSecond(), Duration.ofDays(2));
        assertThat(warmed).containsExactly(0L);
    }

    private WarmingOrchestrator groupedWarmer(AtomicReference<Map<Long, Long>> totals, List<Long> warmed,
                                             Runnable duringWarm) {
        var detector = new DataChangeDetector(backend, sql -> sql, sql -> {
            var builder = ResultFrame.builder().column("bucket_start", ColumnType.LONG).column("total", ColumnType.LONG);
            Map<Long, Long> snapshot = totals.get();
            if (snapshot == null) throw new IllegalStateException("validation source unavailable");
            snapshot.forEach(builder::row);
            return builder.build();
        }, "SELECT {bucketSeconds} FROM traffic WHERE ts >= {start} AND ts <= {end}", Duration.ofDays(7));
        return new WarmingOrchestrator(backend, sql -> {
            warmed.add(Long.parseLong(sql));
            duringWarm.run();
            return trafficRows(1);
        }, (sql, gapPlan) -> Long.toString(gapPlan.tail().orElseThrow().startTimestampSeconds()),
                new WarmingQueue(), popularity, new InMemoryCoverageIndexAdapter(),
                null, detector, null, new WarmingConfiguration(DAY, 0, 100, Duration.ofDays(1), 100), movableClock);
    }

    private WarmingReport runGroupedCycle(WarmingOrchestrator warmer) {
        warmer.recordQuery(TRAFFIC, traffic());
        return warmer.warmLookbackWindow(now.get().getEpochSecond(), Duration.ofDays(2));
    }

    @Test
    void popularQueriesAreWarmedAfterARestartFromTheirPersistedSql() {
        popularity.recordObservation(TRAFFIC, TRAFFIC_SQL);
        WarmingOrchestrator restartedWarmer = warmer(1, 100, null, null);

        WarmingReport report = restartedWarmer.warmCycle(0, 2 * DAY - 1, false);

        assertThat(report.patternsWarmed()).isEqualTo(1);
        QueryHash currentHash = new QueryHashGenerator().generateQueryHash(traffic(), 300);
        assertThat(backend.existsForKeys(List.of(CacheKeyFactory.buildBucketKey(currentHash, 0, DAY)))).containsExactly(true);
        assertThat(isCached(0)).isFalse();
    }

    @Test
    void restartedPatternsUseConfiguredStepAndCurrentSourceEligibility() {
        popularity.recordObservation(TRAFFIC, TRAFFIC_SQL);
        var configuration = new WarmingConfiguration(DAY, 10, 100, Duration.ofDays(1), 100, 600);
        var restarted = new WarmingOrchestrator(backend, sql -> trafficRows(1), (sql, plan) -> sql,
                new WarmingQueue(), popularity, null, sql -> traffic(), null, null,
                configuration, movableClock, CacheConfiguration.defaults());
        restarted.warmCycle(0, DAY - 1, false);
        QueryHash currentHash = new QueryHashGenerator().generateQueryHash(traffic(), 600);
        assertThat(backend.existsForKeys(List.of(CacheKeyFactory.buildBucketKey(currentHash, 0, DAY)))).containsExactly(true);
        assertThat(isCached(0)).isFalse();

        var viewOnly = new WarmingOrchestrator(backend, sql -> { throw new AssertionError("HOT_VIEW must not warm"); },
                (sql, plan) -> sql, new WarmingQueue(), popularity, null,
                sql -> new CanonicalQueryObject(traffic().hashComponents(), traffic().timeRange(), traffic().postProcessing(),
                        traffic().metadata().withUsesHotBatch(false), TRAFFIC_SQL, traffic().sourceSignature(),
                        traffic().projectionSignature()), null, null,
                configuration, movableClock, CacheConfiguration.defaults());
        assertThat(viewOnly.warmCycle(DAY, 2 * DAY - 1, false).bucketsWarmed()).isZero();
    }
    @Test
    void queryObservationDoesNotWaitForBackgroundSparkWork() throws Exception {
        var enteredSpark = new java.util.concurrent.CountDownLatch(1);
        var releaseSpark = new java.util.concurrent.CountDownLatch(1);
        var queue = new WarmingQueue();
        var warmer = new WarmingOrchestrator(backend, sql -> {
            enteredSpark.countDown();
            try {
                if (!releaseSpark.await(5, java.util.concurrent.TimeUnit.SECONDS))
                    throw new AssertionError("test did not release Spark");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            return trafficRows(1);
        }, (sql, plan) -> sql, queue, popularity, DAY, 0);
        warmer.recordQuery(TRAFFIC, traffic());
        try (var tasks = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var cycle = tasks.submit(() -> warmer.warmCycle(0, DAY - 1, false));
            try {
                assertThat(enteredSpark.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                tasks.submit(() -> warmer.recordQuery(TRAFFIC, traffic()))
                        .get(2, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(queue.size()).isEqualTo(1);
            } finally {
                releaseSpark.countDown();
            }
            assertThat(cycle.get(5, java.util.concurrent.TimeUnit.SECONDS).bucketsWarmed()).isEqualTo(1);
        }
    }

}
