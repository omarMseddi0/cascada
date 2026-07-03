package com.cascada.cache.application;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.tracking.QueryPopularityTracker;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.cache.domain.warming.WarmingQueue;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the warming orchestration ({@code _perform_warming_cycle} / {@code _warm_single_pattern_sequential}):
 * Layer-1 queue drain, Layer-2 top-N de-duplication, per-bucket EXISTS-skip, and force-overwrite —
 * using a fake Spark that counts executions and the real in-memory backend.
 */
class WarmingOrchestratorTest {

    private static final long DAY = 86_400L;
    private static final QueryHash A = QueryHash.of("0000000000000000000000000000000a");
    private static final QueryHash B = QueryHash.of("0000000000000000000000000000000b");

    private final AtomicInteger sparkCalls = new AtomicInteger();
    private final InMemoryBlobCacheBackendAdapter backend =
            new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
    private final GapQueryRewriterPort passthroughGap = (sql, plan) -> sql;

    private ResultFrame fakeResult() {
        sparkCalls.incrementAndGet();
        return ResultFrame.builder().column("appName", ColumnType.STRING).column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "SUM(bytes)", 1.0)).build();
    }

    private CanonicalQueryObject canonical(String sql) {
        return new CanonicalQueryObject(
                new HashComponents(List.of("appName"), List.of("SUM(bytes)"), List.of(), 0),
                new TimeRange(0, 3 * DAY - 1), PostProcessing.none(), QueryMetadata.globalAggregate(),
                sql, List.of("traffic"), List.of());
    }

    private WarmingOrchestrator orchestrator(WarmingQueue queue, QueryPopularityTracker tracker) {
        return new WarmingOrchestrator(backend, sql -> fakeResult(), passthroughGap,
                queue, tracker, DAY, 10);
    }

    @Test
    void warmsEveryBodyBucketForEachQueuedPattern() {
        WarmingOrchestrator orchestrator =
                orchestrator(new WarmingQueue(), new QueryPopularityTracker());
        orchestrator.recordQuery(A, canonical("SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName"));
        orchestrator.recordQuery(B, canonical("SELECT appName, SUM(bytes) FROM other WHERE ts >= 0 AND ts <= 100 GROUP BY appName"));

        WarmingOrchestrator.WarmingReport report = orchestrator.warmCycle(0, 3 * DAY - 1, false);

        assertThat(report.patternsWarmed()).isEqualTo(2);
        assertThat(report.bucketsWarmed()).isEqualTo(6); // 3 days x 2 patterns
        assertThat(sparkCalls.get()).isEqualTo(6);
        assertThat(backend.storedBucketCount()).isEqualTo(6);
    }

    @Test
    void skipsAlreadyWarmedBucketsOnTheSecondCycle() {
        QueryPopularityTracker tracker = new QueryPopularityTracker();
        WarmingOrchestrator orchestrator = orchestrator(new WarmingQueue(), tracker);
        orchestrator.recordQuery(A, canonical("SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName"));

        orchestrator.warmCycle(0, 3 * DAY - 1, false); // Layer 1 warms 3 buckets
        sparkCalls.set(0);

        // Second cycle: queue is drained, but Layer 2 (top-N) re-selects A; every bucket already exists.
        WarmingOrchestrator.WarmingReport second = orchestrator.warmCycle(0, 3 * DAY - 1, false);
        assertThat(second.bucketsWarmed()).isZero();
        assertThat(second.bucketsSkipped()).isEqualTo(3);
        assertThat(sparkCalls.get()).isZero(); // nothing recomputed
    }

    @Test
    void forceOverwriteRewarmsEvenAlreadyWarmedBuckets() {
        QueryPopularityTracker tracker = new QueryPopularityTracker();
        WarmingOrchestrator orchestrator = orchestrator(new WarmingQueue(), tracker);
        orchestrator.recordQuery(A, canonical("SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName"));
        orchestrator.warmCycle(0, 3 * DAY - 1, false);
        sparkCalls.set(0);

        WarmingOrchestrator.WarmingReport forced = orchestrator.warmCycle(0, 3 * DAY - 1, true);
        assertThat(forced.bucketsWarmed()).isEqualTo(3);
        assertThat(sparkCalls.get()).isEqualTo(3); // all re-computed
    }

    @Test
    void layer2DoesNotDoubleWarmAHashLayer1AlreadyCovered() {
        QueryPopularityTracker tracker = new QueryPopularityTracker();
        WarmingOrchestrator orchestrator = orchestrator(new WarmingQueue(), tracker);
        // record twice so the tracker definitely has it in top-N, and the queue has one vote
        orchestrator.recordQuery(A, canonical("SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName"));
        orchestrator.recordQuery(A, canonical("SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName"));

        WarmingOrchestrator.WarmingReport report = orchestrator.warmCycle(0, 3 * DAY - 1, false);
        assertThat(report.patternsWarmed()).isEqualTo(1); // not warmed twice
        assertThat(report.bucketsWarmed()).isEqualTo(3);
    }

    @Test
    void perBucketExistsSkipWarmsOnlyTheNewlyRequestedBucket() {
        WarmingOrchestrator orchestrator =
                orchestrator(new WarmingQueue(), new QueryPopularityTracker());
        CanonicalQueryObject canonical = canonical(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");

        orchestrator.warmSinglePattern(A, canonical, 0, DAY - 1, false); // warm only day 0
        sparkCalls.set(0);
        WarmingOrchestrator.PatternWarmingResult result =
                orchestrator.warmSinglePattern(A, canonical, 0, 2 * DAY - 1, false); // days 0 and 1

        assertThat(result.bucketsSkipped()).isEqualTo(1); // day 0 already warm
        assertThat(result.bucketsWarmed()).isEqualTo(1);  // only day 1 computed
        assertThat(sparkCalls.get()).isEqualTo(1);
    }

    @Test
    void neverWarmsTheTrailingPartialBucket() {
        // README caveat 1: with warmEnd mid-bucket (e.g. "now"), the truncated last bucket must NOT be
        // stored under the full-bucket key — the EXISTS-skip would then treat the partial frame as the
        // whole bucket forever (permanent undercount with no self-healing). Warming stops at the last
        // COMPLETE bucket boundary; the live partial bucket stays a gap for the read path to compute.
        WarmingOrchestrator orchestrator =
                orchestrator(new WarmingQueue(), new QueryPopularityTracker());
        CanonicalQueryObject canonical = canonical(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");

        long midThirdBucket = 2 * DAY + 12_345; // "now" is inside day 2
        WarmingOrchestrator.PatternWarmingResult result =
                orchestrator.warmSinglePattern(A, canonical, 0, midThirdBucket, false);

        assertThat(result.bucketsWarmed()).isEqualTo(2); // days 0 and 1 only — day 2 is incomplete
        assertThat(sparkCalls.get()).isEqualTo(2);
        String partialBucketKey = com.cascada.cache.domain.CacheKeyFactory.buildBucketKey(A, 2 * DAY, DAY);
        assertThat(backend.existsForKeys(List.of(partialBucketKey))).containsExactly(false);
    }

    @Test
    void warmsTheLastBucketWhenTheWindowEndsExactlyOnItsBoundary() {
        WarmingOrchestrator orchestrator =
                orchestrator(new WarmingQueue(), new QueryPopularityTracker());
        CanonicalQueryObject canonical = canonical(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");

        WarmingOrchestrator.PatternWarmingResult result =
                orchestrator.warmSinglePattern(A, canonical, 0, 3 * DAY - 1, false); // inclusive boundary

        assertThat(result.bucketsWarmed()).isEqualTo(3); // all three buckets are complete
    }

    @Test
    void coverageBitmapSkipsWarmedBucketsWithoutAnyPerBucketExistsRoundTrip() {
        // A 55-day lookback must NOT pay 55 EXISTS calls every cycle: with a coverage index, one
        // bitmap load answers presence for the whole window and only the missing buckets are warmed.
        com.cascada.cache.adapter.index.InMemoryCoverageIndexAdapter coverageIndex =
                new com.cascada.cache.adapter.index.InMemoryCoverageIndexAdapter();
        WarmingOrchestrator orchestrator = new WarmingOrchestrator(backend, sql -> fakeResult(),
                passthroughGap, new WarmingQueue(), new QueryPopularityTracker(),
                coverageIndex, DAY, 10);
        CanonicalQueryObject canonical = canonical(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");

        orchestrator.warmSinglePattern(A, canonical, 0, 55 * DAY - 1, false); // day 0..54 warmed + bits set
        sparkCalls.set(0);
        long existsCallsBefore = backend.existsCallCount();

        WarmingOrchestrator.PatternWarmingResult next =
                orchestrator.warmSinglePattern(A, canonical, 0, 56 * DAY - 1, false); // one new day

        assertThat(next.bucketsSkipped()).isEqualTo(55);
        assertThat(next.bucketsWarmed()).isEqualTo(1);   // only day 55 computed
        assertThat(sparkCalls.get()).isEqualTo(1);
        assertThat(backend.existsCallCount()).isEqualTo(existsCallsBefore); // zero EXISTS round-trips
    }

    @Test
    void forceOverwriteIgnoresTheCoverageBitmap() {
        com.cascada.cache.adapter.index.InMemoryCoverageIndexAdapter coverageIndex =
                new com.cascada.cache.adapter.index.InMemoryCoverageIndexAdapter();
        WarmingOrchestrator orchestrator = new WarmingOrchestrator(backend, sql -> fakeResult(),
                passthroughGap, new WarmingQueue(), new QueryPopularityTracker(),
                coverageIndex, DAY, 10);
        CanonicalQueryObject canonical = canonical(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
        orchestrator.warmSinglePattern(A, canonical, 0, 3 * DAY - 1, false);
        sparkCalls.set(0);

        WarmingOrchestrator.PatternWarmingResult forced =
                orchestrator.warmSinglePattern(A, canonical, 0, 3 * DAY - 1, true);
        assertThat(forced.bucketsWarmed()).isEqualTo(3); // data-change signal recomputes everything
        assertThat(sparkCalls.get()).isEqualTo(3);
    }
}
