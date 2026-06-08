package com.cascada.sql.simulation;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.PortableFrameSerializer;
import com.cascada.cache.application.CacheExecutionConfiguration;
import com.cascada.cache.application.CacheExecutionEngine;
import com.cascada.cache.domain.CacheKeyFactory;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.identity.domain.QueryHash;
import com.cascada.sql.rewrite.GapQueryRewriterAdapter;
import com.cascada.sql.simulation.DirectComputeOracle.MeasureSpec;
import com.cascada.sql.simulation.DirectComputeOracle.TrafficEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cache-correctness gate (TESTING §1.3): the Java heir to {@code in_memory_simulation.py}. For
 * each scenario it primes some day-buckets in the in-memory backend, runs the real engine (which
 * fetches cached buckets and computes the gap via the oracle), and asserts the result is frame-equal
 * to an independent direct aggregation over the full range at the user's step.
 *
 * <p>This proves the governing invariant: <em>the cache path does the same math as the direct path</em>.
 */
class CacheCorrectnessSimulationTest {

    private static final long DAY = 86_400L;
    private static final long START = 0;
    private static final long END = 3 * DAY - 1;

    private final PortableFrameSerializer serializer = new PortableFrameSerializer();
    private final QueryHashGenerator hashGenerator = new QueryHashGenerator();
    private final GapQueryRewriterPort gapRewriter = new GapQueryRewriterAdapter("ts", DAY);

    private List<TrafficEvent> buildFakeEvents() {
        List<TrafficEvent> events = new ArrayList<>();
        String[] apps = {"netflix", "youtube"};
        String[] devices = {"mobile", "tablet"};
        for (long day = 0; day < 3; day++) {
            for (int slot = 0; slot < 4; slot++) { // four 300s slots per day
                long ts = day * DAY + slot * 300L;
                for (String app : apps) {
                    for (String device : devices) {
                        double bytes = day * 100 + slot * 10 + (app.equals("netflix") ? 1 : 2);
                        double latency = slot + (device.equals("mobile") ? 10 : 20);
                        events.add(new TrafficEvent(ts, app, device, bytes, latency));
                    }
                }
            }
        }
        return events;
    }

    @Test
    void globalAggregateSumMatchesDirectComputeOnAPartialHit() {
        DirectComputeOracle oracle = new DirectComputeOracle(buildFakeEvents(), List.of("appName"),
                List.of(new MeasureSpec("SUM", "bytes", "SUM(bytes)")), false);

        CanonicalQueryObject canonical = canonical(
                List.of("appName"), List.of("SUM(bytes)"), QueryMetadata.globalAggregate(),
                "SELECT appName, SUM(bytes) AS s FROM traffic WHERE ts >= 0 AND ts <= 259199 GROUP BY appName");

        ResultFrame cacheResult = runEngineWithDaysCached(canonical, oracle, List.of(0L, DAY));
        ResultFrame groundTruth = oracle.computeGroundTruth(START, END, DirectComputeOracle.FIXED_STEP_SECONDS);

        assertFramesEqual(cacheResult, groundTruth);
    }

    @Test
    void timeSeriesSumResampledToUserStepMatchesDirectCompute() {
        DirectComputeOracle oracle = new DirectComputeOracle(buildFakeEvents(), List.of("appName"),
                List.of(new MeasureSpec("SUM", "bytes", "SUM(bytes)")), true);

        CanonicalQueryObject canonical = canonical(
                List.of("appName"), List.of("SUM(bytes)"), QueryMetadata.timeSeries(600),
                "SELECT FLOOR(ts/300)*300 AS ts, appName, SUM(bytes) AS s FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 259199 GROUP BY FLOOR(ts/300)*300, appName");

        ResultFrame cacheResult = runEngineWithDaysCached(canonical, oracle, List.of(0L, DAY));
        ResultFrame groundTruth = oracle.computeGroundTruth(START, END, 600);

        assertFramesEqual(cacheResult, groundTruth);
    }

    @Test
    void timeSeriesAverageReconstructedFromSumAndCountMatchesDirectCompute() {
        DirectComputeOracle oracle = new DirectComputeOracle(buildFakeEvents(), List.of("deviceType"),
                List.of(new MeasureSpec("COUNT", "latency", "COUNT(latency)"),
                        new MeasureSpec("SUM", "latency", "SUM(latency)")), true);

        QueryMetadata metadata = QueryMetadata.timeSeries(600)
                .withOriginalAggregates(List.of("AVG(latency) AS avgLatency"));
        CanonicalQueryObject canonical = canonical(
                List.of("deviceType"), List.of("COUNT(latency)", "SUM(latency)"), metadata,
                "SELECT FLOOR(ts/300)*300 AS ts, deviceType, SUM(latency) AS sl, COUNT(latency) AS cl "
                        + "FROM traffic WHERE ts >= 0 AND ts <= 259199 GROUP BY FLOOR(ts/300)*300, deviceType");

        // all three days cached -> exercises the all-cache path (no Spark gap)
        ResultFrame cacheResult = runEngineWithDaysCached(canonical, oracle, List.of(0L, DAY, 2 * DAY));

        ResultFrame sumCountGroundTruth = oracle.computeGroundTruth(START, END, 600);
        ResultFrame expected = reconstructAverageFrame(sumCountGroundTruth, "latency", "avgLatency");

        assertFramesEqual(cacheResult, expected);
    }

    // --- harness helpers -------------------------------------------------------------------------

    private CanonicalQueryObject canonical(List<String> groupBy, List<String> aggregates, QueryMetadata metadata,
                                           String physicalSql) {
        HashComponents components = new HashComponents(groupBy, aggregates, List.of(), 0);
        return new CanonicalQueryObject(components, new TimeRange(START, END), PostProcessing.none(), metadata,
                physicalSql, List.of("traffic"), List.of());
    }

    private ResultFrame runEngineWithDaysCached(CanonicalQueryObject canonical, DirectComputeOracle oracle,
                                                List<Long> daysToCache) {
        QueryHash hash = hashGenerator.generateQueryHash(canonical, DirectComputeOracle.FIXED_STEP_SECONDS);
        InMemoryBlobCacheBackendAdapter backend = new InMemoryBlobCacheBackendAdapter(serializer);
        for (long day : daysToCache) {
            backend.store(CacheKeyFactory.buildBucketKey(hash, day, DAY), oracle.computeDayBucket(day, DAY));
        }
        CacheExecutionEngine engine = new CacheExecutionEngine(
                backend, oracle, gapRewriter, CacheExecutionConfiguration.defaults());
        return engine.execute(canonical, hash);
    }

    /** Independent AVG reconstruction for the ground truth: alias = SUM(col) / COUNT(col). */
    private ResultFrame reconstructAverageFrame(ResultFrame sumCountFrame, String field, String alias) {
        String sumColumn = "SUM(" + field + ")";
        String countColumn = "COUNT(" + field + ")";
        ResultFrame.Builder builder = ResultFrame.builder();
        for (String column : sumCountFrame.columnNames()) {
            if (!column.equals(sumColumn) && !column.equals(countColumn)) {
                builder.column(column, sumCountFrame.columnType(column));
            }
        }
        builder.column(alias, ColumnType.DOUBLE);
        for (Map<String, Object> row : sumCountFrame.rows()) {
            Map<String, Object> rebuilt = new LinkedHashMap<>(row);
            double sum = ((Number) row.get(sumColumn)).doubleValue();
            double count = ((Number) row.get(countColumn)).doubleValue();
            rebuilt.remove(sumColumn);
            rebuilt.remove(countColumn);
            rebuilt.put(alias, count == 0 ? Double.NaN : sum / count);
            builder.row(rebuilt);
        }
        return builder.build();
    }

    private void assertFramesEqual(ResultFrame actual, ResultFrame expected) {
        assertThat(normalize(actual)).isEqualTo(normalize(expected));
    }

    /** Canonicalise a frame for comparison: sort rows by all columns, round doubles. */
    private List<Map<String, Object>> normalize(ResultFrame frame) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> row : frame.rows()) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (String column : frame.columnNames()) {
                Object value = row.get(column);
                if (value instanceof Number number) {
                    normalized.put(column, Math.round(number.doubleValue() * 1_000_000.0) / 1_000_000.0);
                } else {
                    normalized.put(column, value);
                }
            }
            rows.add(normalized);
        }
        rows.sort((left, right) -> left.toString().compareTo(right.toString()));
        return rows;
    }
}
