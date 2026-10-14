package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.sketch.DataSketchesDistinctSketchAdapter;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.PostProcessing;
import com.cascada.cache.domain.query.QueryMetadata;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.identity.domain.QueryHash;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.datasketches.hll.HllSketch;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** One bucket-labelled Spark job fills many buckets; sketch measures merge by HLL union. */
class BucketLabelledExecutionTest {

    private static final long DAY = 86_400L;
    private static final long HOUR = 3_600L;
    private static final QueryHash HASH = QueryHash.of("0000000000000000000000000000000c");

    private final InMemoryBlobCacheBackendAdapter backend =
            new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
    private final List<String> executed = new ArrayList<>();

    /** Labelled SQL carries the requested body days so the fake executor can answer for them. */
    private final GapQueryRewriterPort rewriter = new GapQueryRewriterPort() {
        @Override
        public String buildGapQuery(String physicalSql, GapPlan gapPlan) {
            return "GAP " + gapPlan.body();
        }

        @Override
        public Optional<String> buildBucketedGapQuery(String physicalSql, GapPlan gapPlan, String bucketColumn) {
            return Optional.of("LABELLED " + gapPlan.body());
        }
    };

    /** Two hourly rows in every day except day 1, which has no data. */
    private final QueryExecutorPort spark = sql -> {
        executed.add(sql);
        ResultFrameBuilder builder = ResultFrame.builder().column("ts", ColumnType.LONG)
                .column("total", ColumnType.DOUBLE);
        boolean labelled = sql.startsWith("LABELLED");
        if (labelled) builder.column(CacheExecutionEngine.BUCKET_COLUMN, ColumnType.LONG);
        for (String day : sql.replaceAll(".*\\[|\\]", "").split(",\\s*")) {
            if (day.isBlank() || Long.parseLong(day) == DAY) continue;
            long start = Long.parseLong(day);
            for (long hour = 0; hour < 2; hour++) {
                if (labelled) builder.row(start + hour * HOUR, 1.0, start);
                else builder.row(start + hour * HOUR, 1.0);
            }
        }
        return builder.build();
    };

    private CanonicalQueryObject hourlySeries() {
        return new CanonicalQueryObject(HashComponents.of(List.of("ts"), List.of("SUM(amount)"), List.of()),
                new TimeRange(0, 3 * DAY - 1), PostProcessing.none(), QueryMetadata.timeSeries(3_600),
                "SERIES_SQL", List.of("events"), List.of());
    }

    private CacheExecutionEngine engine() {
        return new CacheExecutionEngine(backend, spark, rewriter,
                new CacheExecutionConfiguration(DAY, 300, "ts"));
    }

    private boolean stored(long day) {
        return backend.existsForKeys(List.of(CacheKeyFactory.buildBucketKey(HASH, day, DAY))).get(0);
    }

    @Test
    void coldTimeSeriesComputesEveryBucketInOneSparkJobAndStoresEachBucket() {
        ResultFrame answer = engine().execute(hourlySeries(), HASH);

        assertThat(executed).containsExactly("LABELLED [0, 86400, 172800]");
        assertThat(answer.rowCount()).isEqualTo(4);
        assertThat(stored(0)).isTrue();
        assertThat(stored(DAY)).as("an empty grouped bucket is a valid, cacheable result").isTrue();
        assertThat(stored(2 * DAY)).isTrue();

        executed.clear();
        assertThat(engine().execute(hourlySeries(), HASH).rowCount()).isEqualTo(4);
        assertThat(executed).as("the repeated query is a full cache hit").isEmpty();
    }

    @Test
    void partialHitStoresTheMissingBucketsItComputed() {
        backend.store(CacheKeyFactory.buildBucketKey(HASH, 0, DAY), spark.execute("GAP [0]"));
        executed.clear();

        ResultFrame answer = engine().execute(hourlySeries(), HASH);

        assertThat(executed).containsExactly("LABELLED [86400, 172800]");
        assertThat(answer.rowCount()).isEqualTo(4);
        assertThat(stored(DAY)).isTrue();
        assertThat(stored(2 * DAY)).isTrue();
    }

    @Test
    void warmingComputesMissingBucketsInOneSparkJob() {
        WarmingOrchestrator warming = new WarmingOrchestrator(backend, spark, rewriter, new WarmingQueue(),
                new QueryPopularityTracker(), DAY, 10);

        var result = warming.warmSinglePattern(HASH, hourlySeries(), 0, 3 * DAY - 1, false);

        assertThat(result.bucketsWarmed()).isEqualTo(3);
        assertThat(executed).containsExactly("LABELLED [0, 86400, 172800]");
        assertThat(warming.warmSinglePattern(HASH, hourlySeries(), 0, 3 * DAY - 1, false).bucketsSkipped())
                .isEqualTo(3);
        assertThat(executed).hasSize(1);
    }

    @Test
    void distinctSketchMeasuresMergeByUnionAndLeaveAsEstimates() {
        CanonicalQueryObject distinctUsers = new CanonicalQueryObject(
                HashComponents.of(List.of("region"), List.of("APPROX_COUNT_DISTINCT(user_id)"), List.of()),
                new TimeRange(0, 2 * DAY - 1), PostProcessing.none(),
                new QueryMetadata(false, 0L, List.of(), Map.of(), Optional.empty(), false,
                        List.of("APPROX_COUNT_DISTINCT(user_id) AS users"),
                        Map.of("users", AggregateFunction.DISTINCT_SKETCH)),
                "SQL", List.of("events"), List.of());
        FrameMergeService merging = new FrameMergeService(300, "ts", new DataSketchesDistinctSketchAdapter(), null);

        IncrementalFrameMerge merge = merging.incremental(distinctUsers);
        merge.add(sketchFrame("a", 0, 6_000));      // day one: users 0..5999
        merge.add(sketchFrame("a", 4_000, 10_000)); // day two overlaps 2000 users
        merge.add(sketchFrame("b", 0, 0));          // a group with an empty sketch
        ResultFrame answer = merge.finish();

        assertThat(answer.columnType("users")).isEqualTo(ColumnType.LONG);
        assertThat(answer.longAt(0, answer.columnIndex("users"))).isCloseTo(10_000L, within(400L));
        assertThat(answer.longAt(1, answer.columnIndex("users"))).isZero();
    }

    private static ResultFrame sketchFrame(String region, int fromUser, int toUser) {
        HllSketch sketch = new HllSketch(DataSketchesDistinctSketchAdapter.LOG_CONFIG_K);
        for (int user = fromUser; user < toUser; user++) sketch.update("v" + user);
        String encoded = Base64.getMimeEncoder().encodeToString(sketch.toUpdatableByteArray());
        return ResultFrame.builder().column("region", ColumnType.STRING).column("users", ColumnType.STRING)
                .row(region, encoded).build();
    }
}
