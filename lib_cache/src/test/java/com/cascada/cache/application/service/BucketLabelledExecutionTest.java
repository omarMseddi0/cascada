package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.sketch.DataSketchesDistinctSketchAdapter;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.PostProcessing;
import com.cascada.cache.domain.query.QueryMetadata;
import com.cascada.cache.domain.time.TimeRange;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.datasketches.hll.HllSketch;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Sketch measures merge by HLL union. */
class BucketLabelledExecutionTest {

    private static final long DAY = 86_400L;

    @Test
    void distinctSketchMeasuresMergeByUnionAndLeaveAsEstimates() {
        CanonicalQueryObject distinctUsers = new CanonicalQueryObject(
                HashComponents.of(List.of("region"), List.of("APPROX_COUNT_DISTINCT(user_id)"), List.of()),
                new TimeRange(0, 2 * DAY - 1), PostProcessing.none(),
                new QueryMetadata(false, 0L, List.of(), Map.of(), Optional.empty(), false,
                        List.of("APPROX_COUNT_DISTINCT(user_id) AS users"),
                        Map.of("users", AggregateFunction.DISTINCT_SKETCH)),
                "SQL", List.of("events"), List.of());
        FrameMergeService merging = new FrameMergeService(300, "ts", new DataSketchesDistinctSketchAdapter());

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
