package com.cascada.cache.domain.time;

import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.TimeSeriesBucketResampler;
import com.cascada.cache.domain.merge.TimeSeriesBucketResampler.TimeSeriesRow;
import com.cascada.cache.domain.time.TimeBucketPyramid.HierarchicalPlan;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TimeBucketPyramidTest {

    private static final long DAY = 86_400L;
    private static final long HOUR = 3_600L;
    private final TimeBucketPyramid pyramid = new TimeBucketPyramid();

    @Test
    void bucketStartFloorsToEachLevel() {
        assertThat(pyramid.bucketStart(86_700, BucketLevel.FIVE_MINUTE)).isEqualTo(86_700); // 86700 = 289*300
        assertThat(pyramid.bucketStart(90_061, BucketLevel.HOUR)).isEqualTo(90_000);        // 25 * 3600
        assertThat(pyramid.bucketStart(2 * DAY + 5, BucketLevel.DAY)).isEqualTo(2 * DAY);
    }

    @Test
    void aBucketIsCompleteOnlyOnceItsWholeSpanIsInThePast() {
        assertThat(pyramid.isCompleteBucket(0, BucketLevel.HOUR, HOUR)).isTrue();      // [0,3600) done at now=3600
        assertThat(pyramid.isCompleteBucket(0, BucketLevel.HOUR, HOUR - 1)).isFalse(); // still in progress
    }

    @Test
    void todayPlusPastDaysAssemblesFromMixedLevelsAndOnlyTheLiveSubBucketIsPartial() {
        // now = 2 days + 3 hours + 100s into the live hour
        long now = 2 * DAY + 3 * HOUR + 100;
        HierarchicalPlan plan = pyramid.assemble(0, now);

        // days 0 and 1 are complete; day 2 is in progress
        assertThat(plan.completeDayBucketStarts()).containsExactly(0L, DAY);
        // hours 0,1,2 of day 2 are complete; hour 3 is live
        assertThat(plan.completeHourBucketStartsToday())
                .containsExactly(2 * DAY, 2 * DAY + HOUR, 2 * DAY + 2 * HOUR);
        assertThat(plan.livePartialRange()).isPresent();
        assertThat(plan.livePartialRange().get().startTimestampSeconds()).isEqualTo(2 * DAY + 3 * HOUR);
        assertThat(plan.livePartialRange().get().endTimestampSeconds()).isEqualTo(now);
    }

    @Test
    void compactionFromFiveMinuteToHourPreservesTotals() {
        // twelve 5-minute buckets in one hour, each summing 1.0 -> the hour bucket must sum to 12.0
        TimeSeriesBucketResampler resampler = new TimeSeriesBucketResampler();
        List<TimeSeriesRow> fiveMinute = new ArrayList<>();
        for (int slot = 0; slot < 12; slot++) {
            long bucketStart = pyramid.bucketStart(slot * 300L, BucketLevel.FIVE_MINUTE);
            fiveMinute.add(new TimeSeriesRow(bucketStart, Map.of("appName", "netflix"), Map.of("sum_bytes", 1.0)));
        }
        List<TimeSeriesRow> hourly = resampler.resampleToUserStep(
                fiveMinute, 300, (int) HOUR, Map.of("sum_bytes", AggregateFunction.SUM));

        assertThat(hourly).hasSize(1);
        assertThat(hourly.get(0).bucketStartSeconds()).isZero();
        assertThat(hourly.get(0).measures().get("sum_bytes")).isEqualTo(12.0);
    }

    @Test
    void rollUpBucketStartMapsAFinerBucketToItsParent() {
        long fiveMinuteStart = 2 * DAY + 7 * HOUR + 1500; // somewhere inside hour 7 of day 2
        assertThat(pyramid.rollUpBucketStart(fiveMinuteStart, BucketLevel.HOUR)).isEqualTo(2 * DAY + 7 * HOUR);
        assertThat(pyramid.rollUpBucketStart(fiveMinuteStart, BucketLevel.DAY)).isEqualTo(2 * DAY);
    }
}
