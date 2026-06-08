package com.cascada.cache.domain.time;

import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ports the head/body/tail behaviour of {@code TimeBucketCalculator} from {@code time_utils.py}
 * and pins it with worked scenarios plus algebraic properties (jqwik).
 */
class TimeBucketCalculatorTest {

    private static final long DAY = 86_400L;
    private final TimeBucketCalculator calculator = new TimeBucketCalculator();

    @Test
    void threeWholeDaysProduceThreeBodyBucketsAndNoPartialEdges() {
        DailyBuckets buckets = calculator.getDailyBuckets(0, 3 * DAY - 1);
        assertThat(buckets.head()).isEmpty();
        assertThat(buckets.tail()).isEmpty();
        assertThat(buckets.body()).containsExactly(0L, DAY, 2 * DAY);
    }

    @Test
    void startAfterMidnightProducesAPartialHeadDay() {
        DailyBuckets buckets = calculator.getDailyBuckets(100, 2 * DAY - 1);
        assertThat(buckets.head()).contains(new TimeRange(100, DAY - 1));
        assertThat(buckets.body()).containsExactly(DAY);
        assertThat(buckets.tail()).isEmpty();
    }

    @Test
    void endBeforeMidnightProducesAPartialTailDay() {
        DailyBuckets buckets = calculator.getDailyBuckets(0, DAY + 100);
        assertThat(buckets.head()).isEmpty();
        assertThat(buckets.body()).containsExactly(0L);
        assertThat(buckets.tail()).contains(new TimeRange(DAY, DAY + 100));
    }

    @Test
    void subDayRangeHasNoBodyAndHeadEqualsTailRange() {
        DailyBuckets buckets = calculator.getDailyBuckets(100, 200);
        assertThat(buckets.body()).isEmpty();
        assertThat(buckets.head()).contains(new TimeRange(100, 200));
        assertThat(buckets.tail()).contains(new TimeRange(100, 200));
    }

    @Test
    void calculateGapsRemovesAlreadyCachedBodyDays() {
        GapPlan gapPlan = calculator.calculateGaps(0, 3 * DAY - 1, List.of(DAY));
        assertThat(gapPlan.body()).containsExactly(0L, 2 * DAY);
        assertThat(gapPlan.hasGaps()).isTrue();
    }

    @Test
    void calculateGapsWithEverythingCachedReportsNoGaps() {
        GapPlan gapPlan = calculator.calculateGaps(0, 3 * DAY - 1, List.of(0L, DAY, 2 * DAY));
        assertThat(gapPlan.body()).isEmpty();
        assertThat(gapPlan.hasGaps()).isFalse();
    }

    @Test
    void nonPositiveBucketWidthFallsBackToOneDay() {
        TimeBucketCalculator degenerate = new TimeBucketCalculator(0);
        assertThat(degenerate.secondsPerBucket()).isEqualTo(DAY);
    }

    @Property
    void bodyDaysAreStrictlyIncreasingDayAlignedMultiples(
            @ForAll @LongRange(min = 0, max = 50L * 86_400L) long start,
            @ForAll @LongRange(min = 0, max = 50L * 86_400L) long extra) {
        long end = start + extra;
        List<Long> body = calculator.getDailyBuckets(start, end).body();
        long previous = Long.MIN_VALUE;
        for (long day : body) {
            assertThat(day % DAY).isZero();
            assertThat(day).isGreaterThan(previous);
            previous = day;
        }
    }

    @Property
    void partialEdgesAlwaysAnchorToTheRequestedBoundaries(
            @ForAll @LongRange(min = 1, max = 10L * 86_400L) long start,
            @ForAll @LongRange(min = 1, max = 10L * 86_400L) long extra) {
        long end = start + extra;
        DailyBuckets buckets = calculator.getDailyBuckets(start, end);
        buckets.head().ifPresent(head -> assertThat(head.startTimestampSeconds()).isEqualTo(start));
        buckets.tail().ifPresent(tail -> assertThat(tail.endTimestampSeconds()).isEqualTo(end));
    }
}
