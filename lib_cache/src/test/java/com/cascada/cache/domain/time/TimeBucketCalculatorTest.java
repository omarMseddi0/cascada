package com.cascada.cache.domain.time;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void materializesAtMostTheConfiguredBucketLimit() {
        long withinLimitEnd = (long) TimeBucketCalculator.MAX_BUCKETS_PER_PLAN * DAY - 1;
        assertThat(calculator.getDailyBuckets(0, withinLimitEnd).body())
                .hasSize(TimeBucketCalculator.MAX_BUCKETS_PER_PLAN);

        long aboveLimitEnd = (long) (TimeBucketCalculator.MAX_BUCKETS_PER_PLAN + 1) * DAY - 1;
        assertThatThrownBy(() -> calculator.getDailyBuckets(0, aboveLimitEnd))
                .isInstanceOf(BucketEnumerationLimitExceededException.class);
    }

    @Test
    void narrowRangesAtLongExtremesRemainPartialInsteadOfOverflowing() {
        DailyBuckets nearMinimum = calculator.getDailyBuckets(Long.MIN_VALUE, Long.MIN_VALUE + 10);
        assertThat(nearMinimum.body()).isEmpty();
        assertThat(nearMinimum.head().isPresent() || nearMinimum.tail().isPresent()).isTrue();
        nearMinimum.head().ifPresent(range -> {
            assertThat(range.startTimestampSeconds()).isEqualTo(Long.MIN_VALUE);
            assertThat(range.endTimestampSeconds()).isLessThanOrEqualTo(Long.MIN_VALUE + 10);
        });
        nearMinimum.tail().ifPresent(range -> {
            assertThat(range.startTimestampSeconds()).isGreaterThanOrEqualTo(Long.MIN_VALUE);
            assertThat(range.endTimestampSeconds()).isEqualTo(Long.MIN_VALUE + 10);
        });

        DailyBuckets nearMaximum = calculator.getDailyBuckets(Long.MAX_VALUE, Long.MAX_VALUE);
        assertThat(nearMaximum.body()).isEmpty();
        assertThat(nearMaximum.head().isPresent() || nearMaximum.tail().isPresent()).isTrue();
        nearMaximum.head().ifPresent(range -> assertThat(range).isEqualTo(new TimeRange(Long.MAX_VALUE, Long.MAX_VALUE)));
        nearMaximum.tail().ifPresent(range -> assertThat(range).isEqualTo(new TimeRange(Long.MAX_VALUE, Long.MAX_VALUE)));
    }

    @Test
    void aCompleteSingleSecondBucketAtLongMaxRemainsRepresentable() {
        DailyBuckets buckets = new TimeBucketCalculator(1).getDailyBuckets(Long.MAX_VALUE, Long.MAX_VALUE);

        assertThat(buckets.body()).containsExactly(Long.MAX_VALUE);
        assertThat(buckets.head()).isEmpty();
        assertThat(buckets.tail()).isEmpty();
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
