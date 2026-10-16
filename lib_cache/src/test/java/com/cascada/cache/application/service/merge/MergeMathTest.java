package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import java.util.List;
import java.util.Map;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The correctness gate at the pure-math level: the cache path must do the same arithmetic as a
 * direct compute. These tests run the production aggregator and AVG rule against the named root
 * causes from {@code CACHE_INCONSISTENCY_EXPLAINED.md}.
 */
class MergeMathTest {

    @Nested
    class AverageReconstruction {

        @Test
        void reconstructsAverageFromSummedSumAndCountNotByAveragingAverages() {
            // day1: sum=20 count=2 ; day2: sum=400 count=4 -> 420/6 = 70 (NOT (10+100)/2 = 55).
            assertThat(SparkAverage.average(20.0 + 400.0, 2L + 4L)).isEqualTo(70.0);
        }

        @Test
        void zeroCountYieldsSqlNull() {
            assertThat(SparkAverage.average(0.0, 0L)).isNull();
        }

        @Test
        void aDoubleCountMustBeAnExactNonNegativeInteger() {
            assertThat(SparkAverage.average(10.0, 4.0)).isEqualTo(2.5);
            assertThatThrownBy(() -> SparkAverage.average(10.0, 2.6)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> SparkAverage.average(10.0, -1L)).isInstanceOf(IllegalArgumentException.class);
        }

        @Property
        void mergingBucketIngredientsThenDividingEqualsTheGlobalAverage(
                @ForAll @Size(min = 1, max = 8) List<@DoubleRange(min = 0, max = 1_000) Double> sums,
                @ForAll @IntRange(min = 1, max = 50) int countPerBucket) {
            double totalSum = sums.stream().mapToDouble(Double::doubleValue).sum();
            long totalCount = (long) countPerBucket * sums.size();

            List<ResultFrame> buckets = sums.stream().map(sum -> ResultFrame.builder()
                    .column("SUM(x)", ColumnType.DOUBLE).column("COUNT(x)", ColumnType.LONG)
                    .row(sum, (long) countPerBucket).build()).toList();
            ResultFrame merged = TypedFrameAggregator.aggregate(buckets, List.of(),
                    Map.of("SUM(x)", AggregateFunction.SUM, "COUNT(x)", AggregateFunction.COUNT), "ts", 300);

            assertThat(merged.valueAt(0, 1)).isEqualTo(totalCount);
            assertThat(SparkAverage.average(merged.valueAt(0, 0), merged.valueAt(0, 1)))
                    .isCloseTo(totalSum / totalCount, org.assertj.core.data.Offset.offset(1e-9));
        }
    }

    @Nested
    class GlobalAggregate {

        private ResultFrame partial(String app, double sum, long count) {
            return ResultFrame.builder().column("appName", ColumnType.STRING)
                    .column("sum_bytes", ColumnType.DOUBLE).column("count_bytes", ColumnType.LONG)
                    .row(app, sum, count).build();
        }

        private ResultFrame merge(List<ResultFrame> partials, Map<String, AggregateFunction> functions) {
            return TypedFrameAggregator.aggregate(partials, List.of("appName"), functions, "ts", 300);
        }

        @Test
        void distinctBucketsOfOneGroupSumTheirPartials() {
            ResultFrame merged = merge(List.of(partial("netflix", 100, 2), partial("netflix", 50, 1)),
                    Map.of("sum_bytes", AggregateFunction.SUM, "count_bytes", AggregateFunction.COUNT));
            assertThat(merged.rowCount()).isEqualTo(1);
            assertThat(merged.valueAt(0, 1)).isEqualTo(150.0);
            assertThat(merged.valueAt(0, 2)).isEqualTo(3L);
        }

        @Test
        void minimumAndMaximumCombineCorrectlyAcrossPartials() {
            ResultFrame low = ResultFrame.builder().column("appName", ColumnType.STRING)
                    .column("min_latency", ColumnType.DOUBLE).column("max_latency", ColumnType.DOUBLE)
                    .row("netflix", 5.0, 9.0).build();
            ResultFrame high = ResultFrame.builder().column("appName", ColumnType.STRING)
                    .column("min_latency", ColumnType.DOUBLE).column("max_latency", ColumnType.DOUBLE)
                    .row("netflix", 3.0, 12.0).build();
            ResultFrame merged = merge(List.of(low, high),
                    Map.of("min_latency", AggregateFunction.MINIMUM, "max_latency", AggregateFunction.MAXIMUM));
            assertThat(merged.valueAt(0, 1)).isEqualTo(3.0);
            assertThat(merged.valueAt(0, 2)).isEqualTo(12.0);
        }

        @Test
        void aValidNaNIsDistinctFromAMissingMeasure() {
            ResultFrame frame = ResultFrame.builder().column("appName", ColumnType.STRING)
                    .column("min_latency", ColumnType.DOUBLE).column("max_latency", ColumnType.DOUBLE)
                    .row("netflix", null, null)
                    .row("netflix", Double.NaN, Double.NaN)
                    .row("netflix", 2.0, 2.0)
                    .build();
            ResultFrame merged = merge(List.of(frame),
                    Map.of("min_latency", AggregateFunction.MINIMUM, "max_latency", AggregateFunction.MAXIMUM));
            assertThat(merged.rowCount()).isEqualTo(1);
            assertThat(merged.valueAt(0, 1)).isEqualTo(2.0);
            assertThat((Double) merged.valueAt(0, 2)).isNaN();
        }

        @Test
        void distinctGroupsAreKeptSeparateAndDeterministicallyOrdered() {
            ResultFrame merged = merge(List.of(partial("youtube", 10, 1), partial("netflix", 20, 2)),
                    Map.of("sum_bytes", AggregateFunction.SUM, "count_bytes", AggregateFunction.COUNT));
            assertThat(merged.rowCount()).isEqualTo(2);
            assertThat(merged.valueAt(0, 0)).isEqualTo("netflix");
            assertThat(merged.valueAt(1, 0)).isEqualTo("youtube");
        }

        @Test
        void negativeAndPositiveZeroKeysFormOneGroupAsInSpark() {
            ResultFrame frame = ResultFrame.builder().column("rate", ColumnType.DOUBLE)
                    .column("SUM(x)", ColumnType.LONG)
                    .row(-0.0, 1L).row(0.0, 2L).build();
            ResultFrame merged = TypedFrameAggregator.aggregate(List.of(frame), List.of("rate"),
                    Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300);
            assertThat(merged.rowCount()).isEqualTo(1);
            assertThat(merged.valueAt(0, 0)).isEqualTo(0.0);
            assertThat(merged.valueAt(0, 1)).isEqualTo(3L);
        }
    }

    @Nested
    class TimeSeriesResampling {

        private ResultFrame series(long[] timestamps, Double[] values) {
            var builder = ResultFrame.builder().column("ts", ColumnType.LONG).column("appName", ColumnType.STRING)
                    .column("measure", ColumnType.DOUBLE);
            for (int index = 0; index < timestamps.length; index++) {
                builder.row(timestamps[index], "netflix", values[index]);
            }
            return builder.build();
        }

        private ResultFrame resample(ResultFrame frame, int step, AggregateFunction function) {
            return TypedFrameAggregator.aggregate(List.of(frame), List.of("ts", "appName"),
                    Map.of("measure", function), "ts", step);
        }

        @Test
        void rebucketingProducesEpochSecondBucketStartsAndNeverCollapsesToZero() {
            // RC5 guard, exactly the scenario named in TESTING_AND_AGENT_WORKFLOWS §1.8.
            long[] raw = {86_400, 86_700, 87_000, 87_300, 87_600, 87_900, 88_200};
            Double[] ones = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
            ResultFrame resampled = resample(series(raw, ones), 1900, AggregateFunction.SUM);
            List<Object> starts = java.util.stream.IntStream.range(0, resampled.rowCount())
                    .mapToObj(row -> resampled.valueAt(row, 0)).toList();
            assertThat(starts).contains(85_500L, 87_400L).doesNotContain(0L);
        }

        @Test
        void resamplingRollsUpFinerBucketsIntoTheCoarserUserStep() {
            ResultFrame resampled = resample(series(new long[] {0, 300, 600}, new Double[] {1.0, 1.0, 1.0}),
                    900, AggregateFunction.SUM);
            assertThat(resampled.rowCount()).isEqualTo(1);
            assertThat(resampled.valueAt(0, 0)).isEqualTo(0L);
            assertThat(resampled.valueAt(0, 2)).isEqualTo(3.0);
        }

        @Test
        void resamplingPreservesAValidNaNMeasure() {
            ResultFrame resampled = resample(series(new long[] {0, 300}, new Double[] {null, Double.NaN}),
                    600, AggregateFunction.MAXIMUM);
            assertThat(resampled.rowCount()).isEqualTo(1);
            assertThat((Double) resampled.valueAt(0, 2)).isNaN();
        }

        @Test
        void aUserStepNotCoarserThanTheStoredStepLeavesRowsUnchanged() {
            ResultFrame resampled = resample(series(new long[] {0, 300}, new Double[] {1.0, 2.0}),
                    300, AggregateFunction.SUM);
            assertThat(resampled.rowCount()).isEqualTo(2);
        }

        @Property
        void everyRebucketedStartIsAlignedToTheStep(
                @ForAll @Size(min = 1, max = 20) List<@net.jqwik.api.constraints.LongRange(min = 0, max = 10_000_000) Long> timestamps,
                @ForAll @IntRange(min = 60, max = 3_600) int step) {
            long[] raw = timestamps.stream().mapToLong(Long::longValue).toArray();
            Double[] values = new Double[raw.length];
            java.util.Arrays.fill(values, 1.0);
            ResultFrame resampled = resample(series(raw, values), step, AggregateFunction.SUM);
            for (int row = 0; row < resampled.rowCount(); row++) {
                assertThat((Long) resampled.valueAt(row, 0) % step).isZero();
            }
        }
    }
}
