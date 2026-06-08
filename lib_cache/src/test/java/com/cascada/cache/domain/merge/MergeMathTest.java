package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.merge.AverageReconstructionService.SumAndCount;
import com.cascada.cache.domain.merge.TimeSeriesBucketResampler.TimeSeriesRow;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The correctness gate at the pure-math level: the cache path must do the same arithmetic as a
 * direct compute. These tests encode the named root causes from {@code CACHE_INCONSISTENCY_EXPLAINED.md}.
 */
class MergeMathTest {

    @Nested
    class AverageReconstruction {

        private final AverageReconstructionService service = new AverageReconstructionService();

        @Test
        void reconstructsAverageFromSummedSumAndCountNotByAveragingAverages() {
            // day1: sum=20 count=2 ; day2: sum=400 count=4 -> 420/6 = 70 (NOT (10+100)/2 = 55).
            double reconstructed = service.reconstructAverageFromStoredSumAndCount(20 + 400, 2 + 4);
            assertThat(reconstructed).isEqualTo(70.0);
        }

        @Test
        void reconstructAcrossBucketsMatchesTheScalarForm() {
            double across = service.reconstructAverageAcrossBuckets(
                    List.of(new SumAndCount(20, 2), new SumAndCount(400, 4)));
            assertThat(across).isEqualTo(70.0);
        }

        @Test
        void zeroCountYieldsNaN() {
            assertThat(service.reconstructAverageFromStoredSumAndCount(0, 0)).isNaN();
        }

        @Property
        void perBucketReconstructionEqualsGlobalReconstruction(
                @ForAll @Size(min = 1, max = 8) List<@DoubleRange(min = 0, max = 1_000) Double> sums,
                @ForAll @IntRange(min = 1, max = 50) int countPerBucket) {
            double totalSum = sums.stream().mapToDouble(Double::doubleValue).sum();
            long totalCount = (long) countPerBucket * sums.size();
            double expected = totalSum / totalCount;

            List<SumAndCount> ingredients = sums.stream()
                    .map(sum -> new SumAndCount(sum, countPerBucket))
                    .toList();
            assertThat(service.reconstructAverageAcrossBuckets(ingredients)).isCloseTo(expected, within());
        }

        private org.assertj.core.data.Offset<Double> within() {
            return org.assertj.core.data.Offset.offset(1e-9);
        }
    }

    @Nested
    class GlobalAggregate {

        private final GlobalAggregateMerger merger = new GlobalAggregateMerger();

        private AggregationRow row(String app, double sum, double count) {
            return new AggregationRow(Map.of("appName", app), Map.of("sum_bytes", sum, "count_bytes", count));
        }

        @Test
        void exactDuplicateRowsFromCacheAndSparkAreCollapsedBeforeAggregation() {
            // RC3: the same physical row arriving from both sources must not be summed twice.
            List<AggregationRow> rows = List.of(row("netflix", 100, 2), row("netflix", 100, 2));
            List<AggregationRow> merged = merger.merge(rows,
                    Map.of("sum_bytes", AggregateFunction.SUM, "count_bytes", AggregateFunction.COUNT), true);

            assertThat(merged).hasSize(1);
            assertThat(merged.get(0).measure("sum_bytes")).isEqualTo(100.0);
            assertThat(merged.get(0).measure("count_bytes")).isEqualTo(2.0);
        }

        @Test
        void withoutDedupGenuineDistinctRowsStillSum() {
            List<AggregationRow> rows = List.of(row("netflix", 100, 2), row("netflix", 50, 1));
            List<AggregationRow> merged = merger.merge(rows,
                    Map.of("sum_bytes", AggregateFunction.SUM, "count_bytes", AggregateFunction.COUNT), true);
            assertThat(merged).hasSize(1);
            assertThat(merged.get(0).measure("sum_bytes")).isEqualTo(150.0);
            assertThat(merged.get(0).measure("count_bytes")).isEqualTo(3.0);
        }

        @Test
        void minimumAndMaximumCombineCorrectlyAcrossPartials() {
            AggregationRow low = new AggregationRow(Map.of("appName", "netflix"),
                    Map.of("min_latency", 5.0, "max_latency", 9.0));
            AggregationRow high = new AggregationRow(Map.of("appName", "netflix"),
                    Map.of("min_latency", 3.0, "max_latency", 12.0));
            List<AggregationRow> merged = merger.merge(List.of(low, high),
                    Map.of("min_latency", AggregateFunction.MINIMUM, "max_latency", AggregateFunction.MAXIMUM), false);
            assertThat(merged).hasSize(1);
            assertThat(merged.get(0).measure("min_latency")).isEqualTo(3.0);
            assertThat(merged.get(0).measure("max_latency")).isEqualTo(12.0);
        }

        @Test
        void distinctGroupsAreKeptSeparateAndDeterministicallyOrdered() {
            List<AggregationRow> merged = merger.merge(
                    List.of(row("youtube", 10, 1), row("netflix", 20, 2)),
                    Map.of("sum_bytes", AggregateFunction.SUM, "count_bytes", AggregateFunction.COUNT), false);
            assertThat(merged).hasSize(2);
            assertThat(merged.get(0).dimensions().get("appName")).isEqualTo("netflix");
            assertThat(merged.get(1).dimensions().get("appName")).isEqualTo("youtube");
        }
    }

    @Nested
    class TimeSeriesResampling {

        private final TimeSeriesBucketResampler resampler = new TimeSeriesBucketResampler();

        @Test
        void rebucketingProducesEpochSecondBucketStartsAndNeverCollapsesToZero() {
            // RC5 guard, exactly the scenario named in TESTING_AND_AGENT_WORKFLOWS §1.8.
            long[] raw = {86_400, 86_700, 87_000, 87_300, 87_600, 87_900, 88_200};
            long[] bucketStarts = resampler.rebucketToRequestedStepSeconds(raw, 1900);
            assertThat(bucketStarts).contains(85_500L, 87_400L);
            assertThat(bucketStarts).doesNotContain(0L);
        }

        @Test
        void resamplingRollsUpFinerBucketsIntoTheCoarserUserStep() {
            List<TimeSeriesRow> rows = List.of(
                    new TimeSeriesRow(0, Map.of("appName", "netflix"), Map.of("sum_bytes", 1.0)),
                    new TimeSeriesRow(300, Map.of("appName", "netflix"), Map.of("sum_bytes", 1.0)),
                    new TimeSeriesRow(600, Map.of("appName", "netflix"), Map.of("sum_bytes", 1.0)));
            List<TimeSeriesRow> resampled = resampler.resampleToUserStep(rows, 300, 900,
                    Map.of("sum_bytes", AggregateFunction.SUM));
            assertThat(resampled).hasSize(1);
            assertThat(resampled.get(0).bucketStartSeconds()).isZero();
            assertThat(resampled.get(0).measures().get("sum_bytes")).isEqualTo(3.0);
        }

        @Test
        void aUserStepNotCoarserThanTheFixedStepLeavesRowsUnchanged() {
            List<TimeSeriesRow> rows = List.of(
                    new TimeSeriesRow(0, Map.of("appName", "netflix"), Map.of("sum_bytes", 1.0)),
                    new TimeSeriesRow(300, Map.of("appName", "netflix"), Map.of("sum_bytes", 2.0)));
            assertThat(resampler.resampleToUserStep(rows, 300, 300, Map.of("sum_bytes", AggregateFunction.SUM)))
                    .hasSize(2);
        }

        @Property
        void everyRebucketedStartIsAlignedToTheStep(
                @ForAll @Size(min = 1, max = 20) List<@net.jqwik.api.constraints.LongRange(min = 0, max = 10_000_000) Long> timestamps,
                @ForAll @IntRange(min = 60, max = 3_600) int step) {
            long[] raw = timestamps.stream().mapToLong(Long::longValue).toArray();
            for (long bucketStart : resampler.rebucketToRequestedStepSeconds(raw, step)) {
                assertThat(bucketStart % step).isZero();
            }
        }
    }
}
