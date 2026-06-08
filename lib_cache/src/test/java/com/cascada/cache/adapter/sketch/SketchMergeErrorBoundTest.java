package com.cascada.cache.adapter.sketch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Error-bound golden tests for the sketch path (TESTING §1.5): merged HLL distinct counts stay within
 * ~2% of exact, and merged KLL quantiles stay near the true quantile — across day-bucket merges, the
 * thing exact aggregates cannot do.
 */
class SketchMergeErrorBoundTest {

    @Test
    void hyperLogLogDistinctCountAcrossMergedDaysIsWithinTwoPercent() {
        int trueCardinality = 200_000;
        // Three per-day sketches, each seeing a disjoint third of the universe.
        List<HyperLogLogDistinctCounter> perDay = new ArrayList<>();
        for (int day = 0; day < 3; day++) {
            HyperLogLogDistinctCounter counter = new HyperLogLogDistinctCounter();
            for (int value = day; value < trueCardinality; value += 3) {
                counter.add("subscriber-" + value);
            }
            perDay.add(counter);
        }
        double estimate = HyperLogLogDistinctCounter.unionOf(perDay).estimateDistinctCount();
        double relativeError = Math.abs(estimate - trueCardinality) / trueCardinality;
        assertThat(relativeError).isLessThanOrEqualTo(0.02);
    }

    @Test
    void kllMedianAcrossMergedDaysIsCloseToTheTrueMedian() {
        // Values 1..30000 spread across three day sketches; true median ~ 15000.
        List<KllQuantileEstimator> perDay = new ArrayList<>();
        for (int day = 0; day < 3; day++) {
            perDay.add(new KllQuantileEstimator());
        }
        for (int value = 1; value <= 30_000; value++) {
            perDay.get(value % 3).add(value);
        }
        double median = KllQuantileEstimator.mergeOf(perDay).quantile(0.5);
        assertThat(median).isBetween(15_000 * 0.97, 15_000 * 1.03);
    }

    @Test
    void kllPercentilesAreMonotonicAfterMerge() {
        List<KllQuantileEstimator> perDay = new ArrayList<>();
        KllQuantileEstimator single = new KllQuantileEstimator();
        for (int value = 1; value <= 100_000; value++) {
            single.add(value);
        }
        perDay.add(single);
        KllQuantileEstimator merged = KllQuantileEstimator.mergeOf(perDay);
        assertThat(merged.quantile(0.5)).isLessThan(merged.quantile(0.9));
        assertThat(merged.quantile(0.9)).isLessThan(merged.quantile(0.99));
    }
}
