package com.cascada.cache.domain.merge;

import java.util.List;

/**
 * Reconstructs {@code AVG} from separately-merged {@code SUM} and {@code COUNT} ingredients —
 * the single most important correctness rule in the cache (Root Cause 4 in
 * {@code CACHE_INCONSISTENCY_EXPLAINED.md}). Ported from {@code reconstruct_average_from...} and
 * {@code _reconstruct_avg_columns} in {@code merging.py}.
 *
 * <p>Averaging averages is wrong across buckets: day1 (sum=20,count=2) and day2 (sum=400,count=4)
 * give {@code 420/6 = 70}, NOT {@code (10+100)/2 = 55}. The engine therefore never stores {@code AVG}
 * directly; it stores {@code SUM} and {@code COUNT}, merges each additively, then divides.
 */
public final class AverageReconstructionService {

    /** {@code totalSum / totalCount}; a zero count yields {@code NaN} (the "no data" marker). */
    public double reconstructAverageFromStoredSumAndCount(double totalSum, long totalCount) {
        if (totalCount == 0) {
            return Double.NaN;
        }
        return totalSum / totalCount;
    }

    /**
     * Reconstructs an average across many per-bucket {@code (sum, count)} ingredients by first
     * summing each component (the only correct combination) and then dividing once at the end.
     */
    public double reconstructAverageAcrossBuckets(List<SumAndCount> perBucketIngredients) {
        double totalSum = 0.0;
        long totalCount = 0L;
        for (SumAndCount ingredient : perBucketIngredients) {
            totalSum += ingredient.sum();
            totalCount += ingredient.count();
        }
        return reconstructAverageFromStoredSumAndCount(totalSum, totalCount);
    }

    /** One bucket's contribution to an average: the partial sum and the partial row count. */
    public record SumAndCount(double sum, long count) {
        public SumAndCount {
            if (count < 0) {
                throw new IllegalArgumentException("count must be non-negative, but was: " + count);
            }
        }
    }
}
