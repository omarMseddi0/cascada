package com.cascada.cache.domain.merge;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Resamples time-series ingredients stored at the fixed internal step up to the user's requested
 * step, ported from {@code TimeSeriesMergeStrategy._resample_to_user_step} in {@code merging.py}.
 *
 * <p>It guards the time-bucketing root causes:
 * <ul>
 *   <li><b>RC2 (epoch alignment):</b> bucket starts are computed as {@code floor(ts/step)*step} on
 *       epoch-second longs, so non-standard steps such as 1900s align consistently regardless of any
 *       implicit calendar boundary;</li>
 *   <li><b>RC5 (precision collapse):</b> bucket starts are built directly as longs, never by dividing
 *       a nanosecond cast — so they never all collapse to zero;</li>
 *   <li><b>RC1 (step preservation):</b> when the requested step is finer than or equal to the fixed
 *       step there is nothing to roll up, and the input is returned unchanged.</li>
 * </ul>
 */
public final class TimeSeriesBucketResampler {

    /** RC5 / RC2 guard: floor each epoch-second timestamp to its requested-step bucket start. */
    public long[] rebucketToRequestedStepSeconds(long[] timestampsSeconds, int requestedStepSeconds) {
        if (requestedStepSeconds <= 0) {
            throw new IllegalArgumentException("requested step must be > 0, but was: " + requestedStepSeconds);
        }
        long[] bucketStarts = new long[timestampsSeconds.length];
        for (int index = 0; index < timestampsSeconds.length; index++) {
            long timestamp = timestampsSeconds[index];
            bucketStarts[index] = Math.floorDiv(timestamp, requestedStepSeconds) * (long) requestedStepSeconds;
        }
        return bucketStarts;
    }

    /**
     * Rolls up rows stored at {@code fixedStepSeconds} to {@code userStepSeconds}. If the user step
     * is not coarser than the fixed step, the rows are returned unchanged (RC1: never re-derive a
     * finer step). Otherwise rows are re-bucketed and their measures combined per the aggregate map.
     */
    public List<TimeSeriesRow> resampleToUserStep(List<TimeSeriesRow> rows, int fixedStepSeconds,
                                                  int userStepSeconds,
                                                  Map<String, AggregateFunction> measureAggregations) {
        if (userStepSeconds <= fixedStepSeconds) {
            return List.copyOf(rows);
        }

        Map<BucketGroupKey, Map<String, Double>> accumulator = new LinkedHashMap<>();
        for (TimeSeriesRow row : rows) {
            long newBucketStart = Math.floorDiv(row.bucketStartSeconds(), userStepSeconds) * (long) userStepSeconds;
            BucketGroupKey groupKey = new BucketGroupKey(newBucketStart, new TreeMap<>(row.dimensions()));
            Map<String, Double> combined = accumulator.computeIfAbsent(groupKey, key -> new TreeMap<>());
            for (Map.Entry<String, Double> measureEntry : row.measures().entrySet()) {
                AggregateFunction function =
                        measureAggregations.getOrDefault(measureEntry.getKey(), AggregateFunction.SUM);
                combined.merge(measureEntry.getKey(), measureEntry.getValue(), function::combine);
            }
        }

        List<TimeSeriesRow> resampled = new ArrayList<>(accumulator.size());
        for (Map.Entry<BucketGroupKey, Map<String, Double>> entry : accumulator.entrySet()) {
            resampled.add(new TimeSeriesRow(entry.getKey().bucketStart(), entry.getKey().dimensions(),
                    entry.getValue()));
        }
        resampled.sort(Comparator.comparingLong(TimeSeriesRow::bucketStartSeconds)
                .thenComparing(row -> row.dimensions().toString()));
        return resampled;
    }

    /** A time-series row keyed by its bucket start plus dimension values, holding numeric measures. */
    public record TimeSeriesRow(long bucketStartSeconds, Map<String, String> dimensions,
                                Map<String, Double> measures) {
        public TimeSeriesRow {
            dimensions = new TreeMap<>(dimensions);
            measures = new TreeMap<>(measures);
        }
    }

    private record BucketGroupKey(long bucketStart, Map<String, String> dimensions) {
    }
}
