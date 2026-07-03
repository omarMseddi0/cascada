package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.merge.columnar.ColumnarHashAggregator;
import com.cascada.cache.domain.merge.columnar.ColumnarHashAggregator.GroupedResult;
import com.cascada.cache.domain.merge.columnar.DictionaryEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Resamples time-series ingredients stored at the fixed internal step up to the user's requested
 * step, ported from {@code TimeSeriesMergeStrategy._resample_to_user_step} in {@code merging.py}.
 * The roll-up itself runs on {@link ColumnarHashAggregator} — the shared vectorized group-by — so
 * this class only adapts row objects to columnar primitives and back.
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

    private final ColumnarHashAggregator aggregator = new ColumnarHashAggregator();

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
        if (rows.isEmpty()) {
            return List.of();
        }

        // Column layout: sorted union of dimension and measure names across all rows.
        TreeSet<String> dimensionNames = new TreeSet<>();
        TreeSet<String> measureNames = new TreeSet<>();
        for (TimeSeriesRow row : rows) {
            dimensionNames.addAll(row.dimensions().keySet());
            measureNames.addAll(row.measures().keySet());
        }
        String[] dimensions = dimensionNames.toArray(String[]::new);
        String[] measureColumns = measureNames.toArray(String[]::new);

        int rowCount = rows.size();
        DictionaryEncoder encoder = new DictionaryEncoder();
        long[] buckets = new long[rowCount];
        int[][] dimensionCodes = new int[dimensions.length][rowCount];
        double[][] measures = new double[measureColumns.length][rowCount];
        for (double[] column : measures) {
            Arrays.fill(column, Double.NaN);
        }
        for (int r = 0; r < rowCount; r++) {
            TimeSeriesRow row = rows.get(r);
            buckets[r] = Math.floorDiv(row.bucketStartSeconds(), userStepSeconds) * (long) userStepSeconds;
            for (int d = 0; d < dimensions.length; d++) {
                String value = row.dimensions().get(dimensions[d]);
                dimensionCodes[d][r] = value == null ? DictionaryEncoder.ABSENT : encoder.encode(value);
            }
            for (int m = 0; m < measureColumns.length; m++) {
                Double value = row.measures().get(measureColumns[m]);
                if (value != null) {
                    measures[m][r] = value;
                }
            }
        }

        AggregateFunction[] functions = new AggregateFunction[measureColumns.length];
        for (int m = 0; m < measureColumns.length; m++) {
            functions[m] = measureAggregations.getOrDefault(measureColumns[m], AggregateFunction.SUM);
        }

        GroupedResult grouped = aggregator.aggregate(rowCount, buckets, dimensionCodes, measures,
                functions, null);

        List<TimeSeriesRow> resampled = new ArrayList<>(grouped.groupCount());
        for (int g = 0; g < grouped.groupCount(); g++) {
            Map<String, String> groupDimensions = new TreeMap<>();
            for (int d = 0; d < dimensions.length; d++) {
                int code = grouped.dimensionCode(g, d);
                if (code != DictionaryEncoder.ABSENT) {
                    groupDimensions.put(dimensions[d], encoder.decode(code));
                }
            }
            Map<String, Double> groupMeasures = new TreeMap<>();
            for (int m = 0; m < measureColumns.length; m++) {
                double value = grouped.measureAccumulators()[m][g];
                if (!Double.isNaN(value)) {
                    groupMeasures.put(measureColumns[m], value);
                }
            }
            resampled.add(new TimeSeriesRow(grouped.groupBuckets()[g], groupDimensions, groupMeasures));
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
}
