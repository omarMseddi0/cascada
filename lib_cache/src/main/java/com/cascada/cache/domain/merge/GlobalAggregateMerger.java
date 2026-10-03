package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.merge.columnar.ColumnarHashAggregator;
import com.cascada.cache.domain.merge.columnar.ColumnarHashAggregator.GroupedResult;
import com.cascada.cache.domain.merge.columnar.DictionaryEncoder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Map-reduce merge for non-time-series queries, ported from {@code GlobalAggregateMergeStrategy} in
 * {@code merging.py}. It concatenates partial results from cached buckets and the Spark gap, then
 * groups by the dimension key and combines each measure with its declared aggregate function
 * ({@code SUM}/{@code COUNT}→sum, {@code MIN}→min, {@code MAX}→max).
 *
 * <p>Since the columnar rewrite this class is a thin row-object adapter over
 * {@link ColumnarHashAggregator} — the single vectorized group-by operator all merge paths share —
 * so the row-level API (and its tests) stay stable while the hot loop runs on primitive arrays.
 *
 * <p>It guards two of the documented root causes:
 * <ul>
 *   <li><b>RC3 (duplicate inflation):</b> exact-duplicate rows (identical key <em>and</em> measures)
 *       are dropped before any summing, so a body day fetched from both cache and Spark is never
 *       double-counted;</li>
 *   <li><b>RC4 (AVG):</b> {@code COUNT} columns merge additively here, and {@link AverageReconstructionService}
 *       divides only at the very end.</li>
 * </ul>
 */
public final class GlobalAggregateMerger {

    private final ColumnarHashAggregator aggregator = new ColumnarHashAggregator();

    /**
     * @param rows                 partial rows from every source (cache + gap)
     * @param measureAggregations  how each measure column combines; columns absent from the map
     *                             default to {@link AggregateFunction#SUM} (the Python default)
     * @param dropExactDuplicateRows whether to collapse byte-identical duplicate rows first (RC3)
     */
    public List<AggregationRow> merge(List<AggregationRow> rows,
                                      Map<String, AggregateFunction> measureAggregations,
                                      boolean dropExactDuplicateRows) {
        if (rows.isEmpty()) {
            return List.of();
        }

        // Column layout: the sorted union of dimension and measure names across all partial rows.
        TreeSet<String> dimensionNames = new TreeSet<>();
        TreeSet<String> measureNames = new TreeSet<>();
        for (AggregationRow row : rows) {
            dimensionNames.addAll(row.dimensions().keySet());
            measureNames.addAll(row.measures().keySet());
        }
        String[] dimensions = dimensionNames.toArray(String[]::new);
        String[] measureColumns = measureNames.toArray(String[]::new);

        // Encode rows and carry presence separately so a valid NaN is not treated as missing.
        int rowCount = rows.size();
        DictionaryEncoder encoder = new DictionaryEncoder();
        int[][] dimensionCodes = new int[dimensions.length][rowCount];
        double[][] measures = new double[measureColumns.length][rowCount];
        boolean[][] measurePresent = new boolean[measureColumns.length][rowCount];
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            AggregationRow inputRow = rows.get(rowIndex);
            for (int dimensionIndex = 0; dimensionIndex < dimensions.length; dimensionIndex++) {
                String value = inputRow.dimensions().get(dimensions[dimensionIndex]);
                dimensionCodes[dimensionIndex][rowIndex] = value == null
                        ? DictionaryEncoder.ABSENT : encoder.encode(value);
            }
            for (int measureIndex = 0; measureIndex < measureColumns.length; measureIndex++) {
                Double value = inputRow.measures().get(measureColumns[measureIndex]);
                if (value != null) {
                    measures[measureIndex][rowIndex] = value;
                    measurePresent[measureIndex][rowIndex] = true;
                }
            }
        }

        AggregateFunction[] functions = new AggregateFunction[measureColumns.length];
        for (int measureIndex = 0; measureIndex < measureColumns.length; measureIndex++) {
            functions[measureIndex] = measureAggregations.getOrDefault(measureColumns[measureIndex],
                    AggregateFunction.SUM);
        }

        boolean[] keepMask = dropExactDuplicateRows
                ? aggregator.deduplicateExactRows(rowCount, null, dimensionCodes, measures, measurePresent)
                : null;
        GroupedResult grouped = aggregator.aggregate(rowCount, null, dimensionCodes, measures,
                functions, keepMask, measurePresent);

        // Rebuild row objects and sort deterministically by the canonical grouping-key string.
        List<AggregationRow> merged = new ArrayList<>(grouped.groupCount());
        for (int groupIndex = 0; groupIndex < grouped.groupCount(); groupIndex++) {
            Map<String, String> groupDimensions = new TreeMap<>();
            for (int dimensionIndex = 0; dimensionIndex < dimensions.length; dimensionIndex++) {
                int code = grouped.dimensionCode(groupIndex, dimensionIndex);
                if (code != DictionaryEncoder.ABSENT) {
                    groupDimensions.put(dimensions[dimensionIndex], encoder.decode(code));
                }
            }
            Map<String, Double> groupMeasures = new TreeMap<>();
            for (int measureIndex = 0; measureIndex < measureColumns.length; measureIndex++) {
                double value = grouped.measureAccumulators()[measureIndex][groupIndex];
                if (grouped.measurePresent()[measureIndex][groupIndex]) {
                    groupMeasures.put(measureColumns[measureIndex], value);
                }
            }
            merged.add(new AggregationRow(groupDimensions, groupMeasures));
        }
        merged.sort(Comparator.comparing(aggregationRow -> aggregationRow.groupingKey().toString()));
        return merged;
    }
}
