package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.service.merge.columnar.ColumnarHashAggregator;
import com.cascada.cache.application.service.merge.columnar.DictionaryEncoder;
import com.cascada.cache.application.service.merge.columnar.GroupedResult;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AggregationRow;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Map-reduce merge for non-time-series queries, ported from {@code GlobalAggregateMergeStrategy} in
 * {@code merging.py}. It concatenates partial results from cached buckets and the Spark gap, then
 * groups by the dimension key and combines each measure with its declared aggregate function
 * ({@code SUM}/{@code COUNT}→sum, {@code MIN}→min, {@code MAX}→max).
 *
 * <p>Since the columnar rewrite this class is a thin row-object adapter over
 * {@link ColumnarHashAggregator}, while typed result frames use {@link TypedFrameAggregator},
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
            RowColumnMapper.encodeDimensions(inputRow.dimensions(), dimensions, dimensionCodes, encoder, rowIndex);
            RowColumnMapper.encodeMeasures(inputRow.measures(), measureColumns, measures, measurePresent, rowIndex);
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
            Map<String, String> groupDimensions = RowColumnMapper.decodeDimensions(grouped, groupIndex, dimensions, encoder);
            Map<String, Double> groupMeasures = RowColumnMapper.decodeMeasures(grouped, groupIndex, measureColumns);
            merged.add(new AggregationRow(groupDimensions, groupMeasures));
        }
        merged.sort(Comparator.comparing(aggregationRow -> aggregationRow.groupingKey().toString()));
        return merged;
    }
}
