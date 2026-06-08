package com.cascada.cache.domain.merge;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Map-reduce merge for non-time-series queries, ported from {@code GlobalAggregateMergeStrategy} in
 * {@code merging.py}. It concatenates partial results from cached buckets and the Spark gap, then
 * groups by the dimension key and combines each measure with its declared aggregate function
 * ({@code SUM}/{@code COUNT}→sum, {@code MIN}→min, {@code MAX}→max).
 *
 * <p>It guards two of the documented root causes:
 * <ul>
 *   <li><b>RC3 (duplicate inflation):</b> {@code dropExactDuplicateRows} removes rows that are
 *       byte-identical in both key and measures — the danger when the same body day is fetched from
 *       <em>both</em> cache and Spark — before any summing, so nothing is double-counted;</li>
 *   <li><b>RC4 (AVG):</b> {@code COUNT} columns merge additively here, and {@link AverageReconstructionService}
 *       divides only at the very end.</li>
 * </ul>
 */
public final class GlobalAggregateMerger {

    /**
     * @param rows                 partial rows from every source (cache + gap)
     * @param measureAggregations  how each measure column combines; columns absent from the map
     *                             default to {@link AggregateFunction#SUM} (the Python default)
     * @param dropExactDuplicateRows whether to collapse byte-identical duplicate rows first (RC3)
     */
    public List<AggregationRow> merge(List<AggregationRow> rows,
                                      Map<String, AggregateFunction> measureAggregations,
                                      boolean dropExactDuplicateRows) {
        List<AggregationRow> effectiveRows = dropExactDuplicateRows ? rows.stream().distinct().toList() : rows;

        // Preserve first-seen group order, then sort deterministically at the end.
        Map<Map<String, String>, Map<String, Double>> accumulator = new LinkedHashMap<>();
        for (AggregationRow row : effectiveRows) {
            Map<String, Double> combined = accumulator.computeIfAbsent(row.groupingKey(), key -> new TreeMap<>());
            for (Map.Entry<String, Double> measureEntry : row.measures().entrySet()) {
                String measureName = measureEntry.getKey();
                double incoming = measureEntry.getValue();
                AggregateFunction function = measureAggregations.getOrDefault(measureName, AggregateFunction.SUM);
                combined.merge(measureName, incoming, function::combine);
            }
        }

        List<AggregationRow> merged = new ArrayList<>(accumulator.size());
        for (Map.Entry<Map<String, String>, Map<String, Double>> entry : accumulator.entrySet()) {
            merged.add(new AggregationRow(entry.getKey(), entry.getValue()));
        }
        merged.sort(Comparator.comparing(row -> row.groupingKey().toString()));
        return merged;
    }
}
