package com.cascada.cache.domain.merge;

import java.util.Map;
import java.util.TreeMap;

/**
 * One row of a partial or merged aggregation result: a set of dimension values (the GROUP BY key)
 * and a set of named numeric measures (the stored ingredients such as {@code sum_bytes},
 * {@code count_bytes}).
 *
 * <p>This is the framework-free stand-in for a pandas/Tablesaw row used by the domain merge math, so
 * the correctness of the merge can be unit-tested without a dataframe library on the classpath.
 * Dimensions are kept in a {@link TreeMap} so the grouping key is order-independent.
 */
public record AggregationRow(Map<String, String> dimensions, Map<String, Double> measures) {

    public AggregationRow {
        dimensions = new TreeMap<>(dimensions);
        measures = new TreeMap<>(measures);
    }

    /** The canonical grouping key: dimension values in sorted-key order. */
    public Map<String, String> groupingKey() {
        return dimensions;
    }

    public double measure(String name) {
        Double value = measures.get(name);
        if (value == null) {
            throw new IllegalArgumentException("no measure named '" + name + "' in row " + this);
        }
        return value;
    }
}
