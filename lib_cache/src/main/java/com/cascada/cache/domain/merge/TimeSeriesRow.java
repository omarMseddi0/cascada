package com.cascada.cache.domain.merge;

import java.util.Map;
import java.util.TreeMap;

public record TimeSeriesRow(long bucketStartSeconds, Map<String, String> dimensions,
                            Map<String, Double> measures) {
    public TimeSeriesRow {
        dimensions = new TreeMap<>(dimensions);
        measures = new TreeMap<>(measures);
    }
}
