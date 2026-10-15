package com.cascada.app.config;

import java.util.Set;

/**
 * Operator-declared cache guardrails: columns whose GROUP BY would cache near-unique rows, filter columns
 * Delta data skipping already makes fast, an optional minimum cacheable window ({@code null} means no
 * minimum; 0 or -1 disable caching), and the largest bucket frame worth storing.
 */
public record CacheSafetySettings(Set<String> highCardinalityColumns, Set<String> liquidClusteredFilterColumns,
                                  Long minimumCacheableTimeRangeSeconds, int maximumCachedRows) {

    public static final int DEFAULT_MAXIMUM_CACHED_ROWS = 50_000;

    public CacheSafetySettings {
        highCardinalityColumns = Set.copyOf(highCardinalityColumns);
        liquidClusteredFilterColumns = Set.copyOf(liquidClusteredFilterColumns);
        if (maximumCachedRows < 0) throw new IllegalArgumentException("maximumCachedRows must not be negative");
    }

    public static CacheSafetySettings defaults() {
        return new CacheSafetySettings(Set.of(), Set.of(), null, DEFAULT_MAXIMUM_CACHED_ROWS);
    }
}
