package com.cascada.sql.canonical;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The set of physical column names treated as the time dimension, generalising the Python
 * {@code TIME_COLUMN_NAMES = {ts, starttime, stoptime, timestamp}} from {@code data_cache_adapter.py}.
 *
 * <p>Any domain can supply its own set, so the cache's time-series detection is not hard-wired to one
 * microservice's schema (the "general, not custom" mandate, plan §8.9).
 */
public record TimeDimensionMap(Set<String> physicalTimeColumns) {

    public TimeDimensionMap {
        physicalTimeColumns = physicalTimeColumns.stream()
                .map(name -> name.toLowerCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    public static TimeDimensionMap defaults() {
        return new TimeDimensionMap(Set.of("ts", "starttime", "stoptime", "timestamp"));
    }

    public boolean isTimeColumn(String columnName) {
        return columnName != null && physicalTimeColumns.contains(columnName.toLowerCase(Locale.ROOT));
    }
}
