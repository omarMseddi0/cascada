package com.cascada.cache.domain.merge;

import java.util.Locale;
import java.util.Optional;

/**
 * Aggregate identity. Partial COUNT values merge by addition in the application layer; AVERAGE is
 * stored as SUM, COUNT and input-type ingredients and rebuilt with Spark's result rules.
 */
public enum AggregateFunction {
    SUM, COUNT, MINIMUM, MAXIMUM, AVERAGE;

    /** The aggregate a SQL function name denotes ({@code SUM}, {@code COUNT}, {@code MIN}, {@code MAX}, {@code AVG}). */
    public static Optional<AggregateFunction> fromSqlName(String name) {
        return Optional.ofNullable(switch (name.toUpperCase(Locale.ROOT)) {
            case "SUM" -> SUM;
            case "COUNT" -> COUNT;
            case "MIN" -> MINIMUM;
            case "MAX" -> MAXIMUM;
            case "AVG" -> AVERAGE;
            default -> null;
        });
    }
}
