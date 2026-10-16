package com.cascada.cache.domain.merge;

import java.util.Locale;

/**
 * Column names of the ingredients a cached {@code AVG(x) AS alias} is stored as: the SUM and the
 * COUNT of x, and Spark's {@code typeof(x)}, which fixes the result type and decimal rounding.
 * Every internal ingredient name the cache generates is reserved, so a user alias can never collide
 * with one.
 */
public final class AverageIngredients {

    /** Prefix of every name the cache generates for its own ingredient columns. */
    public static final String RESERVED_PREFIX = "__cascada_";
    private static final String PREFIX = RESERVED_PREFIX + "avg_";

    private AverageIngredients() {
    }

    public static String sumColumn(String alias) {
        return alias + PREFIX + "sum";
    }

    public static String countColumn(String alias) {
        return alias + PREFIX + "count";
    }

    public static String typeColumn(String alias) {
        return alias + PREFIX + "type";
    }

    /** True for a name the cache reserves for its own ingredient columns. */
    public static boolean isReservedName(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.startsWith(RESERVED_PREFIX) || lower.contains(PREFIX);
    }
}
