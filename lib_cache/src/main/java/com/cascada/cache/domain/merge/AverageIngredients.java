package com.cascada.cache.domain.merge;

/**
 * Column names of the ingredients a cached {@code AVG(x) AS alias} is stored as: the SUM and the
 * COUNT of x, and Spark's {@code typeof(x)}, which fixes the result type and decimal rounding.
 */
public final class AverageIngredients {

    private static final String PREFIX = "__cascada_avg_";

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
}
