package com.cascada.cache.domain.merge;

/**
 * Column names for an expression over aggregates such as {@code SUM(a) / COUNT(b) AS ratio}: each
 * aggregate inside it is stored as its own term column, with Spark's {@code typeof} of that term.
 */
public final class CompositeIngredients {

    private CompositeIngredients() {
    }

    public static String termPrefix(String output) {
        return output + "__cascada_term";
    }

    public static String termColumn(String output, int term) {
        return termPrefix(output) + term;
    }

    public static String typeColumn(String termColumn) {
        return termColumn + "__cascada_type";
    }
}
