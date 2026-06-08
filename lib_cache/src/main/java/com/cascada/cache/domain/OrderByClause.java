package com.cascada.cache.domain;

import java.util.Optional;

/**
 * One deferred ORDER BY term applied after the cache merge (ported from the list form of
 * {@code post_processing["order_by"]} in {@code merging.apply_post_processing}).
 *
 * <p>Either a concrete {@code column} or a simple arithmetic {@code expression} is set.
 * {@code nullsFirst} controls null placement, matching the pandas {@code na_position} behaviour.
 */
public record OrderByClause(Optional<String> column, Optional<String> expression, boolean ascending,
                            boolean nullsFirst) {

    public OrderByClause {
        if (column.isEmpty() && expression.isEmpty()) {
            throw new IllegalArgumentException("order-by clause must carry either a column or an expression");
        }
    }

    public static OrderByClause forColumn(String column, boolean ascending, boolean nullsFirst) {
        return new OrderByClause(Optional.of(column), Optional.empty(), ascending, nullsFirst);
    }

    public static OrderByClause forExpression(String expression, boolean ascending, boolean nullsFirst) {
        return new OrderByClause(Optional.empty(), Optional.of(expression), ascending, nullsFirst);
    }
}
