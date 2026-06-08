package com.cascada.cache.domain;

import java.util.List;
import java.util.Optional;

/**
 * The ORDER BY / LIMIT that must be applied <em>after</em> cached and freshly-computed
 * ingredients are merged — never before, or a {@code LIMIT 2} on a partial bucket would
 * drop rows that belong in the final answer (ported from {@code PostProcessing} in
 * {@code domain.py} and the deferral logic in {@code merging.apply_post_processing}).
 */
public record PostProcessing(Optional<Integer> limit, List<OrderByClause> orderBy) {

    public PostProcessing {
        limit.ifPresent(value -> {
            if (value < 0) {
                throw new IllegalArgumentException("limit must be non-negative, but was: " + value);
            }
        });
        orderBy = List.copyOf(orderBy);
    }

    public static PostProcessing none() {
        return new PostProcessing(Optional.empty(), List.of());
    }

    public boolean hasOrderBy() {
        return !orderBy.isEmpty();
    }

    public boolean hasLimit() {
        return limit.isPresent();
    }
}
