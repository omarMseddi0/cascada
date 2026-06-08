package com.cascada.cache.domain.cube;

import java.util.Set;

/**
 * The "shape" of a query for cube subsumption (ARCHITECTURE §4): its group-by set, filter set, and
 * aggregate set. Two queries with the same shape hit the same cache entry; a coarser shape can answer
 * a finer one by roll-up and filter-down.
 */
public record QueryShape(Set<String> groupBy, Set<String> filters, Set<String> aggregates) {

    public QueryShape {
        groupBy = Set.copyOf(groupBy);
        filters = Set.copyOf(filters);
        aggregates = Set.copyOf(aggregates);
    }
}
