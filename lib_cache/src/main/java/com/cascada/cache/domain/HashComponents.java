package com.cascada.cache.domain;

import java.util.List;

/**
 * The time-independent "DNA" of a query that feeds the logic hash: the group-by columns,
 * the aggregate expressions, the filter clauses, and the fixed step.
 *
 * <p>Ported from the {@code HashComponents} dataclass in {@code domain.py}. The lists are
 * defensively copied and the canonicalizer is expected to have already sorted and de-duplicated
 * them, so that reordering group-by columns or filters does not change the resulting hash
 * (ARCHITECTURE §"Hashing determinism").
 */
public record HashComponents(List<String> groupBy, List<String> aggregates, List<String> filters, int step) {

    public HashComponents {
        groupBy = List.copyOf(groupBy);
        aggregates = List.copyOf(aggregates);
        filters = List.copyOf(filters);
    }

    public static HashComponents of(List<String> groupBy, List<String> aggregates, List<String> filters) {
        return new HashComponents(groupBy, aggregates, filters, 0);
    }

    public HashComponents withStep(int newStep) {
        return new HashComponents(groupBy, aggregates, filters, newStep);
    }
}
