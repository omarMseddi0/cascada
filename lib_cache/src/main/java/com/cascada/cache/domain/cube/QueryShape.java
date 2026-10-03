package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.merge.AggregateFunction;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The "shape" of a query for cube subsumption (ARCHITECTURE §4): its group-by set, filter set, and
 * aggregate set. Two queries with the same shape hit the same cache entry; a coarser shape can answer
 * a finer one by roll-up and filter-down.
 */
public record QueryShape(Set<String> groupBy, Set<String> filters, Set<String> aggregates,
                         Set<String> sources, Map<String, AggregateFunction> outputAggregates,
                         List<String> projectionSignature) {

    public QueryShape {
        groupBy = Set.copyOf(groupBy);
        filters = Set.copyOf(filters);
        aggregates = Set.copyOf(aggregates);
        sources = Set.copyOf(sources);
        outputAggregates = Map.copyOf(outputAggregates);
        projectionSignature = List.copyOf(projectionSignature);
    }

    public QueryShape(Set<String> groupBy, Set<String> filters, Set<String> aggregates,
                      Set<String> sources, Map<String, AggregateFunction> outputAggregates) {
        this(groupBy, filters, aggregates, sources, outputAggregates, List.of());
    }

    public QueryShape(Set<String> groupBy, Set<String> filters, Set<String> aggregates) {
        this(groupBy, filters, aggregates, Set.of(), Map.of(), List.of());
    }
}
