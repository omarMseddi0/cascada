package com.cascada.cache.domain.model.cube;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** A restricted, source-local aggregate recognized in translated SQL. */
public record ExactCubeQuery(String path, String timeColumn, CubeInterval interval, String predicate,
                             Map<String, String> predicateTypes, List<CubeKeyFilter> keyFilters,
                             List<String> grain, Set<String> groupAliases, String input, Set<CubeMeasure> measures,
                             List<CubeProjection> projections, List<CubeSort> order, Integer limit,
                             String outerSql, String outerRelation, String pinnedSqlTemplate, String sourceToken) {
    public ExactCubeQuery {
        predicateTypes = Map.copyOf(predicateTypes);
        keyFilters = List.copyOf(keyFilters);
        grain = List.copyOf(grain);
        groupAliases = Set.copyOf(groupAliases);
        measures = Set.copyOf(measures);
        projections = List.copyOf(projections);
        order = List.copyOf(order);
    }

}
