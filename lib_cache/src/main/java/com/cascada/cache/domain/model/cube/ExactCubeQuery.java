package com.cascada.cache.domain.model.cube;

import java.util.List;
import java.util.Set;

/** A restricted, source-local aggregate recognized in translated SQL. */
public record ExactCubeQuery(String path, String timeColumn, CubeInterval interval, String predicate,
                             List<String> grain, String input, Set<CubeMeasure> measures,
                             List<CubeProjection> projections, List<CubeSort> order, Integer limit,
                             String outerSql, String outerRelation) {
    public ExactCubeQuery {
        grain = List.copyOf(grain);
        measures = Set.copyOf(measures);
        projections = List.copyOf(projections);
        order = List.copyOf(order);
    }

}
