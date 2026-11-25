package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import java.util.Set;
import java.util.Optional;

public interface ExactCubeSqlPort {
    Optional<ExactCubeQuery> recognize(String physicalSql);

    String stateStatement(ExactCubeQuery query, CubeInterval range, Set<CubeMeasure> measures);
}
