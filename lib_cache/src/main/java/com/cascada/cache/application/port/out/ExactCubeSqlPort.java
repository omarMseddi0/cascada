package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.util.Set;
import java.util.Optional;

public interface ExactCubeSqlPort {
    Optional<ExactCubeQuery> recognize(String physicalSql);

    String pinnedStatement(ExactCubeQuery query, ExactCubeSource source);

    String stateStatement(ExactCubeQuery query, ExactCubeSource source, CubeInterval range, Set<CubeMeasure> measures);

    String pinnedBucketStatement(String sql, ExactCubeSource source);

    String outerStatement(ExactCubeQuery query, ExactCubeSource source);
}
