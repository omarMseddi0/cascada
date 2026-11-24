package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.List;
import java.util.Optional;

/** Delta admission and version-bound execution for registered sources. */
public interface PinnedQueryExecutorPort extends QueryExecutorPort {
    Optional<ExactCubeSource> bind(String registeredPath, String accessScope);

    ResultFrame executePinned(ExactCubeSource binding, String physicalSql);

    default ResultFrame executeWithAggregate(ExactCubeSource binding, String physicalSql, String relation,
                                             ResultFrame frame, List<String> types) {
        throw new UnsupportedOperationException("executor does not support typed aggregate relations");
    }
}
