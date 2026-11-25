package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.util.List;
import java.util.Optional;

/** Ordinary current-data execution and metadata used to validate bucket reuse. */
public interface LatestQueryExecutorPort extends QueryExecutorPort {
    Optional<ExactCubeSource> currentSource(String registeredPath, String accessScope);

    default ResultFrame executeWithAggregate(String physicalSql, String relation, ResultFrame frame, List<String> types) {
        throw new UnsupportedOperationException("executor does not support typed aggregate relations");
    }
}
