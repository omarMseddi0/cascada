package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.util.Optional;

/** Ordinary latest reads and current source metadata for exact cube reuse. */
public interface LatestQueryExecutorPort extends QueryExecutorPort {
    Optional<ExactCubeSource> currentSource(String registeredPath, String accessScope);
}
