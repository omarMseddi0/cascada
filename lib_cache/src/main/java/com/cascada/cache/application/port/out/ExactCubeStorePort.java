package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface ExactCubeStorePort {
    List<ExactCubeFragment> candidates(CubePopulation population, CubeInterval interval, int limit);

    Optional<ResultFrame> read(ExactCubeFragment manifest);

    void publish(CubePopulation population, CubeInterval interval, List<String> grain, Set<CubeMeasure> measures,
                 ResultFrame state, Duration ttl, String publicationFence);

    String publicationFence();
}
