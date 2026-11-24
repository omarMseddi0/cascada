package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

public interface ExactCubeStorePort {
    List<ExactCubeFragment> candidates(CubePopulation population, CubeInterval interval,
                                       List<String> grain, Set<CubeMeasure> measures);

    /** Look up candidate buckets in disjoint time ranges. */
    default List<ExactCubeFragment> candidates(CubePopulation population, List<CubeInterval> intervals,
                                               List<String> grain, Set<CubeMeasure> measures) {
        return intervals.stream().flatMap(interval -> candidates(population, interval, grain, measures).stream()).toList();
    }

    void visitBuckets(List<ExactCubeFragment> manifests, BiConsumer<Integer, Optional<ResultFrame>> visitor);

    /** Atomically writes the ordinary bucket and adds metadata that references that same value. */
    boolean storeBucket(String key, ResultFrame frame, Duration ttl, String generation, CubePopulation population,
                        CubeInterval interval, List<String> grain, String averageAlias);
}
