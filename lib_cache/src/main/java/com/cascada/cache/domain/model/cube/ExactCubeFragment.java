package com.cascada.cache.domain.model.cube;

import java.util.List;
import java.util.Set;

/** Published manifest of a complete pre-HAVING, pre-sort, pre-limit frame. */
public record ExactCubeFragment(String generation, CubePopulation population, CubeInterval interval,
                                List<String> grain, Set<CubeMeasure> measures, int rows,
                                int compressedBytes, int decodedBytes, String digest) {
    public ExactCubeFragment {
        grain = List.copyOf(grain);
        measures = Set.copyOf(measures);
    }

}
