package com.cascada.cache.application.model.query;

import com.cascada.cache.domain.model.cube.CubePopulation;
import java.util.List;

/** Provenance for an ordinary AVG bucket whose original ingredients can serve another shape. */
public record BucketCubeBinding(CubePopulation population, List<String> grain, String averageAlias) {}
