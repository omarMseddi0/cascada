package com.cascada.cache.domain.model.cube;

public record CubePlannerGroup(Object[] dimensionValues, Object[] measureValues, boolean[] measurePresent) {
    public CubePlannerGroup(Object[] dimensionValues, int measureCount) {
        this(dimensionValues, new Object[measureCount], new boolean[measureCount]);
    }
}
