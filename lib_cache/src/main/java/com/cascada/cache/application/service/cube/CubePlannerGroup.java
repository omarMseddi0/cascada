package com.cascada.cache.application.service.cube;

record CubePlannerGroup(Object[] dimensionValues, Object[] measureValues, boolean[] measurePresent) {
    CubePlannerGroup(Object[] dimensionValues, int measureCount) {
        this(dimensionValues, new Object[measureCount], new boolean[measureCount]);
    }
}
