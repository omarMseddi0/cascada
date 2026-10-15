package com.cascada.cache.domain.cube;

record CubeVerifierGroup(Object[] measureValues, boolean[] measurePresent) {
    CubeVerifierGroup(int measureCount) {
        this(new Object[measureCount], new boolean[measureCount]);
    }
}
