package com.cascada.cache.application.service.cube;

record CubeVerifierGroup(Object[] measureValues, boolean[] measurePresent) {
    CubeVerifierGroup(int measureCount) {
        this(new Object[measureCount], new boolean[measureCount]);
    }
}
