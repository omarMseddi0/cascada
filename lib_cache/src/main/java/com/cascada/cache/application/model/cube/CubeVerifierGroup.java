package com.cascada.cache.application.model.cube;

public record CubeVerifierGroup(Object[] measureValues, boolean[] measurePresent) {
    public CubeVerifierGroup(int measureCount) {
        this(new Object[measureCount], new boolean[measureCount]);
    }
}
