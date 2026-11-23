package com.cascada.cache.domain.model.cube;

public enum CubeMeasure {
    SUM("cube_sum"), COUNT("cube_count");

    private final String stateLabel;

    CubeMeasure(String stateLabel) {
        this.stateLabel = stateLabel;
    }

    public String stateLabel() {
        return stateLabel;
    }
}
