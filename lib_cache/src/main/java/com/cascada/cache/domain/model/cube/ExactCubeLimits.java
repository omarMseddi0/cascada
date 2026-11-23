package com.cascada.cache.domain.model.cube;

/** Bounds shared by the plan checker and the bounded Spark state projection. */
public final class ExactCubeLimits {
    public static final int MAX_STATE_BYTES = 8 * 1024 * 1024;
    public static final int MAX_KEY_BYTES = 128;

    private ExactCubeLimits() {}
}
