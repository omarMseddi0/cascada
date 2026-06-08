package com.cascada.cache.domain;

/**
 * The outcome of the safety-rule evaluation, ported from {@code CacheDecision} in
 * {@code domain.py} / {@code cache_safety_checker.py}.
 *
 * <p>The wire values are deliberately the exact strings the Python engine emits
 * ({@code V4_BYPASS}, {@code V1_AGGREGATION}) so that metadata, logs, and any persisted
 * decision remain compatible across the migration.
 */
public enum CacheDecision {

    /** Safe to cache and serve via the aggregation engine. */
    AGGREGATION_V1("V1_AGGREGATION"),

    /** A guardrail forced a bypass; run the physical SQL directly on Spark and do not cache. */
    BYPASS_V4("V4_BYPASS");

    private final String wireValue;

    CacheDecision(String wireValue) {
        this.wireValue = wireValue;
    }

    public String wireValue() {
        return wireValue;
    }

    public boolean isBypass() {
        return this == BYPASS_V4;
    }
}
