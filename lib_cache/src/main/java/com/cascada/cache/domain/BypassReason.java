package com.cascada.cache.domain;

/**
 * The specific guardrail that forced a {@link CacheDecision#BYPASS_V4}.
 *
 * <p>In the Python reference every bypass reason collapsed to the single string
 * {@code 'V4_BYPASS'}. Cascada keeps that wire compatibility (every reason maps to
 * {@link CacheDecision#BYPASS_V4}) but additionally records <em>which</em> rule fired, because
 * the auto-profiler and the cost preview need to explain the decision to the customer (plan §8.10).
 */
public enum BypassReason {

    IMPOSSIBLE_MATH,
    HIGH_CARDINALITY_GROUP_BY,
    LIQUID_CLUSTERED_FILTER,
    INCOMPATIBLE_TIME_STEP,
    MINIMUM_TIME_RANGE,
    PARTIAL_DAY_BUCKET;

    public CacheDecision asCacheDecision() {
        return CacheDecision.BYPASS_V4;
    }
}
