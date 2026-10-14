package com.cascada.cache.domain.merge;

/**
 * HyperLogLog precision shared by the SQL that builds distinct-count sketches and the cache that
 * unions them. Relative standard error is about 1.04 / sqrt(2^LOG_CONFIG_K): 0.10% at 20, so about
 * 95% of estimates fall within 0.2%. A dense sketch at this precision is about 512 KB and a union
 * holds about 1 MB per group; sketches stay small until a group exceeds roughly 100k distinct values.
 */
public final class DistinctSketchPrecision {

    public static final int LOG_CONFIG_K = 20;

    /** Part of the cache identity, so sketches written at another precision are never unioned. */
    public static final String HASH_MARKER = "HLL_SKETCH_LG_CONFIG_K=" + LOG_CONFIG_K;

    private DistinctSketchPrecision() {
    }
}
