package com.cascada.cache.application;

import com.cascada.cache.domain.CacheConstants;

/**
 * The few knobs the execution engine needs: the cache bucket width, the fixed internal storage step,
 * and the physical time column name. Defaults mirror the reference engine (24h buckets, 300s step,
 * {@code ts}).
 */
public record CacheExecutionConfiguration(long bucketSeconds, int fixedStepSeconds, String timeColumnName) {

    public CacheExecutionConfiguration {
        if (bucketSeconds <= 0) {
            throw new IllegalArgumentException("bucketSeconds must be > 0");
        }
        if (fixedStepSeconds <= 0) {
            throw new IllegalArgumentException("fixedStepSeconds must be > 0");
        }
    }

    public static CacheExecutionConfiguration defaults() {
        return new CacheExecutionConfiguration(CacheConstants.SECONDS_PER_DAY,
                CacheConstants.DEFAULT_CACHE_STEP_SECONDS, "ts");
    }
}
