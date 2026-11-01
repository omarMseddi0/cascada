package com.cascada.cache.application.model.config;

import com.cascada.cache.domain.time.CacheTimeConstants;

/**
 * The few knobs the execution engine needs: the cache bucket width, the fixed internal storage step,
 * the physical time column name, and how many hash partitions merge one query's cached frames in
 * parallel. Defaults mirror the reference engine (24h buckets, 300s step, {@code ts}).
 */
public record CacheExecutionConfiguration(long bucketSeconds, int fixedStepSeconds, String timeColumnName,
                                          int mergeWorkerCount) {

    /** Half the hardware threads, at most four: concurrent queries share the same cores. */
    public static final int DEFAULT_MERGE_WORKER_COUNT =
            Math.clamp(Runtime.getRuntime().availableProcessors() / 2, 1, 4);

    public CacheExecutionConfiguration {
        if (bucketSeconds <= 0) {
            throw new IllegalArgumentException("bucketSeconds must be > 0");
        }
        if (fixedStepSeconds <= 0) {
            throw new IllegalArgumentException("fixedStepSeconds must be > 0");
        }
        if (mergeWorkerCount <= 0) {
            throw new IllegalArgumentException("mergeWorkerCount must be > 0");
        }
    }

    public CacheExecutionConfiguration(long bucketSeconds, int fixedStepSeconds, String timeColumnName) {
        this(bucketSeconds, fixedStepSeconds, timeColumnName, DEFAULT_MERGE_WORKER_COUNT);
    }

    public static CacheExecutionConfiguration defaults() {
        return new CacheExecutionConfiguration(CacheTimeConstants.SECONDS_PER_DAY,
                CacheTimeConstants.DEFAULT_CACHE_STEP_SECONDS, "ts");
    }
}
