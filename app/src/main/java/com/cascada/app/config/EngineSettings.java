package com.cascada.app.config;

import com.cascada.cache.application.config.CacheExecutionConfiguration;
import java.util.Objects;

/** Immutable deployment settings, with independent Spark placement and cache storage choices. */
public record EngineSettings(CacheExecutionConfiguration cacheExecution,
                             String redisUri,
                             String mainTableName,
                             String mainTablePath,
                             boolean useLocalSpark,
                             int warmingTopNQueries,
                             CacheBackend cacheBackend) {

    public EngineSettings {
        Objects.requireNonNull(cacheExecution, "cacheExecution");
        Objects.requireNonNull(redisUri, "redisUri");
        Objects.requireNonNull(mainTableName, "mainTableName");
        Objects.requireNonNull(mainTablePath, "mainTablePath");
        Objects.requireNonNull(cacheBackend, "cacheBackend");
        if (warmingTopNQueries < 0) {
            throw new IllegalArgumentException("warmingTopNQueries must be >= 0, but was: " + warmingTopNQueries);
        }
    }

    /** Preserves the original default backend selection for existing callers. */
    public EngineSettings(CacheExecutionConfiguration cacheExecution, String redisUri, String mainTableName,
                          String mainTablePath, boolean useLocalSpark, int warmingTopNQueries) {
        this(cacheExecution, redisUri, mainTableName, mainTablePath, useLocalSpark, warmingTopNQueries,
                useLocalSpark ? CacheBackend.MEMORY : CacheBackend.VALKEY);
    }

    /**
     * Settings suitable for a local developer run or a test: a {@code local[*]} Spark session, a
     * loopback Redis, and the reference bucket/step configuration.
     */
    public static EngineSettings localDefaults() {
        return new EngineSettings(
                CacheExecutionConfiguration.defaults(),
                "redis://localhost:6379",
                "main_data_table",
                "/tmp/cascada/main_data_table",
                true,
                50);
    }
}
