package com.cascada.app.model.config;

import com.cascada.app.model.config.CacheBackend;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import java.util.Objects;

/** Immutable deployment settings, with independent Spark placement and cache storage choices. */
public record EngineSettings(CacheExecutionConfiguration cacheExecution,
                             String redisUri,
                             String mainTableName,
                             String mainTablePath,
                             boolean useLocalSpark,
                             int warmingTopNQueries,
                             CacheBackend cacheBackend, int fetchChunkKeys) {

    public static final int DEFAULT_FETCH_CHUNK_KEYS = 128;

    public EngineSettings {
        Objects.requireNonNull(cacheExecution, "cacheExecution");
        Objects.requireNonNull(redisUri, "redisUri");
        Objects.requireNonNull(mainTableName, "mainTableName");
        Objects.requireNonNull(mainTablePath, "mainTablePath");
        Objects.requireNonNull(cacheBackend, "cacheBackend");
        if (fetchChunkKeys < 1 || fetchChunkKeys > 128)
            throw new IllegalArgumentException("fetchChunkKeys must be between 1 and 128");
        if (warmingTopNQueries < 0) {
            throw new IllegalArgumentException("warmingTopNQueries must be >= 0, but was: " + warmingTopNQueries);
        }
    }

    public EngineSettings(CacheExecutionConfiguration cacheExecution, String redisUri, String mainTableName,
                          String mainTablePath, boolean useLocalSpark, int warmingTopNQueries, CacheBackend cacheBackend) {
        this(cacheExecution, redisUri, mainTableName, mainTablePath, useLocalSpark, warmingTopNQueries,
                cacheBackend, DEFAULT_FETCH_CHUNK_KEYS);
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
