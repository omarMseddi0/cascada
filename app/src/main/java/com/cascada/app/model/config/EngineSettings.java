package com.cascada.app.model.config;

import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import java.util.Objects;

/** Immutable deployment settings, for Spark execution and Valkey storage. */
public record EngineSettings(CacheExecutionConfiguration cacheExecution,
                             String redisUri,
                             String mainTableName,
                             String mainTablePath,
                             boolean useLocalSpark,
                             int warmingTopNQueries) {

    public EngineSettings {
        Objects.requireNonNull(cacheExecution, "cacheExecution");
        Objects.requireNonNull(redisUri, "redisUri");
        Objects.requireNonNull(mainTableName, "mainTableName");
        Objects.requireNonNull(mainTablePath, "mainTablePath");
        if (warmingTopNQueries < 0) {
            throw new IllegalArgumentException("warmingTopNQueries must be >= 0, but was: " + warmingTopNQueries);
        }
    }

    /**
     * Settings suitable for a local developer run or a test: a {@code local[*]} Spark session, a
     * loopback Valkey, and the reference bucket/step configuration.
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
