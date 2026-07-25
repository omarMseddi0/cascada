package com.cascada.app.bootstrap;

import com.cascada.cache.application.service.CacheExecutionConfiguration;

/**
 * Every deployment-time decision the composition root needs, in one place.
 *
 * <p>This is intentionally a dumb value record with no lookups inside it. Where the values come from —
 * environment variables, a mounted ConfigMap, a CLI flag, a test literal — is the decision of whoever
 * builds it (see {@link EnvironmentSettingsReader}), never of the object itself. That separation is what
 * keeps {@link CascadaEngineFactory} deterministic and lets a test wire the whole engine without
 * touching the machine.
 *
 * @param cacheExecution   bucket width, fixed internal storage step, and physical time column name
 * @param redisUri         Valkey/Redis endpoint for the hot cache tier
 * @param mainTableName    the logical table name customers write in their SQL
 * @param mainTablePath    the physical Delta path that logical table resolves to
 * @param useLocalSpark    {@code true} builds a {@code local[*]} session, {@code false} a {@code k8s://} one;
 *                         nothing else differs between local and cluster
 * @param warmingTopNQueries how many popular query patterns the Layer-2 warmer considers per cycle
 */
public record EngineSettings(CacheExecutionConfiguration cacheExecution,
                             String redisUri,
                             String mainTableName,
                             String mainTablePath,
                             boolean useLocalSpark,
                             int warmingTopNQueries) {

    public EngineSettings {
        if (warmingTopNQueries < 0) {
            throw new IllegalArgumentException("warmingTopNQueries must be >= 0, but was: " + warmingTopNQueries);
        }
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
