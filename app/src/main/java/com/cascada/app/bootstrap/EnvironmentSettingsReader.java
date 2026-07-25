package com.cascada.app.bootstrap;

import com.cascada.cache.application.service.CacheExecutionConfiguration;
import com.cascada.spark.application.port.out.EnvironmentPort;

/**
 * Turns the surrounding environment into an {@link EngineSettings}. This is the <em>only</em> place in
 * the application that decides what an environment variable means.
 *
 * <p>Keeping it separate from {@link EngineSettings} matters: the settings record stays a pure value that
 * a test can construct literally, while every "read this variable, fall back to that default" rule lives
 * in one readable list. Adding a ConfigMap or a control-plane API as a configuration source later is a
 * new reader, not a change to the engine.
 *
 * <p>Note it takes an {@link EnvironmentPort} rather than calling {@link System#getenv} itself, so this
 * class too is unit-testable with a map.
 */
public final class EnvironmentSettingsReader {

    private final EnvironmentPort environment;

    public EnvironmentSettingsReader(EnvironmentPort environment) {
        this.environment = environment;
    }

    /** Read every setting, falling back to the local defaults for anything unset. */
    public EngineSettings read() {
        EngineSettings defaults = EngineSettings.localDefaults();
        CacheExecutionConfiguration cacheExecution = new CacheExecutionConfiguration(
                longSetting("CASCADA_BUCKET_SECONDS", defaults.cacheExecution().bucketSeconds()),
                (int) longSetting("CASCADA_FIXED_STEP_SECONDS", defaults.cacheExecution().fixedStepSeconds()),
                environment.getOrDefault("CASCADA_TIME_COLUMN", defaults.cacheExecution().timeColumnName()));

        return new EngineSettings(
                cacheExecution,
                environment.getOrDefault("REDIS_URL", defaults.redisUri()),
                environment.getOrDefault("CASCADA_MAIN_TABLE_NAME", defaults.mainTableName()),
                environment.getOrDefault("CASCADA_MAIN_TABLE_PATH", defaults.mainTablePath()),
                // Absence of an explicit Kubernetes master means "local" — a dev machine should never
                // accidentally aim at a cluster because a variable was forgotten.
                !"cluster".equalsIgnoreCase(environment.getOrDefault("CASCADA_RUN_MODE", "local")),
                (int) longSetting("CASCADA_WARMING_TOP_N", defaults.warmingTopNQueries()));
    }

    private long longSetting(String name, long fallback) {
        String raw = environment.get(name);
        if (raw == null) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException notANumber) {
            // Fail loudly: silently falling back would start the engine with a bucket width the
            // operator did not ask for, and every key written under it would be wrong.
            throw new IllegalArgumentException(name + " must be an integer, but was: '" + raw + "'", notANumber);
        }
    }
}
