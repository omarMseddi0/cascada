package com.cascada.app.adapter.out.configuration;

import com.cascada.app.model.config.CacheBackend;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import java.util.Objects;
import java.util.function.Function;

/** Parses app deployment settings from an injected environment lookup. */
public final class EnvironmentSettingsReader {

    private final Function<String, String> environment;

    public EnvironmentSettingsReader(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    /** Read every setting, falling back to the local defaults for anything unset. */
    public EngineSettings read() {
        EngineSettings defaults = EngineSettings.localDefaults();
        CacheExecutionConfiguration cacheExecution = new CacheExecutionConfiguration(
                longSetting("CASCADA_BUCKET_SECONDS", defaults.cacheExecution().bucketSeconds()),
                intSetting("CASCADA_FIXED_STEP_SECONDS", defaults.cacheExecution().fixedStepSeconds()),
                setting("CASCADA_TIME_COLUMN", defaults.cacheExecution().timeColumnName()),
                positiveIntSetting("CASCADA_MERGE_WORKERS", defaults.cacheExecution().mergeWorkerCount()));

        boolean localSpark = localSparkMode(setting("CASCADA_RUN_MODE", "local"));

        return new EngineSettings(
                cacheExecution,
                setting("REDIS_URL", defaults.redisUri()),
                setting("CASCADA_MAIN_TABLE_NAME", defaults.mainTableName()),
                setting("CASCADA_MAIN_TABLE_PATH", defaults.mainTablePath()),
                localSpark,
                intSetting("CASCADA_WARMING_TOP_N", defaults.warmingTopNQueries()),
                CacheBackend.parse(setting("CASCADA_CACHE_BACKEND", localSpark ? "memory" : "valkey")));
    }

    private String setting(String name, String fallback) {
        String value = environment.apply(name);
        return value == null ? fallback : value;
    }

    private static boolean localSparkMode(String value) {
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "local" -> true;
            case "cluster" -> false;
            default -> throw new IllegalArgumentException("CASCADA_RUN_MODE must be local or cluster: " + value);
        };
    }

    private int intSetting(String name, int fallback) {
        return SettingValues.intValue(name, environment.apply(name), fallback, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    private int positiveIntSetting(String name, int fallback) {
        return SettingValues.intValue(name, environment.apply(name), fallback, 1, Integer.MAX_VALUE);
    }

    private long longSetting(String name, long fallback) {
        return SettingValues.longValue(name, environment.apply(name), fallback, Long.MIN_VALUE, Long.MAX_VALUE);
    }
}
