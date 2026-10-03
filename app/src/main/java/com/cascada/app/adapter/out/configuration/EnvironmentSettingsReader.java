package com.cascada.app.adapter.out.configuration;

import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.app.config.EngineSettings;
import com.cascada.app.config.CacheBackend;
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
                setting("CASCADA_TIME_COLUMN", defaults.cacheExecution().timeColumnName()));

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
        long value = longSetting(name, fallback);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be between " + Integer.MIN_VALUE + " and "
                    + Integer.MAX_VALUE + ", but was: " + value);
        }
        return (int) value;
    }

    private long longSetting(String name, long fallback) {
        String raw = environment.apply(name);
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
