package com.cascada.app.adapter.out.configuration;

import com.cascada.app.model.config.CacheBackend;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import java.util.Objects;
import java.util.function.Function;

/** Reads the central engine/cache JSON with explicit, injected environment overrides. */
public final class EnvironmentSettingsReader {

    private final Function<String, String> environment;

    public EnvironmentSettingsReader(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    /** Read every setting, falling back to the local defaults for anything unset. */
    public EngineSettings read() {
        return read(SettingValues.applicationConfiguration(environment));
    }

    EngineSettings read(com.fasterxml.jackson.databind.JsonNode configuration) {
        EngineSettings defaults = EngineSettings.localDefaults();
        var cache = SettingValues.section(configuration, "cache");
        var engine = SettingValues.section(configuration, "engine");
        var warming = SettingValues.section(configuration, "warming");
        CacheExecutionConfiguration cacheExecution = new CacheExecutionConfiguration(
                longSetting("CASCADA_BUCKET_SECONDS", SettingValues.jsonLong(cache, "bucketSeconds", defaults.cacheExecution().bucketSeconds())),
                intSetting("CASCADA_FIXED_STEP_SECONDS", SettingValues.jsonInt(cache, "fixedStepSeconds", defaults.cacheExecution().fixedStepSeconds())),
                setting("CASCADA_TIME_COLUMN", SettingValues.jsonText(cache, "timeColumnName", defaults.cacheExecution().timeColumnName())),
                positiveIntSetting("CASCADA_MERGE_WORKERS", SettingValues.jsonInt(cache, "mergeWorkers", defaults.cacheExecution().mergeWorkerCount())));

        boolean localSpark = localSparkMode(setting("CASCADA_RUN_MODE", SettingValues.jsonText(engine, "runMode", "local")));

        return new EngineSettings(
                cacheExecution,
                setting("REDIS_URL", SettingValues.jsonText(cache, "redisUri", defaults.redisUri())),
                setting("CASCADA_MAIN_TABLE_NAME", SettingValues.jsonText(engine, "mainTableName", defaults.mainTableName())),
                setting("CASCADA_MAIN_TABLE_PATH", SettingValues.jsonText(engine, "mainTablePath", defaults.mainTablePath())),
                localSpark,
                intSetting("CASCADA_WARMING_TOP_N", SettingValues.jsonInt(warming, "topNQueries", defaults.warmingTopNQueries())),
                CacheBackend.parse(setting("CASCADA_CACHE_BACKEND", SettingValues.jsonText(cache, "backend", localSpark ? "memory" : "valkey"))));
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
