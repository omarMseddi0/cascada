package com.cascada.app.config;

import java.time.Duration;

/**
 * Warming behaviour. {@code lookback} is both the warmed window and the bucket time-to-live, as in the
 * reference engine ({@code redis_ttl_seconds = lookback_days * 86400}). {@code validationSql} enables the
 * data-change canary when set (logical SQL with {@code {start}}/{@code {end}} placeholders); a null or blank
 * value disables it. {@code popularityRetention} is how long an unused query stays in the Layer-2 top-N.
 */
public record WarmingSettings(boolean enabled, Duration interval, Duration lookback, int maximumPatterns,
                              String validationSql, Duration popularityRetention) {

    public static final Duration DEFAULT_POPULARITY_RETENTION = Duration.ofDays(40);

    public WarmingSettings {
        if (interval == null || interval.isNegative() || interval.toMillis() < 1
                || lookback == null || lookback.isNegative() || lookback.toSeconds() < 1 || maximumPatterns < 1
                || popularityRetention == null || popularityRetention.isNegative() || popularityRetention.isZero())
            throw new IllegalArgumentException("Warming durations and retention limit must be positive");
    }

    public WarmingSettings(boolean enabled, Duration interval, Duration lookback, int maximumPatterns) {
        this(enabled, interval, lookback, maximumPatterns, null, DEFAULT_POPULARITY_RETENTION);
    }

    public static WarmingSettings defaults() {
        return new WarmingSettings(true, Duration.ofMinutes(5), Duration.ofDays(7), 10_000);
    }

    public boolean hasValidationSql() {
        return validationSql != null && !validationSql.isBlank();
    }
}
