package com.cascada.app.model.config;

import java.time.Duration;

/**
 * Warming behaviour. {@code lookback} is both the warmed window and the bucket time-to-live, as in the
 * reference engine ({@code redis_ttl_seconds = lookback_days * 86400}). {@code validationSql} enables the
 * data-change canary when set (logical SQL with {@code {start}}/{@code {end}} placeholders); a null or blank
 * value disables it. For bucket-level validation, include {@code {bucketSeconds}} and return one row
 * per bucket with its epoch-second start as a {@code BIGINT} column named {@code bucket_start}. For example:
 * <pre>
 * SELECT CAST(FLOOR(ts / {bucketSeconds}) * {bucketSeconds} AS BIGINT) AS bucket_start,
 *        COUNT(*) AS row_count, SUM(bytes) AS bytes_total
 * FROM traffic WHERE ts BETWEEN {start} AND {end}
 * GROUP BY CAST(FLOOR(ts / {bucketSeconds}) * {bucketSeconds} AS BIGINT)
 * </pre>
 * The validation columns determine which source changes the cache can detect.
 * {@code popularityRetention} is how long an unused query stays in the Layer-2 top-N.
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
