package com.cascada.cache.application.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Warming knobs: bucket width, Layer-2 depth, pattern retention, how long a warmed bucket lives, and the
 * largest frame worth storing (a near-unique GROUP BY would otherwise fill the cache tier).
 */
public record WarmingConfiguration(long bucketSeconds, int topNQueries, int maximumPatterns,
                                   Duration bucketTimeToLive, int maximumCachedRows) {

    public static final Duration DEFAULT_BUCKET_TIME_TO_LIVE = Duration.ofDays(7);

    public WarmingConfiguration {
        Objects.requireNonNull(bucketTimeToLive, "bucketTimeToLive");
        if (bucketSeconds <= 0 || topNQueries < 0 || maximumPatterns < 1 || maximumCachedRows < 0
                || bucketTimeToLive.isNegative() || bucketTimeToLive.isZero()) {
            throw new IllegalArgumentException("invalid warming configuration");
        }
    }

    public static WarmingConfiguration of(long bucketSeconds, int topNQueries, int maximumPatterns) {
        return new WarmingConfiguration(bucketSeconds, topNQueries, maximumPatterns,
                DEFAULT_BUCKET_TIME_TO_LIVE, Integer.MAX_VALUE);
    }
}
