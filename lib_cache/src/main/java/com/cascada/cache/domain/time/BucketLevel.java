package com.cascada.cache.domain.time;

import java.util.Optional;

/**
 * A level in the time-bucket pyramid (ARCHITECTURE §6, plan §8.14): {@code FIVE_MINUTE → HOUR → DAY}.
 * Cascada replaces the reference engine's "daily only" assumption with this configurable pyramid so
 * that completed hours of <em>today</em> can be cached and only the current partial sub-bucket is ever
 * recomputed — which is what unlocks real-time dashboards while keeping the invariant that a stored
 * bucket is always internally complete.
 */
public enum BucketLevel {

    FIVE_MINUTE(300),
    HOUR(3_600),
    DAY(86_400);

    private final long secondsPerBucket;

    BucketLevel(long secondsPerBucket) {
        this.secondsPerBucket = secondsPerBucket;
    }

    public long secondsPerBucket() {
        return secondsPerBucket;
    }

    /** The next coarser level a completed bucket compacts into, or empty for the coarsest. */
    public Optional<BucketLevel> parent() {
        return switch (this) {
            case FIVE_MINUTE -> Optional.of(HOUR);
            case HOUR -> Optional.of(DAY);
            case DAY -> Optional.empty();
        };
    }
}
