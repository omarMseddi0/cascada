package com.cascada.cache.domain;

/**
 * A half-bounded-inclusive window of epoch seconds {@code [startTimestampSeconds, endTimestampSeconds]}.
 *
 * <p>Ported from the {@code TimeRange} dataclass in {@code domain.py}. The time range is
 * deliberately excluded from the logic hash (cache_hashing.py) so that the same query intent
 * over different windows shares cache buckets.
 */
public record TimeRange(long startTimestampSeconds, long endTimestampSeconds) {

    public TimeRange {
        if (endTimestampSeconds < startTimestampSeconds) {
            throw new IllegalArgumentException(
                    "time range end (" + endTimestampSeconds + ") must be >= start (" + startTimestampSeconds + ")");
        }
    }

    public static TimeRange of(long startTimestampSeconds, long endTimestampSeconds) {
        return new TimeRange(startTimestampSeconds, endTimestampSeconds);
    }

    /** Inclusive duration in seconds, mirroring the Python {@code end_ts - start_ts}. */
    public long durationSeconds() {
        return endTimestampSeconds - startTimestampSeconds;
    }
}
