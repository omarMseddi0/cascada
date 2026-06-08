package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.TimeRange;

import java.util.Optional;

/**
 * RULE 5 — bypass a query that covers a single, <em>partial</em> cache bucket, because storing a
 * fraction of a day-bucket would corrupt the invariant that a stored bucket is always internally
 * complete. Ported from {@code PartialDayRule} in {@code safety_rules.py}.
 *
 * <p>A bucket of {@code H} hours spans {@code H*3600} seconds; the full inclusive range is
 * {@code bucketSeconds - 1}. The query bypasses when its start and end fall in the same bucket AND
 * its duration is strictly less than {@code bucketSeconds - 1} (the Python condition exactly).
 * Cascada's hierarchical pyramid (plan §8.14) later narrows this to only the live sub-bucket.
 */
public final class PartialDayBucketRule implements SafetyRule {

    private static final int SECONDS_PER_HOUR = 3_600;

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        long bucketSeconds = (long) configuration.cacheBucketHours() * SECONDS_PER_HOUR;

        TimeRange timeRange = canonicalObject.timeRange();
        long startBucket = (timeRange.startTimestampSeconds() / bucketSeconds) * bucketSeconds;
        long endBucket = (timeRange.endTimestampSeconds() / bucketSeconds) * bucketSeconds;

        boolean isSingleBucket = startBucket == endBucket;
        long durationSeconds = timeRange.durationSeconds();

        if (isSingleBucket && durationSeconds < (bucketSeconds - 1)) {
            return Optional.of(BypassReason.PARTIAL_DAY_BUCKET);
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "PartialDayBucketRule";
    }
}
