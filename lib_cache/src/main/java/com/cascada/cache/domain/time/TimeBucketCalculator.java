package com.cascada.cache.domain.time;

import com.cascada.cache.domain.CacheConstants;
import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pure day-bucket math, ported line-for-line from {@code TimeBucketCalculator} in
 * {@code time_utils.py} (itself an exact copy of {@code CacheExecutionEngine._get_daily_buckets},
 * lines 105-145).
 *
 * <p>The head/body/tail split is correctness-critical: it decides which whole days can be served
 * from cache and which partial edges must be recomputed. The arithmetic
 * {@code (timestamp / bucketSeconds) * bucketSeconds} uses Java long floor division, which matches
 * Python's {@code //} for the non-negative epoch seconds the cache deals in.
 */
public final class TimeBucketCalculator {

    private final long secondsPerBucket;

    public TimeBucketCalculator() {
        this(CacheConstants.SECONDS_PER_DAY);
    }

    public TimeBucketCalculator(long secondsPerBucket) {
        // Mirrors the Python guard: a non-positive value falls back to one day.
        this.secondsPerBucket = secondsPerBucket > 0 ? secondsPerBucket : CacheConstants.SECONDS_PER_DAY;
    }

    /** Ported from {@code _get_daily_buckets(start_ts, end_ts)}. */
    public DailyBuckets getDailyBuckets(long startTimestampSeconds, long endTimestampSeconds) {
        long firstDayStart = (startTimestampSeconds / secondsPerBucket) * secondsPerBucket;
        long lastDayStart = (endTimestampSeconds / secondsPerBucket) * secondsPerBucket;

        Optional<TimeRange> head = Optional.empty();
        Optional<TimeRange> tail = Optional.empty();

        long firstFullDay;
        if (startTimestampSeconds > firstDayStart) {
            // Query starts after midnight -> partial HEAD day.
            long firstDayEnd = firstDayStart + secondsPerBucket - 1;
            head = Optional.of(new TimeRange(startTimestampSeconds, Math.min(endTimestampSeconds, firstDayEnd)));
            firstFullDay = firstDayStart + secondsPerBucket;
        } else {
            firstFullDay = firstDayStart;
        }

        long lastDayEnd = lastDayStart + secondsPerBucket - 1;
        long lastFullDay;
        if (endTimestampSeconds < lastDayEnd) {
            // Query ends before midnight -> partial TAIL day.
            tail = Optional.of(new TimeRange(Math.max(startTimestampSeconds, lastDayStart), endTimestampSeconds));
            lastFullDay = lastDayStart - secondsPerBucket;
        } else {
            lastFullDay = lastDayStart;
        }

        List<Long> body = new ArrayList<>();
        long currentDay = firstFullDay;
        while (currentDay <= lastFullDay) {
            body.add(currentDay);
            currentDay += secondsPerBucket;
        }

        return new DailyBuckets(head, body, tail);
    }

    /**
     * Ported from {@code calculate_gaps}. Returns the head/tail partial edges plus only those whole
     * body days that are not already cached, so the Spark gap query covers exactly the missing data.
     */
    public GapPlan calculateGaps(long startTimestampSeconds, long endTimestampSeconds, List<Long> cachedDays) {
        DailyBuckets buckets = getDailyBuckets(startTimestampSeconds, endTimestampSeconds);
        List<Long> missingBody = new ArrayList<>();
        for (long day : buckets.body()) {
            if (!cachedDays.contains(day)) {
                missingBody.add(day);
            }
        }
        return new GapPlan(buckets.head(), missingBody, buckets.tail());
    }

    public long secondsPerBucket() {
        return secondsPerBucket;
    }
}
