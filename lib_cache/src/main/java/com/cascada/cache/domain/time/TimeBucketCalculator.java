package com.cascada.cache.domain.time;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Pure day-bucket math, ported line-for-line from {@code TimeBucketCalculator} in
 * {@code time_utils.py} (itself an exact copy of {@code CacheExecutionEngine._get_daily_buckets},
 * lines 105-145).
 *
 * <p>The head/body/tail split is correctness-critical: it decides which whole days can be served
 * from cache and which partial edges must be recomputed. Bucket alignment uses floor division, matching
 * Python's {@code //} across the full timestamp domain.
 */
public final class TimeBucketCalculator {

    /** Hard ceiling for a single materialized bucket plan (about 27 years at daily granularity). */
    public static final int MAX_BUCKETS_PER_PLAN = 10_000;

    private final long secondsPerBucket;

    public TimeBucketCalculator() {
        this(CacheTimeConstants.SECONDS_PER_DAY);
    }

    public TimeBucketCalculator(long secondsPerBucket) {
        // Mirrors the Python guard: a non-positive value falls back to one day.
        this.secondsPerBucket = secondsPerBucket > 0 ? secondsPerBucket : CacheTimeConstants.SECONDS_PER_DAY;
    }

    /** Ported from {@code _get_daily_buckets(start_ts, end_ts)}. */
    public DailyBuckets getDailyBuckets(long startTimestampSeconds, long endTimestampSeconds) {
        if (endTimestampSeconds < startTimestampSeconds) {
            throw new IllegalArgumentException("endTimestampSeconds must not be before startTimestampSeconds");
        }
        // Use BigInteger for the aligned boundaries: the containing bucket for Long.MIN_VALUE can
        // begin just outside the long domain, and the last bucket's inclusive end can exceed MAX.
        // Those cases are partial edges, not arithmetic failures or cacheable complete buckets.
        BigInteger width = BigInteger.valueOf(secondsPerBucket);
        BigInteger start = BigInteger.valueOf(startTimestampSeconds);
        BigInteger end = BigInteger.valueOf(endTimestampSeconds);
        BigInteger firstDayStart = floorBucketStart(start, width);
        BigInteger lastDayStart = floorBucketStart(end, width);

        Optional<TimeRange> head = Optional.empty();
        Optional<TimeRange> tail = Optional.empty();

        BigInteger firstFullDay;
        if (start.compareTo(firstDayStart) > 0) {
            // Query starts after midnight -> partial HEAD day.
            BigInteger firstDayEnd = firstDayStart.add(width).subtract(BigInteger.ONE);
            long headEnd = end.min(firstDayEnd).longValueExact();
            head = Optional.of(new TimeRange(startTimestampSeconds, headEnd));
            firstFullDay = firstDayStart.add(width);
        } else {
            firstFullDay = firstDayStart;
        }

        BigInteger lastDayEnd = lastDayStart.add(width).subtract(BigInteger.ONE);
        BigInteger lastFullDay;
        if (end.compareTo(lastDayEnd) < 0) {
            // Query ends before midnight -> partial TAIL day.
            long tailStart = start.max(lastDayStart).longValueExact();
            tail = Optional.of(new TimeRange(tailStart, endTimestampSeconds));
            lastFullDay = lastDayStart.subtract(width);
        } else {
            lastFullDay = lastDayStart;
        }

        long bucketCount = 0;
        if (firstFullDay.compareTo(lastFullDay) <= 0) {
            BigInteger distance = lastFullDay.subtract(firstFullDay);
            BigInteger count = distance.divide(width).add(BigInteger.ONE);
            if (count.compareTo(BigInteger.valueOf(MAX_BUCKETS_PER_PLAN)) > 0) {
                throw new BucketEnumerationLimitExceededException(MAX_BUCKETS_PER_PLAN);
            }
            bucketCount = count.longValueExact();
        }

        List<Long> body = new ArrayList<>((int) bucketCount);
        for (long index = 0; index < bucketCount; index++) {
            BigInteger bucketStart = firstFullDay.add(width.multiply(BigInteger.valueOf(index)));
            try {
                body.add(bucketStart.longValueExact());
            } catch (ArithmeticException outsideLongDomain) {
                throw new BucketEnumerationLimitExceededException(MAX_BUCKETS_PER_PLAN);
            }
        }

        return new DailyBuckets(head, body, tail);
    }

    private BigInteger floorBucketStart(BigInteger timestamp, BigInteger width) {
        BigInteger[] division = timestamp.divideAndRemainder(width);
        BigInteger quotient = division[0];
        if (division[1].signum() < 0) {
            quotient = quotient.subtract(BigInteger.ONE);
        }
        return quotient.multiply(width);
    }

    /**
     * Ported from {@code calculate_gaps}. Returns the head/tail partial edges plus only those whole
     * body days that are not already cached, so the Spark gap query covers exactly the missing data.
     */
    public GapPlan calculateGaps(long startTimestampSeconds, long endTimestampSeconds, List<Long> cachedDays) {
        DailyBuckets buckets = getDailyBuckets(startTimestampSeconds, endTimestampSeconds);
        // O(1) membership: cachedDays arrives as a List, and List.contains inside the body loop made
        // gap analysis O(body x cached) — noticeable on year-long windows.
        Set<Long> cachedDaySet = new HashSet<>(cachedDays);
        List<Long> missingBody = new ArrayList<>();
        for (long day : buckets.body()) {
            if (!cachedDaySet.contains(day)) {
                missingBody.add(day);
            }
        }
        return new GapPlan(buckets.head(), missingBody, buckets.tail());
    }

    public long secondsPerBucket() {
        return secondsPerBucket;
    }
}
