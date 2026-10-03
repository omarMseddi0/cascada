package com.cascada.cache.domain.time;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Decomposes a query range across pyramid levels (ARCHITECTURE §6, plan §8.14). A "today + last N days"
 * query assembles from completed DAY buckets, the completed HOUR buckets of the in-progress day, and a
 * single live partial sub-bucket that is recomputed. This generalises the reference engine's blanket
 * {@code PartialDayRule}: instead of bypassing all of today, only the live sub-bucket bypasses.
 */
public final class TimeBucketPyramid {

    /** Floor a timestamp to the start of its bucket at the given level. */
    public long bucketStart(long timestampSeconds, BucketLevel level) {
        long size = level.secondsPerBucket();
        try {
            return Math.multiplyExact(Math.floorDiv(timestampSeconds, size), size);
        } catch (ArithmeticException outsideLongDomain) {
            throw new BucketEnumerationLimitExceededException(TimeBucketCalculator.MAX_BUCKETS_PER_PLAN);
        }
    }

    /** A bucket is complete (and therefore cacheable) once its whole span lies in the past. */
    public boolean isCompleteBucket(long bucketStartSeconds, BucketLevel level, long nowSeconds) {
        try {
            return Math.addExact(bucketStartSeconds, level.secondsPerBucket()) <= nowSeconds;
        } catch (ArithmeticException outsideLongDomain) {
            return false;
        }
    }

    /** The coarser bucket start that a completed finer bucket compacts into at a boundary. */
    public long rollUpBucketStart(long finerBucketStartSeconds, BucketLevel toLevel) {
        return bucketStart(finerBucketStartSeconds, toLevel);
    }

    /**
     * Split {@code [startSeconds, nowSeconds]} into: complete whole days, the complete hours of the
     * in-progress day, and the single live partial hour. The complete levels are served from cache;
     * only {@code livePartial} reaches Spark.
     */
    public HierarchicalPlan assemble(long startSeconds, long nowSeconds) {
        if (startSeconds > nowSeconds) {
            throw new IllegalArgumentException("startSeconds must not be after nowSeconds");
        }
        long currentDayStart = bucketStart(nowSeconds, BucketLevel.DAY);
        long startDay = bucketStart(startSeconds, BucketLevel.DAY);

        List<Long> completeDays = new ArrayList<>();
        Optional<TimeRange> leadingPartial = Optional.empty();
        long day = startDay;
        if (day < startSeconds) {
            if (day < currentDayStart) {
                long firstDayEnd = day + BucketLevel.DAY.secondsPerBucket() - 1;
                leadingPartial = Optional.of(new TimeRange(startSeconds, firstDayEnd));
                day = Math.addExact(day, BucketLevel.DAY.secondsPerBucket());
            }
        }
        BigInteger completeDayCount = day < currentDayStart
                ? BigInteger.valueOf(currentDayStart).subtract(BigInteger.ONE).subtract(BigInteger.valueOf(day))
                .divide(BigInteger.valueOf(BucketLevel.DAY.secondsPerBucket())).add(BigInteger.ONE)
                : BigInteger.ZERO;
        if (completeDayCount.compareTo(BigInteger.valueOf(TimeBucketCalculator.MAX_BUCKETS_PER_PLAN)) > 0) {
            throw new BucketEnumerationLimitExceededException(TimeBucketCalculator.MAX_BUCKETS_PER_PLAN);
        }
        long daysToAdd = completeDayCount.longValueExact();
        long completeDay = day;
        for (long index = 0; index < daysToAdd; index++) {
            completeDays.add(completeDay);
            if (index + 1 < daysToAdd) {
                completeDay = Math.addExact(completeDay, BucketLevel.DAY.secondsPerBucket());
            }
        }

        List<Long> completeHoursOfToday = new ArrayList<>();
        long liveHourStart = bucketStart(nowSeconds, BucketLevel.HOUR);
        long firstHour = Math.max(currentDayStart, bucketStart(startSeconds, BucketLevel.HOUR));
        // When the query starts part-way through an earlier hour on today's date, the completed
        // hour buckets begin at the following hour. Preserve the leading seconds as a separate
        // live range; if start and now share an hour, livePartial below owns the whole range.
        long startHourStart = bucketStart(startSeconds, BucketLevel.HOUR);
        if (startDay == currentDayStart && startSeconds > startHourStart && startHourStart < liveHourStart) {
            long firstCompleteHourStart = startHourStart + BucketLevel.HOUR.secondsPerBucket();
            leadingPartial = Optional.of(new TimeRange(startSeconds,
                    Math.min(nowSeconds, firstCompleteHourStart - 1)));
        }
        if (firstHour < startSeconds) {
            try {
                firstHour = Math.addExact(firstHour, BucketLevel.HOUR.secondsPerBucket());
            } catch (ArithmeticException outsideLongDomain) {
                firstHour = Long.MAX_VALUE;
            }
        }
        for (long hour = firstHour; hour < liveHourStart; hour += BucketLevel.HOUR.secondsPerBucket()) {
            completeHoursOfToday.add(hour);
        }

        long liveStart = Math.max(startSeconds, liveHourStart);
        Optional<TimeRange> livePartial = liveStart <= nowSeconds
                ? Optional.of(new TimeRange(liveStart, nowSeconds))
                : Optional.empty();

        return new HierarchicalPlan(leadingPartial, completeDays, completeHoursOfToday, livePartial);
    }

    /**
     * The assembled levels for a query: complete day-bucket starts (cache), complete hour-bucket starts
     * of the in-progress day (cache), and the live partial range (recompute).
     */
    public record HierarchicalPlan(Optional<TimeRange> leadingPartialRange, List<Long> completeDayBucketStarts,
                                   List<Long> completeHourBucketStartsToday,
                                   Optional<TimeRange> livePartialRange) {
        public HierarchicalPlan {
            completeDayBucketStarts = List.copyOf(completeDayBucketStarts);
            completeHourBucketStartsToday = List.copyOf(completeHourBucketStartsToday);
        }
    }
}
