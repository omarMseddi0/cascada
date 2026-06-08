package com.cascada.cache.domain.time;

import com.cascada.cache.domain.TimeRange;

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
        return Math.floorDiv(timestampSeconds, size) * size;
    }

    /** A bucket is complete (and therefore cacheable) once its whole span lies in the past. */
    public boolean isCompleteBucket(long bucketStartSeconds, BucketLevel level, long nowSeconds) {
        return bucketStartSeconds + level.secondsPerBucket() <= nowSeconds;
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
        long currentDayStart = bucketStart(nowSeconds, BucketLevel.DAY);

        List<Long> completeDays = new ArrayList<>();
        long day = bucketStart(startSeconds, BucketLevel.DAY);
        if (day < startSeconds) {
            day += BucketLevel.DAY.secondsPerBucket(); // first whole day at/after start
        }
        while (day < currentDayStart) {
            completeDays.add(day);
            day += BucketLevel.DAY.secondsPerBucket();
        }

        List<Long> completeHoursOfToday = new ArrayList<>();
        long liveHourStart = bucketStart(nowSeconds, BucketLevel.HOUR);
        for (long hour = currentDayStart; hour < liveHourStart; hour += BucketLevel.HOUR.secondsPerBucket()) {
            completeHoursOfToday.add(hour);
        }

        Optional<TimeRange> livePartial = liveHourStart <= nowSeconds
                ? Optional.of(new TimeRange(liveHourStart, nowSeconds))
                : Optional.empty();

        return new HierarchicalPlan(completeDays, completeHoursOfToday, livePartial);
    }

    /**
     * The assembled levels for a query: complete day-bucket starts (cache), complete hour-bucket starts
     * of the in-progress day (cache), and the live partial range (recompute).
     */
    public record HierarchicalPlan(List<Long> completeDayBucketStarts, List<Long> completeHourBucketStartsToday,
                                   Optional<TimeRange> livePartialRange) {
        public HierarchicalPlan {
            completeDayBucketStarts = List.copyOf(completeDayBucketStarts);
            completeHourBucketStartsToday = List.copyOf(completeHourBucketStartsToday);
        }
    }
}
