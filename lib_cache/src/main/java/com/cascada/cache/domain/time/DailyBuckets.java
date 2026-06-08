package com.cascada.cache.domain.time;

import com.cascada.cache.domain.TimeRange;

import java.util.List;
import java.util.Optional;

/**
 * The decomposition of a query range into an optional partial {@code head} day, a list of whole
 * {@code body} day-bucket start timestamps, and an optional partial {@code tail} day.
 *
 * <p>Ported from the {@code {"head", "body", "tail"}} dictionary returned by
 * {@code TimeBucketCalculator._get_daily_buckets} in {@code time_utils.py}.
 */
public record DailyBuckets(Optional<TimeRange> head, List<Long> body, Optional<TimeRange> tail) {

    public DailyBuckets {
        body = List.copyOf(body);
    }
}
