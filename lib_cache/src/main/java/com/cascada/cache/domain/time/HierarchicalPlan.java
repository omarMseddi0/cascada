package com.cascada.cache.domain.time;

import java.util.List;
import java.util.Optional;

public record HierarchicalPlan(Optional<TimeRange> leadingPartialRange, List<Long> completeDayBucketStarts,
                               List<Long> completeHourBucketStartsToday,
                               Optional<TimeRange> livePartialRange) {
    public HierarchicalPlan {
        completeDayBucketStarts = List.copyOf(completeDayBucketStarts);
        completeHourBucketStartsToday = List.copyOf(completeHourBucketStartsToday);
    }
}
