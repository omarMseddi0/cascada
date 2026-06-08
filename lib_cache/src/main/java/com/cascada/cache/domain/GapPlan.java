package com.cascada.cache.domain;

import java.util.List;
import java.util.Optional;

/**
 * The portion of a query's time range that is NOT yet in cache and therefore must be computed
 * on Spark: an optional partial {@code head} day, a list of whole {@code body} day-bucket starts,
 * and an optional partial {@code tail} day.
 *
 * <p>Ported from {@code GapPlan} in {@code domain.py}. The gap is the only thing that ever reaches
 * Spark; everything else is served from cached ingredients (ARCHITECTURE §3).
 */
public record GapPlan(Optional<TimeRange> head, List<Long> body, Optional<TimeRange> tail) {

    public GapPlan {
        body = List.copyOf(body);
    }

    /** Mirrors the Python {@code GapPlan.has_gaps} property. */
    public boolean hasGaps() {
        return head.isPresent() || !body.isEmpty() || tail.isPresent();
    }
}
