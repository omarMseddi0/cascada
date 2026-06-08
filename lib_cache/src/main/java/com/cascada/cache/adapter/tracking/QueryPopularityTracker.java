package com.cascada.cache.adapter.tracking;

import com.cascada.identity.domain.QueryHash;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks query popularity (the Layer-2 warming feed and an auto-profiler input), ported from
 * {@code query_tracker.py}. The production adapter batches into Redis sorted sets ({@code QT:V1:TOP});
 * this in-memory adapter keeps the same observable behaviour — cumulative hit counts and a top-N by
 * popularity — so the warmer and the profiler can be developed and tested without Redis.
 */
public final class QueryPopularityTracker {

    private final Map<QueryHash, AtomicLong> hitCountByQuery = new ConcurrentHashMap<>();

    /** Record one observation of a query (a cache hit or a submission), incrementing its counter. */
    public void recordObservation(QueryHash queryHash) {
        hitCountByQuery.computeIfAbsent(queryHash, ignored -> new AtomicLong()).incrementAndGet();
    }

    public long hitCount(QueryHash queryHash) {
        AtomicLong counter = hitCountByQuery.get(queryHash);
        return counter == null ? 0L : counter.get();
    }

    /** The most popular {@code limit} queries, highest first — the warmer's prioritisation feed. */
    public List<QueryHash> topByPopularity(int limit) {
        return hitCountByQuery.entrySet().stream()
                .sorted(Comparator.<Map.Entry<QueryHash, AtomicLong>>comparingLong(entry -> entry.getValue().get())
                        .reversed())
                .limit(limit)
                .map(Map.Entry::getKey)
                .toList();
    }

    public int distinctQueryCount() {
        return hitCountByQuery.size();
    }
}
