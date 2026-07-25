package com.cascada.cache.adapter.out.tracking;

import com.cascada.cache.application.port.out.QueryPopularityPort;
import com.cascada.identity.domain.QueryHash;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The in-memory {@link QueryPopularityPort} adapter: tracks query popularity (the Layer-2 warming feed
 * and an auto-profiler input). The production adapter batches into Redis sorted sets
 * ({@code QT:V1:TOP}); this one keeps the same observable behaviour — cumulative hit counts and a
 * top-N by popularity — so the warmer and the profiler can be developed and tested without Redis.
 *
 * <p>This class is an <b>adapter</b>: it may only be referenced from a composition root (the {@code app}
 * module or a test), never from {@code application} or {@code domain}, which see it solely through
 * {@link QueryPopularityPort}.
 */
public final class QueryPopularityTracker implements QueryPopularityPort {

    private final Map<QueryHash, AtomicLong> hitCountByQuery = new ConcurrentHashMap<>();

    /** Record one observation of a query (a cache hit or a submission), incrementing its counter. */
    @Override
    public void recordObservation(QueryHash queryHash) {
        hitCountByQuery.computeIfAbsent(queryHash, ignored -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public long hitCount(QueryHash queryHash) {
        AtomicLong counter = hitCountByQuery.get(queryHash);
        return counter == null ? 0L : counter.get();
    }

    /** The most popular {@code limit} queries, highest first — the warmer's prioritisation feed. */
    @Override
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
