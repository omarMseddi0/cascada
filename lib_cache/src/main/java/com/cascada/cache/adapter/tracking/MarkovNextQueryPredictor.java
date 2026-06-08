package com.cascada.cache.adapter.tracking;

import com.cascada.identity.domain.QueryHash;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A first-order Markov predictor over query-session transitions, the speculative-warming engine of
 * plan §8.15. It learns "after query A, users usually run B" from the session transitions the query
 * tracker records, and ranks the likely next drill-downs so the warmer can prefetch them before the
 * user asks — turning a future cache miss into a hit.
 */
public final class MarkovNextQueryPredictor {

    private final Map<QueryHash, Map<QueryHash, AtomicLong>> transitionCounts = new ConcurrentHashMap<>();

    /** Record that {@code next} followed {@code previous} within a session. */
    public void recordTransition(QueryHash previous, QueryHash next) {
        transitionCounts
                .computeIfAbsent(previous, ignored -> new ConcurrentHashMap<>())
                .computeIfAbsent(next, ignored -> new AtomicLong())
                .incrementAndGet();
    }

    /** The most likely next queries after {@code current}, highest probability first. */
    public List<QueryHash> predictNext(QueryHash current, int limit) {
        Map<QueryHash, AtomicLong> followers = transitionCounts.get(current);
        if (followers == null) {
            return List.of();
        }
        return followers.entrySet().stream()
                .sorted(Comparator.<Map.Entry<QueryHash, AtomicLong>>comparingLong(entry -> entry.getValue().get())
                        .reversed())
                .limit(limit)
                .map(Map.Entry::getKey)
                .toList();
    }

    /** The estimated probability that {@code next} follows {@code current}. */
    public double transitionProbability(QueryHash current, QueryHash next) {
        Map<QueryHash, AtomicLong> followers = transitionCounts.get(current);
        if (followers == null || followers.isEmpty()) {
            return 0.0;
        }
        long total = followers.values().stream().mapToLong(AtomicLong::get).sum();
        AtomicLong count = followers.get(next);
        return count == null ? 0.0 : (double) count.get() / total;
    }
}
