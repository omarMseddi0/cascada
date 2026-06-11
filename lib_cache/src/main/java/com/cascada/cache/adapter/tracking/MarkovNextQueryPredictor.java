package com.cascada.cache.adapter.tracking;

import com.cascada.identity.domain.QueryHash;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A first-order Markov predictor over query-session transitions, the speculative-warming engine of
 * plan §8.15. It learns "after query A, users usually run B" from the session transitions the query
 * tracker records, and ranks the likely next drill-downs so the warmer can prefetch them before the
 * user asks — turning a future cache miss into a hit.
 *
 * <p><strong>Bounded memory:</strong> a long-running service must not grow this map forever as the
 * workload drifts. Two caps keep it constant-size:
 * <ul>
 *   <li>at most {@code maxSourceQueries} source hashes, evicted least-recently-used (an
 *       access-ordered {@link LinkedHashMap}) — stale parents age out as dashboards change;</li>
 *   <li>at most {@code maxFollowersPerSource} followers per source — when full, the
 *       lowest-count follower is dropped to admit a new one, so the heavy hitters survive.</li>
 * </ul>
 * Mutation and reads are synchronized on the instance; the structure is tiny (hashes and longs), so
 * the lock is uncontended relative to the Spark/Valkey work around it.
 */
public final class MarkovNextQueryPredictor {

    private static final int DEFAULT_MAX_SOURCE_QUERIES = 10_000;
    private static final int DEFAULT_MAX_FOLLOWERS_PER_SOURCE = 64;

    private final int maxSourceQueries;
    private final int maxFollowersPerSource;
    private final LinkedHashMap<QueryHash, Map<QueryHash, Long>> transitionCounts;

    public MarkovNextQueryPredictor() {
        this(DEFAULT_MAX_SOURCE_QUERIES, DEFAULT_MAX_FOLLOWERS_PER_SOURCE);
    }

    public MarkovNextQueryPredictor(int maxSourceQueries, int maxFollowersPerSource) {
        if (maxSourceQueries <= 0 || maxFollowersPerSource <= 0) {
            throw new IllegalArgumentException("predictor caps must be > 0");
        }
        this.maxSourceQueries = maxSourceQueries;
        this.maxFollowersPerSource = maxFollowersPerSource;
        this.transitionCounts = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<QueryHash, Map<QueryHash, Long>> eldest) {
                return size() > MarkovNextQueryPredictor.this.maxSourceQueries;
            }
        };
    }

    /** Record that {@code next} followed {@code previous} within a session. */
    public synchronized void recordTransition(QueryHash previous, QueryHash next) {
        Map<QueryHash, Long> followers =
                transitionCounts.computeIfAbsent(previous, ignored -> new LinkedHashMap<>());
        Long current = followers.get(next);
        if (current == null && followers.size() >= maxFollowersPerSource) {
            evictWeakestFollower(followers);
        }
        followers.merge(next, 1L, Long::sum);
    }

    /** The most likely next queries after {@code current}, highest probability first. */
    public synchronized List<QueryHash> predictNext(QueryHash current, int limit) {
        Map<QueryHash, Long> followers = transitionCounts.get(current);
        if (followers == null) {
            return List.of();
        }
        return followers.entrySet().stream()
                .sorted(Comparator.<Map.Entry<QueryHash, Long>>comparingLong(Map.Entry::getValue).reversed())
                .limit(limit)
                .map(Map.Entry::getKey)
                .toList();
    }

    /** The estimated probability that {@code next} follows {@code current}. */
    public synchronized double transitionProbability(QueryHash current, QueryHash next) {
        Map<QueryHash, Long> followers = transitionCounts.get(current);
        if (followers == null || followers.isEmpty()) {
            return 0.0;
        }
        long total = followers.values().stream().mapToLong(Long::longValue).sum();
        Long count = followers.get(next);
        return count == null ? 0.0 : (double) count / total;
    }

    /** Diagnostic: how many source hashes are currently tracked (≤ {@code maxSourceQueries}). */
    public synchronized int trackedSourceCount() {
        return transitionCounts.size();
    }

    private void evictWeakestFollower(Map<QueryHash, Long> followers) {
        QueryHash weakest = null;
        long weakestCount = Long.MAX_VALUE;
        for (Map.Entry<QueryHash, Long> entry : new ArrayList<>(followers.entrySet())) {
            if (entry.getValue() < weakestCount) {
                weakestCount = entry.getValue();
                weakest = entry.getKey();
            }
        }
        if (weakest != null) {
            followers.remove(weakest);
        }
    }
}
