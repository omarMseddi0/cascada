package com.cascada.cache.domain.warming;

import com.cascada.identity.domain.QueryHash;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The Layer-1 (voted) warming queue, ported from {@code warming_queue.py}: a hash-deduplicated,
 * insertion-ordered queue that the read path "votes" into when a query just missed the cache, and the
 * background warmer drains. Duplicate votes for the same query collapse, and the queue is lost on
 * restart (it is a best-effort hint, not a durable log) — exactly the reference semantics.
 *
 * <p>The Python {@code OrderedDict + Lock} becomes a {@link LinkedHashSet} guarded by a
 * {@link ReentrantLock}.
 */
public final class WarmingQueue {

    private final LinkedHashSet<QueryHash> votedQueries = new LinkedHashSet<>();
    private final ReentrantLock lock = new ReentrantLock();

    /** Record a vote to warm this query. A repeat vote for a queued hash is a no-op (dedup). */
    public void vote(QueryHash queryHash) {
        lock.lock();
        try {
            votedQueries.add(queryHash);
        } finally {
            lock.unlock();
        }
    }

    /** Drain every voted query in insertion order and clear the queue. */
    public List<QueryHash> consumeAll() {
        lock.lock();
        try {
            List<QueryHash> drained = new ArrayList<>(votedQueries);
            votedQueries.clear();
            return drained;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return votedQueries.size();
        } finally {
            lock.unlock();
        }
    }
}
