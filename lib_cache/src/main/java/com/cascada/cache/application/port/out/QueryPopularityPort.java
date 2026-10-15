package com.cascada.cache.application.port.out;

import com.cascada.identity.domain.QueryHash;

import java.util.List;

/**
 * Outbound (driven) port for the Layer-2 warming feed: a durable count of how often each query logic
 * hash has been observed, and the top-N most popular hashes.
 *
 * <p><b>Why this port exists.</b> {@code WarmingOrchestrator} lives in the application layer and used
 * to import {@code adapter.tracking.QueryPopularityTracker} directly. That is an
 * application&nbsp;&rarr;&nbsp;adapter dependency, i.e. a dependency pointing <em>outward</em>, which the
 * hexagon forbids ("all source code dependencies may only point from the outside inwards"). The
 * orchestrator now depends on this interface; the in-memory tracker and any future Redis
 * sorted-set tracker are interchangeable implementations behind it.
 *
 * <p>Semantics an implementation must honour:
 * <ul>
 *   <li>{@link #recordObservation} is monotonic — counts never decrease;</li>
 *   <li>{@link #topByPopularity} returns at most {@code limit} hashes, highest count first;</li>
 *   <li>a {@code limit} of {@code 0} returns an empty list (the warmer disables Layer 2 that way).</li>
 * </ul>
 */
public interface QueryPopularityPort {

    /** Record one observation of a query (a submission or a cache hit), incrementing its counter. */
    void recordObservation(QueryHash queryHash);

    /** The cumulative observation count for a hash; {@code 0} when never seen. */
    long hitCount(QueryHash queryHash);

    /** The most popular {@code limit} query hashes, highest count first. */
    List<QueryHash> topByPopularity(int limit);
}
